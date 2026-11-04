'use strict';

const express = require('express');
const multer = require('multer');
const { spawn } = require('child_process');
const fs = require('fs');
const fsp = fs.promises;
const path = require('path');
const crypto = require('crypto');
const dns = require('dns').promises;
const net = require('net');
const { Readable } = require('stream');
const { pipeline } = require('stream/promises');

// ───────────────────────── Konfigurasi ─────────────────────────
const PORT = process.env.SERVER_PORT || process.env.PORT || 3000;
const MAX_MB = Number(process.env.MAX_MB || 2048); // batas ukuran input
const MAX_BYTES = MAX_MB * 1024 * 1024;
const CONCURRENCY = Number(process.env.CONCURRENCY || 1); // proses ffmpeg bersamaan
const PRESET = process.env.X264_PRESET || 'medium'; // slow = lebih kecil, lebih lama
const TTL_MS = 60 * 60 * 1000; // hasil dihapus 1 jam setelah selesai
const WORK = path.join(__dirname, 'work');

function tryRequire(name) {
  try { return require(name); } catch { return null; }
}
const FFMPEG = process.env.FFMPEG_PATH || tryRequire('ffmpeg-static') || 'ffmpeg';
const FFPROBE = process.env.FFPROBE_PATH || (tryRequire('ffprobe-static') || {}).path || 'ffprobe';

// Resolusi TIDAK diubah. Yang diturunkan hanya bitrate lewat CRF.
const CRF = {
  h264: { ringan: 24, seimbang: 28, maksimal: 32 },
  h265: { ringan: 28, seimbang: 31, maksimal: 34 },
};

fs.rmSync(WORK, { recursive: true, force: true });
fs.mkdirSync(WORK, { recursive: true });

// ───────────────────────── State job ─────────────────────────
const jobs = new Map();
const queue = [];
let running = 0;

function newId() {
  return crypto.randomBytes(9).toString('base64url');
}

function emit(job, event, data) {
  job.snap[event] = data;
  const msg = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const res of job.clients) res.write(msg);
}

function stage(job, name, extra = {}) {
  emit(job, 'stage', { name, ...extra });
}

function enqueue(job) {
  job.status = 'queued';
  queue.push(job);
  pump();
  updateQueue();
}

function pump() {
  while (running < CONCURRENCY && queue.length) {
    const job = queue.shift();
    running++;
    job.status = 'running';
    runJob(job).finally(() => {
      running--;
      job.updated = Date.now();
      pump();
      updateQueue();
    });
  }
}

function updateQueue() {
  queue.forEach((job, i) => stage(job, 'queue', { position: i + 1 }));
}

function removeJob(id) {
  const job = jobs.get(id);
  if (!job) return;
  job.canceled = true;
  const qi = queue.indexOf(job);
  if (qi >= 0) queue.splice(qi, 1);
  if (job.proc) job.proc.kill('SIGKILL');
  if (job.abort) job.abort.abort();
  for (const res of job.clients) res.end();
  jobs.delete(id);
  fsp.rm(job.dir, { recursive: true, force: true }).catch(() => {});
}

setInterval(() => {
  const now = Date.now();
  for (const [id, job] of jobs) {
    const idle = job.status === 'done' || job.status === 'error';
    if (idle && now - job.updated > TTL_MS) removeJob(id);
  }
}, 5 * 60 * 1000).unref();

// Kirim komentar kosong supaya koneksi SSE tidak diputus proxy.
setInterval(() => {
  for (const job of jobs.values()) for (const res of job.clients) res.write(':\n\n');
}, 15000).unref();

// ───────────────────────── ffprobe & ringkasan ─────────────────────────
function run(cmd, args) {
  return new Promise((resolve, reject) => {
    const p = spawn(cmd, args);
    let out = '';
    let err = '';
    p.stdout.on('data', (d) => (out += d));
    p.stderr.on('data', (d) => (err += d));
    p.on('error', reject);
    p.on('close', (code) => (code === 0 ? resolve(out) : reject(new Error(err || `exit ${code}`))));
  });
}

async function probe(file) {
  const out = await run(FFPROBE, [
    '-v', 'error', '-print_format', 'json', '-show_format', '-show_streams', file,
  ]);
  return JSON.parse(out);
}

function fraction(s) {
  if (!s) return 0;
  const [n, d] = String(s).split('/').map(Number);
  return d ? n / d : n || 0;
}

function summarize(p, size, ext) {
  const streams = p.streams || [];
  const v = streams.find((s) => s.codec_type === 'video' && !(s.disposition && s.disposition.attached_pic));
  const a = streams.find((s) => s.codec_type === 'audio');
  if (!v) return null;

  let width = v.width;
  let height = v.height;
  const sd = (v.side_data_list || []).find((x) => x.rotation !== undefined);
  const rot = Math.abs(Number((v.tags && v.tags.rotate) ?? (sd && sd.rotation) ?? 0));
  if (rot === 90 || rot === 270) [width, height] = [height, width];

  const duration = parseFloat(p.format && p.format.duration) || parseFloat(v.duration) || 0;
  const fps = fraction(v.avg_frame_rate) || fraction(v.r_frame_rate);

  return {
    container: ext,
    videoCodec: v.codec_name,
    width,
    height,
    rawWidth: v.width,
    rawHeight: v.height,
    fps: Math.round(fps * 100) / 100,
    duration,
    size,
    totalBitrate: duration ? Math.round((size * 8) / duration) : Number(p.format && p.format.bit_rate) || 0,
    videoBitrate: Number(v.bit_rate) || null,
    audioCodec: a ? a.codec_name : null,
    audioChannels: a ? a.channels : null,
  };
}

// ───────────────────────── Download dari URL ─────────────────────────
function isPrivateIp(ip) {
  if (net.isIPv4(ip)) {
    const [a, b] = ip.split('.').map(Number);
    return (
      a === 0 || a === 10 || a === 127 || a >= 224 ||
      (a === 169 && b === 254) ||
      (a === 172 && b >= 16 && b <= 31) ||
      (a === 192 && b === 168) ||
      (a === 100 && b >= 64 && b <= 127)
    );
  }
  const l = ip.toLowerCase();
  if (l.startsWith('::ffff:')) return isPrivateIp(l.slice(7));
  return l === '::1' || l === '::' || l.startsWith('fc') || l.startsWith('fd') || l.startsWith('fe80');
}

async function assertPublic(hostname) {
  const host = hostname.replace(/^\[|\]$/g, '');
  const addrs = net.isIP(host) ? [{ address: host }] : await dns.lookup(host, { all: true });
  if (!addrs.length || addrs.some((a) => isPrivateIp(a.address))) {
    throw new Error('Alamat URL ini tidak diizinkan.');
  }
}

const TYPE_EXT = {
  'video/mp4': '.mp4',
  'video/webm': '.webm',
  'video/quicktime': '.mov',
  'video/x-matroska': '.mkv',
  'video/x-msvideo': '.avi',
};

async function download(job) {
  let target = new URL(job.url);
  const ctrl = new AbortController();
  job.abort = ctrl;

  try {
    let res;
    for (let hop = 0; hop <= 5; hop++) {
      if (!/^https?:$/.test(target.protocol)) throw new Error('Hanya URL http atau https yang didukung.');
      await assertPublic(target.hostname);
      res = await fetch(target, {
        signal: ctrl.signal,
        redirect: 'manual',
        headers: { 'User-Agent': 'Mozilla/5.0 (compatible; Ciutkan/1.0)' },
      });
      const loc = res.headers.get('location');
      if (res.status >= 300 && res.status < 400 && loc) {
        target = new URL(loc, target);
        continue;
      }
      break;
    }
    if (res.status >= 300 && res.status < 400) throw new Error('URL dialihkan terlalu banyak kali.');
    if (!res.ok) throw new Error(`Server sumber menjawab dengan kode ${res.status}.`);

    const type = (res.headers.get('content-type') || '').toLowerCase().split(';')[0].trim();
    if (type === 'text/html') {
      throw new Error('URL ini halaman web, bukan file video. Gunakan tautan langsung ke file video, misalnya yang berakhiran .mp4.');
    }
    const total = Number(res.headers.get('content-length')) || 0;
    if (total > MAX_BYTES) throw new Error(`Ukuran file melebihi batas ${MAX_MB} MB.`);

    const base = decodeURIComponent(path.basename(target.pathname) || '') || 'video';
    let ext = path.extname(base).toLowerCase();
    if (!/^\.[a-z0-9]{1,5}$/.test(ext)) ext = TYPE_EXT[type] || '';
    job.origName = base.endsWith(ext) ? base : base + ext;
    job.input = path.join(job.dir, 'input' + ext);

    let loaded = 0;
    let last = 0;
    const src = Readable.fromWeb(res.body);
    src.on('data', (chunk) => {
      loaded += chunk.length;
      if (loaded > MAX_BYTES) {
        job.tooBig = true;
        ctrl.abort();
        return;
      }
      const now = Date.now();
      if (now - last > 250) {
        last = now;
        emit(job, 'progress', {
          phase: 'download', loaded, total, percent: total ? (loaded / total) * 100 : null,
        });
      }
    });
    await pipeline(src, fs.createWriteStream(job.input));
    emit(job, 'progress', { phase: 'download', loaded, total: loaded, percent: 100 });
  } catch (e) {
    if (job.tooBig) throw new Error(`Ukuran file melebihi batas ${MAX_MB} MB.`);
    if (job.canceled) throw e;
    if (e.cause && e.cause.code) throw new Error('Gagal terhubung ke URL tersebut.');
    throw e;
  }
}

// ───────────────────────── Encode ─────────────────────────
function encode(job, info, outPath) {
  return new Promise((resolve, reject) => {
    const { codec, quality } = job.opts;
    const crf = String(CRF[codec][quality]);

    const args = ['-hide_banner', '-y', '-i', job.input, '-map', '0:v:0', '-map', '0:a?'];

    if (codec === 'h265') {
      args.push('-c:v', 'libx265', '-preset', PRESET, '-crf', crf, '-pix_fmt', 'yuv420p',
        '-tag:v', 'hvc1', '-x265-params', 'log-level=error');
    } else {
      args.push('-c:v', 'libx264', '-preset', PRESET, '-crf', crf, '-pix_fmt', 'yuv420p');
    }
    // Hanya menambah padding 1 piksel bila dimensi ganjil (syarat yuv420p). Tidak ada scale.
    if (info.rawWidth % 2 || info.rawHeight % 2) {
      args.push('-vf', 'pad=ceil(iw/2)*2:ceil(ih/2)*2');
    }
    args.push('-c:a', 'aac', '-b:a', quality === 'maksimal' ? '96k' : '128k');
    args.push('-movflags', '+faststart', '-progress', 'pipe:1', '-nostats', outPath);

    const p = spawn(FFMPEG, args);
    job.proc = p;

    let buf = '';
    let cur = {};
    let errTail = '';

    p.stdout.on('data', (d) => {
      buf += d;
      let i;
      while ((i = buf.indexOf('\n')) >= 0) {
        const line = buf.slice(0, i).trim();
        buf = buf.slice(i + 1);
        const eq = line.indexOf('=');
        if (eq < 0) continue;
        const k = line.slice(0, eq);
        cur[k] = line.slice(eq + 1);
        if (k === 'progress') {
          report(cur);
          cur = {};
        }
      }
    });

    function report(c) {
      const t = Number(c.out_time_us ?? c.out_time_ms) / 1e6;
      const dur = info.duration;
      const pct = dur > 0 && isFinite(t) ? Math.min(99.9, Math.max(0, (t / dur) * 100)) : null;
      const sp = parseFloat(c.speed);
      const eta = isFinite(sp) && sp > 0 && dur > 0 && isFinite(t) ? Math.max(0, (dur - t) / sp) : null;
      emit(job, 'progress', {
        phase: 'encode',
        percent: pct,
        time: isFinite(t) ? t : 0,
        duration: dur,
        size: Number(c.total_size) || 0,
        speed: isFinite(sp) ? sp : null,
        fps: Number(c.fps) || null,
        eta,
      });
    }

    p.stderr.on('data', (d) => {
      errTail = (errTail + d).slice(-1500);
    });
    p.on('error', reject);
    p.on('close', (code) => {
      job.proc = null;
      if (job.canceled) return reject(new Error('canceled'));
      if (code === 0) return resolve();
      const lastLine = errTail.trim().split('\n').pop() || `ffmpeg berhenti dengan kode ${code}`;
      reject(new Error(`Proses kompres gagal: ${lastLine}`));
    });
  });
}

// ───────────────────────── Alur utama job ─────────────────────────
async function runJob(job) {
  try {
    if (job.canceled) return;
    if (job.url) {
      stage(job, 'download');
      await download(job);
    }
    if (job.canceled) return;

    stage(job, 'probe');
    const ext = path.extname(job.origName || '').replace('.', '').toLowerCase();
    const size = (await fsp.stat(job.input)).size;
    let p;
    try {
      p = await probe(job.input);
    } catch (e) {
      if (e.code === 'ENOENT') throw e;
      throw new Error('File ini tidak bisa dibaca sebagai video.');
    }
    const info = summarize(p, size, ext || 'video');
    if (!info) throw new Error('Tidak ditemukan aliran video di file ini.');
    emit(job, 'input', info);

    stage(job, 'encode');
    const outPath = path.join(job.dir, 'output.mp4');
    await encode(job, info, outPath);

    const osize = (await fsp.stat(outPath)).size;
    const oinfo = summarize(await probe(outPath), osize, 'mp4');
    const base = path.basename(job.origName || 'video', path.extname(job.origName || ''))
      .replace(/[^\w\-. ]+/g, '_').slice(0, 80) || 'video';

    job.output = outPath;
    job.outName = `${base}-ciutkan.mp4`;
    job.status = 'done';
    fsp.unlink(job.input).catch(() => {});

    const savedBytes = size - osize;
    emit(job, 'done', {
      input: info,
      output: oinfo,
      savedBytes,
      savedPercent: Math.round((savedBytes / size) * 1000) / 10,
      url: `/files/${job.id}`,
      download: `/files/${job.id}?dl=1`,
      filename: job.outName,
    });
  } catch (e) {
    if (job.canceled) return;
    job.status = 'error';
    const message = e.code === 'ENOENT'
      ? 'ffmpeg tidak ditemukan di server. Jalankan "npm install" atau atur FFMPEG_PATH.'
      : e.message || 'Terjadi kesalahan.';
    emit(job, 'error', { message });
  }
}

// ───────────────────────── HTTP ─────────────────────────
const app = express();
app.disable('x-powered-by');
app.use(express.static(path.join(__dirname, 'public')));

const upload = multer({
  storage: multer.diskStorage({
    destination: (req, file, cb) => cb(null, req.jobDir),
    filename: (req, file, cb) => {
      const ext = path.extname(file.originalname).toLowerCase();
      cb(null, 'input' + (/^\.[a-z0-9]{1,5}$/.test(ext) ? ext : ''));
    },
  }),
  limits: { fileSize: MAX_BYTES, files: 1 },
});

app.get('/api/config', (req, res) => res.json({ maxMB: MAX_MB, ttlMinutes: TTL_MS / 60000 }));

app.post(
  '/api/jobs',
  (req, res, next) => {
    req.jobId = newId();
    req.jobDir = path.join(WORK, req.jobId);
    fs.mkdirSync(req.jobDir, { recursive: true });
    next();
  },
  (req, res, next) => {
    upload.single('file')(req, res, (err) => {
      if (!err) return next();
      fsp.rm(req.jobDir, { recursive: true, force: true }).catch(() => {});
      const big = err.code === 'LIMIT_FILE_SIZE';
      res.status(big ? 413 : 400).json({
        error: big ? `Ukuran file melebihi batas ${MAX_MB} MB.` : 'File gagal diterima server.',
      });
    });
  },
  (req, res) => {
    const body = req.body || {};
    const codec = body.codec === 'h265' ? 'h265' : 'h264';
    const quality = ['ringan', 'seimbang', 'maksimal'].includes(body.quality) ? body.quality : 'seimbang';
    const url = String(body.url || '').trim();

    const fail = (code, error) => {
      fsp.rm(req.jobDir, { recursive: true, force: true }).catch(() => {});
      return res.status(code).json({ error });
    };

    if (!req.file && !url) return fail(400, 'Pilih file video atau isi URL dulu.');
    if (!req.file) {
      try {
        const u = new URL(url);
        if (!/^https?:$/.test(u.protocol)) throw new Error();
      } catch {
        return fail(400, 'URL tidak valid. Gunakan alamat yang diawali http:// atau https://');
      }
    }

    const job = {
      id: req.jobId,
      dir: req.jobDir,
      url: req.file ? null : url,
      input: req.file ? req.file.path : null,
      origName: req.file ? Buffer.from(req.file.originalname, 'latin1').toString('utf8') : null,
      opts: { codec, quality },
      status: 'queued',
      updated: Date.now(),
      clients: new Set(),
      snap: {},
      canceled: false,
      proc: null,
      abort: null,
    };
    jobs.set(job.id, job);
    enqueue(job);
    res.json({ id: job.id });
  }
);

app.get('/api/jobs/:id/events', (req, res) => {
  const job = jobs.get(req.params.id);
  if (!job) return res.status(404).json({ error: 'Proses tidak ditemukan atau sudah kedaluwarsa.' });

  res.set({
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache, no-transform',
    Connection: 'keep-alive',
    'X-Accel-Buffering': 'no',
  });
  res.flushHeaders();

  for (const ev of ['stage', 'input', 'progress', 'done', 'error']) {
    if (job.snap[ev]) res.write(`event: ${ev}\ndata: ${JSON.stringify(job.snap[ev])}\n\n`);
  }
  job.clients.add(res);
  req.on('close', () => job.clients.delete(res));
});

app.get('/files/:id', (req, res) => {
  const job = jobs.get(req.params.id);
  if (!job || job.status !== 'done') return res.status(404).send('File tidak ditemukan atau sudah kedaluwarsa.');
  res.type('video/mp4');
  if (req.query.dl) res.attachment(job.outName);
  res.sendFile(job.output); // mendukung Range, jadi video bisa di-seek
});

app.delete('/api/jobs/:id', (req, res) => {
  removeJob(req.params.id);
  res.json({ ok: true });
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`Ciutkan berjalan di port ${PORT} (batas ${MAX_MB} MB, ${CONCURRENCY} proses bersamaan)`);
});
