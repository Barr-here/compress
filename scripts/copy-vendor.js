// Menyalin ffmpeg.wasm dari node_modules ke public/vendor supaya disajikan dari domain yang sama
// (worker tidak boleh dimuat dari CDN lain).
const fs = require('fs');
const path = require('path');

const root = path.join(__dirname, '..');
const nm = path.join(root, 'node_modules', '@ffmpeg');
const out = path.join(root, 'public', 'vendor');

fs.rmSync(out, { recursive: true, force: true });
for (const [from, to] of [['ffmpeg/dist/esm', 'ffmpeg'], ['core/dist/esm', 'core']]) {
  const src = path.join(nm, from);
  if (!fs.existsSync(src)) {
    console.error('Tidak ditemukan: ' + src + '. Jalankan npm install dulu.');
    process.exit(1);
  }
  fs.cpSync(src, path.join(out, to), { recursive: true });
}
console.log('ffmpeg.wasm siap di public/vendor');
