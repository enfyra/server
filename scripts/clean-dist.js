const fs = require('node:fs');
const path = require('node:path');

const distPath = path.resolve(process.cwd(), 'dist');
const buildInfoPath = path.resolve(process.cwd(), 'tsconfig.tsbuildinfo');
if (path.basename(distPath) !== 'dist') {
  throw new Error(`Refusing to clean unexpected path: ${distPath}`);
}
fs.rmSync(distPath, { recursive: true, force: true });
fs.rmSync(buildInfoPath, { force: true });
