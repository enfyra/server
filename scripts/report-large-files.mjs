import { readFileSync, readdirSync, statSync } from 'node:fs';
import { join } from 'node:path';

const root = join(process.cwd(), 'src');
const files = [];

function collect(directory) {
  for (const entry of readdirSync(directory)) {
    const path = join(directory, entry);
    if (statSync(path).isDirectory()) collect(path);
    else if (path.endsWith('.ts')) files.push(path);
  }
}

collect(root);
const oversized = files
  .map((path) => ({ path, lines: readFileSync(path, 'utf8').split('\n').length }))
  .filter(({ lines }) => lines > 1000)
  .sort((left, right) => right.lines - left.lines);

for (const { path, lines } of oversized) {
  console.warn(`::warning file=${path},title=Large source file::${lines} LOC exceeds the 1,000 LOC review threshold.`);
}
