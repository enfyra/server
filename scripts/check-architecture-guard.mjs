import { execFileSync } from 'node:child_process';

const baseIndex = process.argv.indexOf('--base');
const base = baseIndex === -1 ? 'HEAD' : process.argv[baseIndex + 1];
const diff = execFileSync(
  'git',
  ['diff', '--no-ext-diff', '--unified=0', '--no-color', base, '--'],
  { encoding: 'utf8' },
);
const changedTests = /^(?:\+\+\+ b\/)?test\/.*\.(?:spec|test)\.ts$/m.test(diff);
const guardedFiles = new Set(['src/init.ts', 'src/express-app.ts']);
let file = '';
const violations = [];

for (const line of diff.split('\n')) {
  if (line.startsWith('+++ b/')) {
    file = line.slice(6);
    continue;
  }
  if (!line.startsWith('+') || line.startsWith('+++')) continue;
  const added = line.slice(1);
  const isGuarded = file.startsWith('src/wiring/') || guardedFiles.has(file);
  if (isGuarded && /(?:^|[<(:,= ]|\btype\s+\w+\s*=\s*)any(?:\b|[\[|;,)>])/.test(added)) {
    violations.push(`${file}: new explicit any is forbidden in wiring/app assembly`);
  }
  if (/throw\s+new\s+\w*(?:Error|Exception)\s*\([^\n]*\b(?:error|err)\.message\b/.test(added)) {
    violations.push(`${file}: error.message-based handling requires a changed regression test`);
  }
}

if (violations.some((violation) => violation.includes('error.message')) && !changedTests) {
  violations.push('Add a matching test under test/**/*.spec.ts for the new error-message handling.');
}

if (violations.length > 0) {
  console.error(violations.join('\n'));
  process.exitCode = 1;
}
