import { spawnSync } from 'node:child_process';

const result = spawnSync('yarn', ['test:e2e:snapshot-migration-matrix'], {
  cwd: process.cwd(),
  env: process.env,
  stdio: 'inherit',
});

if (result.error) throw result.error;
process.exitCode = result.status ?? 1;
