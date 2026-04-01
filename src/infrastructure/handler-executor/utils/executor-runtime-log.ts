import * as fs from 'fs';
import * as path from 'path';

export const ISOLATED_EXECUTOR_RUNTIME_LOG_PATH = path.join(
  process.cwd(),
  'test',
  'logs',
  'isolated-executor-runtime.log',
);

function fileLogEnabled(): boolean {
  if (process.env.ISOLATED_EXECUTOR_FILE_LOG === '0') return false;
  if (process.env.ISOLATED_EXECUTOR_FILE_LOG === '1') return true;
  return process.env.JEST_WORKER_ID !== undefined;
}

export function appendIsolatedExecutorRuntimeLog(record: Record<string, unknown>): void {
  if (!fileLogEnabled()) return;
  try {
    const dir = path.dirname(ISOLATED_EXECUTOR_RUNTIME_LOG_PATH);
    fs.mkdirSync(dir, { recursive: true });
    const line =
      JSON.stringify({
        ts: new Date().toISOString(),
        pid: process.pid,
        ...record,
      }) + '\n';
    fs.appendFileSync(ISOLATED_EXECUTOR_RUNTIME_LOG_PATH, line);
  } catch {
    /* avoid breaking executor on log IO failure */
  }
}
