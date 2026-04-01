import * as fs from 'fs';
import * as path from 'path';

const LOG_FILE = path.join(process.cwd(), 'tmp', 'package-operations.log');

export function pkgLog(source: string, message: string, data?: any) {
  const ts = new Date().toISOString();
  const dataStr = data !== undefined ? ' ' + JSON.stringify(data, null, 0) : '';
  const line = `[${ts}] [${source}] ${message}${dataStr}\n`;
  try {
    fs.appendFileSync(LOG_FILE, line);
  } catch {
    // ignore write errors
  }
}
