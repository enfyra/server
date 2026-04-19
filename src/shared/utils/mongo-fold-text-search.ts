import * as fs from 'fs';
import * as path from 'path';
import { buildMongoFoldTextSearchJs } from './mongo-fold-text-search.template';

const BODY_FILENAME = 'mongo-fold-text-search.body.txt';

let ramCache: string | null = null;

function tryReadBodyFromDisk(): string | null {
  const candidates = [
    path.join(__dirname, BODY_FILENAME),
    path.join(process.cwd(), 'dist', 'shared', 'utils', BODY_FILENAME),
  ];
  for (const p of candidates) {
    try {
      if (fs.existsSync(p)) {
        return fs.readFileSync(p, 'utf8');
      }
    } catch {
      /* ignore */
    }
  }
  return null;
}

export function getMongoFoldTextSearchJs(): string {
  if (ramCache !== null) {
    return ramCache;
  }
  const fromDisk = tryReadBodyFromDisk();
  if (fromDisk) {
    ramCache = fromDisk;
    return ramCache;
  }
  ramCache = buildMongoFoldTextSearchJs();
  return ramCache;
}
