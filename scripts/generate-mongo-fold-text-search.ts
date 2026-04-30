import * as fs from 'fs';
import * as path from 'path';
import { buildMongoFoldTextSearchJs } from '@enfyra/kernel';

const BODY_NAME = 'mongo-fold-text-search.body.txt';
const outPaths = [
  path.join(__dirname, '..', 'dist', 'shared', 'utils', BODY_NAME),
];
const body = buildMongoFoldTextSearchJs();

for (const outPath of outPaths) {
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, body, 'utf8');
}
