import * as fs from 'fs';
import * as path from 'path';
import { buildMongoFoldTextSearchJs } from '../src/shared/utils/mongo-fold-text-search.template';

const BODY_NAME = 'mongo-fold-text-search.body.txt';
const outPath = path.join(
  __dirname,
  '..',
  'dist',
  'shared',
  'utils',
  BODY_NAME,
);
const body = buildMongoFoldTextSearchJs();

fs.mkdirSync(path.dirname(outPath), { recursive: true });
fs.writeFileSync(outPath, body, 'utf8');
