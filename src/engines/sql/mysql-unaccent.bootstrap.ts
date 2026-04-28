import type { Knex } from 'knex';

const markRegex = /\p{Mark}/gu;

function escapeMysqlSingleQuotedLiteral(s: string): string {
  return s.replace(/\\/g, '\\\\').replace(/'/g, "''");
}

function foldScalar(ch: string): string {
  return ch.normalize('NFKD').replace(markRegex, '');
}

let cachedReplaceLines: string[] | null = null;

function buildReplaceLines(): string[] {
  const byFrom = new Map<string, string>();
  for (let cp = 0x80; cp <= 0x10ffff; cp++) {
    try {
      const ch = String.fromCodePoint(cp);
      const folded = foldScalar(ch);
      if (folded === ch || folded.length === 0) {
        continue;
      }
      if (/[\u0000-\u001f\u007f]/.test(folded)) {
        continue;
      }
      if (!byFrom.has(ch)) {
        byFrom.set(ch, folded);
      }
    } catch {
      /* invalid scalar */
    }
  }
  const pairs = [...byFrom.entries()];
  pairs.sort((a, b) => {
    const la = [...a[0]].length;
    const lb = [...b[0]].length;
    if (lb !== la) {
      return lb - la;
    }
    return a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0;
  });
  return pairs.map(([from, to]) => {
    const f = escapeMysqlSingleQuotedLiteral(from);
    const t = escapeMysqlSingleQuotedLiteral(to);
    return `SET input = REPLACE(input, '${f}', '${t}');`;
  });
}

function getReplaceLines(): string[] {
  if (!cachedReplaceLines) {
    cachedReplaceLines = buildReplaceLines();
  }
  return cachedReplaceLines;
}

export function getMysqlUnaccentReplaceStatementsForTests(): string[] {
  return [...getReplaceLines()];
}

export function getMysqlUnaccentDropSql(): string {
  return 'DROP FUNCTION IF EXISTS unaccent';
}

export function getMysqlUnaccentCreateSql(): string {
  const replaceLines = getReplaceLines().join('\n');
  return `CREATE FUNCTION unaccent(input TEXT) RETURNS TEXT
DETERMINISTIC
BEGIN
IF input IS NULL THEN
RETURN NULL;
END IF;
${replaceLines}
SET input = REGEXP_REPLACE(input, '\\\\p{M}+', '');
RETURN input;
END`;
}

export async function installMysqlUnaccent(knex: Knex): Promise<void> {
  const existing = await knex.raw(
    `SELECT 1 FROM information_schema.ROUTINES
     WHERE ROUTINE_SCHEMA = DATABASE()
       AND ROUTINE_TYPE = 'FUNCTION'
       AND ROUTINE_NAME = 'unaccent'
     LIMIT 1`,
  );
  const rows = Array.isArray(existing) ? existing[0] : existing?.rows;
  if (Array.isArray(rows) && rows.length > 0) {
    return;
  }
  try {
    await knex.raw(getMysqlUnaccentCreateSql());
  } catch (err: any) {
    if (err?.errno === 1304 || err?.code === 'ER_SP_ALREADY_EXISTS') {
      return;
    }
    throw err;
  }
}
