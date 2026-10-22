import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import knex from 'knex';

async function main() {
  const directory = process.env.CROSS_DB_PRIVATE_DIRECTORY!;
  const state = JSON.parse(readFileSync(join(directory, 'state.json'), 'utf8'));
  assert.match(state.sourceDatabase, /^enfyra_1endpoint_source_[a-f0-9]{32}$/);
  const [container] = JSON.parse(
    execFileSync('docker', ['inspect', 'postgres-dev'], { encoding: 'utf8' }),
  );
  const env = Object.fromEntries(
    container.Config.Env.map((entry: string) => {
      const index = entry.indexOf('=');
      return [entry.slice(0, index), entry.slice(index + 1)];
    }),
  );
  const db = knex({
    client: 'pg',
    connection: {
      host: '127.0.0.1',
      port: 5432,
      user: env.POSTGRES_USER,
      password: env.POSTGRES_PASSWORD,
      database: state.sourceDatabase,
    },
    pool: { min: 0, max: 1 },
  });
  try {
    const columns = (
      await db.raw(
        `SELECT table_name, column_name, data_type, udt_name, is_nullable, column_default, character_maximum_length, numeric_precision, numeric_scale, datetime_precision FROM information_schema.columns WHERE table_schema = 'public' ORDER BY table_name, ordinal_position`,
      )
    ).rows;
    const constraints = (
      await db.raw(
        `SELECT t.relname AS table_name,c.conname,c.contype,pg_get_constraintdef(c.oid) AS definition,array(SELECT a.attname::text FROM unnest(c.conkey) WITH ORDINALITY AS k(attnum,n) JOIN pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=k.attnum ORDER BY k.n) AS columns,rt.relname AS target_table,array(SELECT a.attname::text FROM unnest(c.confkey) WITH ORDINALITY AS k(attnum,n) JOIN pg_attribute a ON a.attrelid=c.confrelid AND a.attnum=k.attnum ORDER BY k.n) AS target_columns,c.confupdtype,c.confdeltype FROM pg_constraint c JOIN pg_class t ON t.oid=c.conrelid JOIN pg_namespace n ON n.oid=t.relnamespace LEFT JOIN pg_class rt ON rt.oid=c.confrelid WHERE n.nspname='public' ORDER BY t.relname,c.conname`,
      )
    ).rows;
    const indexes = (
      await db.raw(
        `SELECT tablename,indexname,indexdef FROM pg_indexes WHERE schemaname='public' ORDER BY tablename,indexname`,
      )
    ).rows;
    const enums = (
      await db.raw(
        `SELECT t.typname,array_agg(e.enumlabel::text ORDER BY e.enumsortorder) AS labels FROM pg_type t JOIN pg_enum e ON e.enumtypid=t.oid GROUP BY t.typname`,
      )
    ).rows;
    const tables = await db('enfyra_table').select('*');
    const metadataColumns = await db('enfyra_column').select('*');
    const relations = await db('enfyra_relation').select('*');
    const catalog = {
      columns,
      constraints,
      indexes,
      enums,
      tables,
      metadataColumns,
      relations,
    };
    writeFileSync(
      join(directory, 'source-catalog.json'),
      JSON.stringify(catalog, null, 2),
      { mode: 0o600 },
    );
    console.log(
      JSON.stringify({
        physicalTables: new Set(columns.map((column: any) => column.table_name))
          .size,
        metadataTables: tables.length,
        physicalTypes: [
          ...new Set(
            columns.map(
              (column: any) => `${column.data_type}:${column.udt_name}`,
            ),
          ),
        ],
        metadataTypes: [
          ...new Set(metadataColumns.map((column) => column.type)),
        ],
        checks: constraints
          .filter((constraint: any) => constraint.contype === 'c')
          .map((constraint: any) => ({
            table: constraint.table_name,
            definition: constraint.definition,
          })),
        unusualIndexes: indexes
          .filter((index: any) => /WHERE|USING (?!btree)/.test(index.indexdef))
          .map((index: any) => index.indexdef),
      }),
    );
  } finally {
    await db.destroy();
  }
}
void main().catch((error) => {
  console.error(error.message);
  process.exitCode = 1;
});
