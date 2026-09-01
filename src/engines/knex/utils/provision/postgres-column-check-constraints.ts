import type { Knex } from 'knex';

export async function dropPostgresColumnCheckConstraints(
  knex: Knex,
  tableName: string,
  columnName: string,
): Promise<void> {
  const result = await knex.raw(
    `
      SELECT DISTINCT constraint_def.conname AS constraint_name
      FROM pg_constraint constraint_def
      JOIN pg_class relation
        ON relation.oid = constraint_def.conrelid
      JOIN pg_namespace namespace
        ON namespace.oid = relation.relnamespace
      JOIN pg_attribute attribute
        ON attribute.attrelid = relation.oid
       AND attribute.attnum = ANY(constraint_def.conkey)
      WHERE constraint_def.contype = 'c'
        AND namespace.nspname = current_schema()
        AND relation.relname = ?
        AND attribute.attname = ?
    `,
    [tableName, columnName],
  );
  for (const constraint of result.rows ?? []) {
    await knex.raw('ALTER TABLE ?? DROP CONSTRAINT ??', [
      tableName,
      constraint.constraint_name,
    ]);
  }
}
