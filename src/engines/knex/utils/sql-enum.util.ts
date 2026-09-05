import type { Knex } from 'knex';
import { createHash } from 'node:crypto';

const POSTGRES_IDENTIFIER_LIMIT = 63;

export function isPostgresDatabaseType(dbType: string): boolean {
  const normalized = dbType.toLowerCase();
  return normalized.includes('pg') || normalized.includes('postgres');
}

export function getPostgresEnumTypeName(
  tableName: string,
  columnName: string,
): string {
  const name = `${tableName}_${columnName}_enum`;
  if (name.length <= POSTGRES_IDENTIFIER_LIMIT) return name;
  const hash = createHash('md5').update(name).digest('hex').slice(0, 12);
  return `enum_${hash}`;
}

export function addSqlEnumColumn(
  table: Knex.CreateTableBuilder | Knex.AlterTableBuilder,
  tableName: string,
  columnName: string,
  options: readonly string[],
  dbType: string,
): Knex.ColumnBuilder {
  if (isPostgresDatabaseType(dbType)) {
    return table.enu(columnName, [...options], {
      useNative: true,
      enumName: getPostgresEnumTypeName(tableName, columnName),
    });
  }
  return table.enu(columnName, [...options]);
}

export async function hasSqlValuesOutsideEnumOptions(
  knex: Knex,
  tableName: string,
  columnName: string,
  options: readonly string[],
): Promise<boolean> {
  if (options.length === 0) return true;
  const query = knex(tableName).select(columnName).whereNotNull(columnName);
  if (isPostgresDatabaseType(String(knex.client.config.client))) {
    const placeholders = options.map(() => '?').join(', ');
    query.whereRaw(`CAST(?? AS TEXT) NOT IN (${placeholders})`, [
      columnName,
      ...options,
    ]);
  } else {
    query.whereNotIn(columnName, [...options]);
  }
  return Boolean(await query.first());
}
