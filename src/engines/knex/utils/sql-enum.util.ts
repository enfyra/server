import type { Knex } from 'knex';
import { createHash } from 'node:crypto';
import { quoteIdentifier } from './migration/sql-dialect';
import { normalizeEnumOptionsValue } from '../../../shared/utils/json-field-normalizer.util';
import type {
  PostgresEnumTypePlan,
  SqlEnumColumnDefinition,
} from '../types/sql-enum.types';

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
      existingType: true,
      enumName: getPostgresEnumTypeName(tableName, columnName),
    });
  }
  return table.enu(columnName, [...options]);
}

export async function planPostgresEnumTypeCreation(
  knex: Knex,
  tableName: string,
  column: SqlEnumColumnDefinition,
): Promise<PostgresEnumTypePlan> {
  const options = column.options;
  if (!Array.isArray(options) || options.length === 0) {
    throw new Error(`Enum column ${column.name} requires options`);
  }
  if (
    options.some((option) => typeof option !== 'string') ||
    new Set(options).size !== options.length
  ) {
    throw new Error(`Enum column ${column.name} has invalid options`);
  }
  const typeName = getPostgresEnumTypeName(tableName, column.name);
  const result = await knex.raw(
    `SELECT array_agg(enum_values.enumlabel::text ORDER BY enum_values.enumsortorder) AS values
       FROM pg_type types
       JOIN pg_namespace namespace ON namespace.oid = types.typnamespace
       LEFT JOIN pg_enum enum_values ON enum_values.enumtypid = types.oid
      WHERE namespace.nspname = current_schema()
        AND types.typname = ?
      GROUP BY types.oid`,
    [typeName],
  );
  const existing = result.rows?.[0];
  if (existing) {
    const existingValues = Array.isArray(existing.values)
      ? existing.values
      : [];
    if (JSON.stringify(existingValues) !== JSON.stringify(options)) {
      throw new Error(
        `PostgreSQL enum type ${typeName} already exists with incompatible options`,
      );
    }
    return { typeName, statements: [] };
  }
  const values = options
    .map((option) => `'${option.replace(/'/g, "''")}'`)
    .join(', ');
  return {
    typeName,
    statements: [
      `CREATE TYPE ${quoteIdentifier(typeName, 'postgres')} AS ENUM (${values})`,
    ],
  };
}

export async function ensureSqlEnumTypes(
  knex: Knex,
  tableName: string,
  columns: readonly SqlEnumColumnDefinition[],
  dbType: string,
): Promise<void> {
  if (!isPostgresDatabaseType(dbType)) return;
  const statements: string[] = [];
  for (const column of columns) {
    if (column.type !== 'enum') continue;
    const plan = await planPostgresEnumTypeCreation(knex, tableName, {
      ...column,
      options: normalizeEnumOptionsValue(column.options),
    });
    statements.push(...plan.statements);
  }
  for (const statement of statements) {
    await knex.raw(statement);
  }
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
