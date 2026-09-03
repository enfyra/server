import type { Knex } from 'knex';
import { quoteIdentifier } from './sql-dialect';
import { generateColumnDefinition } from './sql-generator';
import {
  getPostgresEnumTypeName,
  hasSqlValuesOutsideEnumOptions,
} from '../sql-enum.util';
import { findPostgresColumnCheckConstraintNames } from '../provision/postgres-column-check-constraints';

type PostgresEnumColumnState = {
  dataType: string;
  udtName: string;
  defaultValue: string | null;
  enumValues: string[];
};

function getEnumOptions(column: any): string[] {
  if (!Array.isArray(column?.options) || column.options.length === 0) {
    throw new Error(
      `Enum column ${String(column?.name ?? '')} requires options`,
    );
  }
  const options = column.options.filter(
    (option: unknown): option is string => typeof option === 'string',
  );
  if (options.length !== column.options.length) {
    throw new Error(
      `Enum column ${String(column?.name ?? '')} has invalid options`,
    );
  }
  return options;
}

function buildPostgresEnumValuesSql(options: readonly string[]): string {
  return options.map((option) => `'${option.replace(/'/g, "''")}'`).join(', ');
}

export function buildPostgresEnumColumnDefinition(
  tableName: string,
  column: any,
): string {
  const enumType = quoteIdentifier(
    getPostgresEnumTypeName(tableName, column.name),
    'postgres',
  );
  return generateColumnDefinition(
    { ...column, type: 'varchar', options: undefined },
    'postgres',
  ).replace(/^VARCHAR\(255\)/i, enumType);
}

async function readPostgresEnumColumnState(
  knex: Knex,
  tableName: string,
  columnName: string,
): Promise<PostgresEnumColumnState | null> {
  const result = await knex.raw(
    `SELECT columns.data_type AS "dataType",
            columns.udt_name AS "udtName",
            columns.column_default AS "defaultValue",
            CASE
              WHEN types.typtype = 'e' THEN
                (SELECT array_agg(enum_values.enumlabel::text ORDER BY enum_values.enumsortorder)
                   FROM pg_enum enum_values
                  WHERE enum_values.enumtypid = types.oid)
              ELSE NULL
            END AS "enumValues"
       FROM information_schema.columns columns
       LEFT JOIN pg_type types ON types.typname = columns.udt_name
      WHERE columns.table_schema = current_schema()
        AND columns.table_name = ?
        AND columns.column_name = ?`,
    [tableName, columnName],
  );
  const row = result.rows?.[0];
  if (!row) return null;
  return {
    dataType: String(row.dataType ?? ''),
    udtName: String(row.udtName ?? ''),
    defaultValue: row.defaultValue ?? null,
    enumValues: Array.isArray(row.enumValues) ? row.enumValues.map(String) : [],
  };
}

export async function planPostgresEnumTypeCreation(
  knex: Knex,
  tableName: string,
  column: any,
): Promise<{ typeName: string; statements: string[] }> {
  const options = getEnumOptions(column);
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
      ? existing.values.map(String)
      : [];
    if (JSON.stringify(existingValues) !== JSON.stringify(options)) {
      throw new Error(
        `PostgreSQL enum type ${typeName} already exists with incompatible options`,
      );
    }
    return { typeName, statements: [] };
  }
  return {
    typeName,
    statements: [
      `CREATE TYPE ${quoteIdentifier(typeName, 'postgres')} AS ENUM (${buildPostgresEnumValuesSql(options)})`,
    ],
  };
}

export async function planPostgresEnumUpdate(
  knex: Knex,
  tableName: string,
  oldColumn: any,
  newColumn: any,
): Promise<string[]> {
  const options = getEnumOptions(newColumn);
  const current = await readPostgresEnumColumnState(
    knex,
    tableName,
    newColumn.name,
  );
  if (!current) {
    throw new Error(`Column ${tableName}.${newColumn.name} does not exist`);
  }
  if (
    await hasSqlValuesOutsideEnumOptions(
      knex,
      tableName,
      newColumn.name,
      options,
    )
  ) {
    throw new Error(
      `Cannot update ${tableName}.${newColumn.name} enum: unsupported persisted values`,
    );
  }

  const optionsChanged =
    oldColumn?.type !== 'enum' ||
    JSON.stringify(oldColumn?.options ?? []) !== JSON.stringify(options);
  const physicalMatches =
    current.dataType === 'USER-DEFINED' &&
    JSON.stringify(current.enumValues) === JSON.stringify(options);
  const table = quoteIdentifier(tableName, 'postgres');
  const column = quoteIdentifier(newColumn.name, 'postgres');
  const statements: string[] = [];

  if (optionsChanged || !physicalMatches) {
    if (current.defaultValue !== null) {
      statements.push(
        `ALTER TABLE ${table} ALTER COLUMN ${column} DROP DEFAULT`,
      );
    }
    const checkConstraints = await findPostgresColumnCheckConstraintNames(
      knex,
      tableName,
      newColumn.name,
    );
    for (const constraintName of checkConstraints) {
      statements.push(
        `ALTER TABLE ${table} DROP CONSTRAINT IF EXISTS ${quoteIdentifier(constraintName, 'postgres')}`,
      );
    }
    statements.push(
      `ALTER TABLE ${table} ALTER COLUMN ${column} TYPE text USING ${column}::text`,
    );

    const typeName = getPostgresEnumTypeName(tableName, newColumn.name);
    if (
      current.dataType === 'USER-DEFINED' &&
      current.udtName &&
      current.udtName !== typeName
    ) {
      statements.push(
        `DROP TYPE IF EXISTS ${quoteIdentifier(current.udtName, 'postgres')}`,
      );
    }
    statements.push(
      `DROP TYPE IF EXISTS ${quoteIdentifier(typeName, 'postgres')}`,
      `CREATE TYPE ${quoteIdentifier(typeName, 'postgres')} AS ENUM (${buildPostgresEnumValuesSql(options)})`,
      `ALTER TABLE ${table} ALTER COLUMN ${column} TYPE ${quoteIdentifier(typeName, 'postgres')} USING ${column}::text::${quoteIdentifier(typeName, 'postgres')}`,
    );
  }

  const definition = buildPostgresEnumColumnDefinition(tableName, newColumn);
  const defaultMatch = definition.match(/\s+DEFAULT\s+(.+)$/i);
  const oldDefault = oldColumn?.defaultValue ?? null;
  const newDefault = newColumn?.defaultValue ?? null;
  if ((optionsChanged || !physicalMatches) && newDefault !== null) {
    if (!defaultMatch?.[1]) {
      throw new Error(
        `Enum column ${tableName}.${newColumn.name} has an invalid default`,
      );
    }
    statements.push(
      `ALTER TABLE ${table} ALTER COLUMN ${column} SET DEFAULT ${defaultMatch[1]}`,
    );
  } else if (
    !optionsChanged &&
    physicalMatches &&
    JSON.stringify(oldDefault) !== JSON.stringify(newDefault)
  ) {
    if (newDefault !== null && !defaultMatch?.[1]) {
      throw new Error(
        `Enum column ${tableName}.${newColumn.name} has an invalid default`,
      );
    }
    statements.push(
      newDefault === null
        ? `ALTER TABLE ${table} ALTER COLUMN ${column} DROP DEFAULT`
        : `ALTER TABLE ${table} ALTER COLUMN ${column} SET DEFAULT ${defaultMatch![1]}`,
    );
  }
  if (oldColumn?.isNullable !== newColumn?.isNullable) {
    statements.push(
      `ALTER TABLE ${table} ALTER COLUMN ${column} ${newColumn?.isNullable === false ? 'SET NOT NULL' : 'DROP NOT NULL'}`,
    );
  }
  return statements;
}
