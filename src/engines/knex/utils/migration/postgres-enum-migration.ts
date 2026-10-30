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

function escapeSqlLiteral(value: string): string {
  return value.replace(/'/g, "''");
}

function buildPostgresEnumValuesSql(options: readonly string[]): string {
  return options.map((option) => `'${escapeSqlLiteral(option)}'`).join(', ');
}

function isOrderPreservingEnumExpansion(
  currentValues: readonly string[],
  nextValues: readonly string[],
): boolean {
  if (currentValues.length === 0 || nextValues.length < currentValues.length) {
    return false;
  }
  let cursor = 0;
  for (const value of nextValues) {
    if (value === currentValues[cursor]) {
      cursor += 1;
      if (cursor === currentValues.length) {
        return true;
      }
    }
  }
  return cursor === currentValues.length;
}

function buildPostgresEnumAddValueStatements(
  typeName: string,
  currentValues: readonly string[],
  nextValues: readonly string[],
): string[] {
  const quotedType = quoteIdentifier(typeName, 'postgres');
  const known = [...currentValues];
  const statements: string[] = [];
  for (let i = 0; i < nextValues.length; i += 1) {
    const value = nextValues[i]!;
    if (known.includes(value)) continue;
    const escapedValue = escapeSqlLiteral(value);
    if (i === 0) {
      const firstKnown = escapeSqlLiteral(known[0]!);
      statements.push(
        `ALTER TYPE ${quotedType} ADD VALUE IF NOT EXISTS '${escapedValue}' BEFORE '${firstKnown}'`,
      );
      known.unshift(value);
    } else {
      const prevValue = escapeSqlLiteral(nextValues[i - 1]!);
      statements.push(
        `ALTER TYPE ${quotedType} ADD VALUE IF NOT EXISTS '${escapedValue}' AFTER '${prevValue}'`,
      );
      const prevIndex = known.indexOf(nextValues[i - 1]!);
      known.splice(prevIndex + 1, 0, value);
    }
  }
  return statements;
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

export { planPostgresEnumTypeCreation } from '../sql-enum.util';

export async function planPostgresEnumUpdate(
  knex: Knex,
  tableName: string,
  oldColumn: any,
  newColumn: any,
  sourceTableName: string = tableName,
): Promise<string[]> {
  const options = getEnumOptions(newColumn);
  const sourceColumnName = oldColumn?.name || newColumn.name;
  const current = await readPostgresEnumColumnState(
    knex,
    sourceTableName,
    sourceColumnName,
  );
  if (!current) {
    throw new Error(
      `Column ${sourceTableName}.${sourceColumnName} does not exist`,
    );
  }
  const isNativeEnumSuperset =
    current.dataType === 'USER-DEFINED' &&
    current.enumValues.length > 0 &&
    current.enumValues.every((value) => options.includes(value));

  if (
    !isNativeEnumSuperset &&
    (await hasSqlValuesOutsideEnumOptions(
      knex,
      sourceTableName,
      sourceColumnName,
      options,
    ))
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
  const typeName = getPostgresEnumTypeName(tableName, newColumn.name);
  const oldDefault = oldColumn?.defaultValue ?? null;
  const newDefault = newColumn?.defaultValue ?? null;
  const canExpandInPlace =
    current.dataType === 'USER-DEFINED' &&
    current.udtName === typeName &&
    isOrderPreservingEnumExpansion(current.enumValues, options) &&
    (newDefault === null || current.enumValues.includes(String(newDefault)));
  const statements: string[] = [];

  if (optionsChanged || !physicalMatches) {
    if (canExpandInPlace) {
      statements.push(
        ...buildPostgresEnumAddValueStatements(
          typeName,
          current.enumValues,
          options,
        ),
      );
    } else {
      if (current.defaultValue !== null) {
        statements.push(
          `ALTER TABLE ${table} ALTER COLUMN ${column} DROP DEFAULT`,
        );
      }
      const checkConstraints = await findPostgresColumnCheckConstraintNames(
        knex,
        sourceTableName,
        sourceColumnName,
      );
      for (const constraintName of checkConstraints) {
        statements.push(
          `ALTER TABLE ${table} DROP CONSTRAINT IF EXISTS ${quoteIdentifier(constraintName, 'postgres')}`,
        );
      }
      statements.push(
        `ALTER TABLE ${table} ALTER COLUMN ${column} TYPE text USING ${column}::text`,
      );

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
  }

  const definition = buildPostgresEnumColumnDefinition(tableName, newColumn);
  const defaultMatch = definition.match(/\s+DEFAULT\s+(.+)$/i);
  if ((optionsChanged || !physicalMatches) && !canExpandInPlace && newDefault !== null) {
    if (!defaultMatch?.[1]) {
      throw new Error(
        `Enum column ${tableName}.${newColumn.name} has an invalid default`,
      );
    }
    statements.push(
      `ALTER TABLE ${table} ALTER COLUMN ${column} SET DEFAULT ${defaultMatch[1]}`,
    );
  } else if (
    (canExpandInPlace || (!optionsChanged && physicalMatches)) &&
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
