import type { Knex } from 'knex';
import { setSqlColumnNullable } from '../../../shared/utils/provision-schema-migration';
import {
  buildSqlForeignKeyContracts,
  buildSqlIndexContracts,
  buildSqlUniqueContracts,
} from '../../knex/utils/sql-physical-schema-contract';
import {
  compareSchemas,
  getCurrentDatabaseSchema,
} from '../../knex/utils/provision/schema-comparison';
import { parseSnapshotToSchema } from '../../knex/utils/provision/schema-parser';

const AUTO_TIMESTAMP_COLUMNS = ['createdAt', 'updatedAt'] as const;

function groupKey(columns: string[]): string {
  return columns.map((column) => column.toLowerCase()).join('|');
}

async function ensureAutoTimestampColumns(
  knex: Knex,
  tableName: string,
): Promise<number> {
  const missing: string[] = [];
  for (const columnName of AUTO_TIMESTAMP_COLUMNS) {
    if (!(await knex.schema.hasColumn(tableName, columnName))) {
      missing.push(columnName);
    }
  }
  if (missing.length === 0) return 0;

  const isPostgres = String(knex.client.config.client)
    .toLowerCase()
    .includes('pg');
  await knex.schema.alterTable(tableName, (table) => {
    for (const columnName of missing) {
      const column = isPostgres
        ? table.timestamp(columnName, { useTz: true })
        : table.timestamp(columnName);
      column.defaultTo(knex.fn.now());
    }
  });
  return missing.length;
}

async function repairTargetNullability(
  knex: Knex,
  tableName: string,
  definition: Record<string, any>,
): Promise<number> {
  const current = await getCurrentDatabaseSchema(knex, tableName);
  const currentColumns = new Map(
    current.columns.map((column) => [column.name, column]),
  );
  let repaired = 0;

  for (const column of definition.columns ?? []) {
    if (column.isPrimary) continue;
    const existing = currentColumns.get(column.name);
    const targetNullable = column.isNullable !== false;
    if (!existing || existing.isNullable === targetNullable) continue;
    await setSqlColumnNullable(knex, tableName, column.name, targetNullable);
    existing.isNullable = targetNullable;
    repaired++;
  }

  for (const foreignKey of buildSqlForeignKeyContracts(
    tableName,
    definition.relations ?? [],
  )) {
    const existing = currentColumns.get(foreignKey.columnName);
    if (!existing || existing.isNullable === foreignKey.nullable) continue;
    await setSqlColumnNullable(
      knex,
      tableName,
      foreignKey.columnName,
      foreignKey.nullable,
    );
    existing.isNullable = foreignKey.nullable;
    repaired++;
  }

  return repaired;
}

async function repairTargetIndexesAndUniques(
  knex: Knex,
  tableName: string,
  definition: Record<string, any>,
): Promise<number> {
  const current = await getCurrentDatabaseSchema(knex, tableName);
  const schema = { tableName, definition, junctionTables: [] };
  const diff = compareSchemas(schema as any, current);
  const target = {
    columns: definition.columns ?? [],
    relations: definition.relations ?? [],
    indexes: definition.indexes ?? [],
    uniques: definition.uniques ?? [],
  };
  const indexContracts = buildSqlIndexContracts(tableName, target);
  const uniqueContracts = buildSqlUniqueContracts(tableName, target);
  let repaired = 0;

  for (const columns of diff.uniquesToAdd) {
    const key = groupKey(columns);
    const contract = uniqueContracts.find(
      (candidate) => groupKey(candidate.physicalColumns) === key,
    );
    await knex.schema.alterTable(tableName, (table) => {
      table.unique(columns, contract?.name);
    });
    repaired++;
  }

  for (const columns of diff.indexesToAdd) {
    const key = groupKey(columns);
    const contract = indexContracts.find(
      (candidate) => groupKey(candidate.physicalColumns) === key,
    );
    await knex.schema.alterTable(tableName, (table) => {
      table.index(columns, contract?.name);
    });
    repaired++;
  }

  return repaired;
}

export async function repairSqlSystemPhysicalTarget(
  knex: Knex,
  snapshot: Record<string, any>,
): Promise<number> {
  let repaired = 0;
  for (const schema of parseSnapshotToSchema(snapshot)) {
    if (!schema.definition.isSystem) continue;
    if (!(await knex.schema.hasTable(schema.tableName))) continue;
    repaired += await ensureAutoTimestampColumns(knex, schema.tableName);
    repaired += await repairTargetNullability(
      knex,
      schema.tableName,
      schema.definition,
    );
    repaired += await repairTargetIndexesAndUniques(
      knex,
      schema.tableName,
      schema.definition,
    );
  }
  return repaired;
}
