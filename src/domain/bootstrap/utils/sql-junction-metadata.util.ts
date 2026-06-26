import { getForeignKeyColumnName, getJunctionTableName } from '@enfyra/kernel';
import { buildSqlJunctionTableContract } from '../../../engines/knex/utils/sql-physical-schema-contract';
import { getSqlJunctionPhysicalNames } from '../../../modules/table-management/utils/sql-junction-naming.util';
import type { IQueryBuilder } from '../../shared/interfaces/query-builder.interface';

async function tableHasColumns(
  knex: any,
  tableName: string,
  columns: string[],
): Promise<boolean> {
  if (!(await knex.schema.hasTable(tableName))) return false;
  const checks = await Promise.all(
    columns.map((column) => knex.schema.hasColumn(tableName, column)),
  );
  return checks.every(Boolean);
}

function getSqlDbType(knex: any): 'mysql' | 'postgres' {
  const client = String(knex?.client?.config?.client || '').toLowerCase();
  return client.includes('mysql') ? 'mysql' : 'postgres';
}

async function getSqlPrimaryKeyType(
  knex: any,
  tableName: string,
): Promise<'uuid' | 'varchar' | 'integer'> {
  const columnInfo = await knex(tableName).columnInfo();
  const idColumn = columnInfo.id || columnInfo._id;
  const type = String(idColumn?.type || '').toLowerCase();
  if (type.includes('uuid')) return 'uuid';
  if (
    type.includes('char') ||
    type.includes('text') ||
    type.includes('string')
  ) {
    return 'varchar';
  }
  return 'integer';
}

function addSqlJunctionColumn(
  table: any,
  columnName: string,
  pkType: 'uuid' | 'varchar' | 'integer',
  dbType: string,
) {
  if (pkType === 'uuid') {
    return dbType === 'postgres'
      ? table.uuid(columnName)
      : table.string(columnName, 36);
  }
  if (pkType === 'varchar') {
    return table.string(columnName, 255);
  }
  return dbType === 'mysql'
    ? table.integer(columnName).unsigned()
    : table.integer(columnName);
}

async function ensureSqlJunctionTable(
  queryBuilderService: IQueryBuilder,
  input: {
    sourceTable: string;
    targetTable: string;
    propertyName: string;
    junctionTable: string;
    sourceColumn: string;
    targetColumn: string;
  },
): Promise<void> {
  const knex = queryBuilderService.getKnex();
  const tableExists = await knex.schema.hasTable(input.junctionTable);
  if (
    tableExists &&
    (await tableHasColumns(knex, input.junctionTable, [
      input.sourceColumn,
      input.targetColumn,
    ]))
  ) {
    return;
  }

  if (tableExists) {
    throw new Error(
      `Junction table ${input.junctionTable} exists but is missing ${input.sourceColumn} or ${input.targetColumn}`,
    );
  }

  const [sourcePkType, targetPkType] = await Promise.all([
    getSqlPrimaryKeyType(knex, input.sourceTable),
    getSqlPrimaryKeyType(knex, input.targetTable),
  ]);
  const dbType = getSqlDbType(knex);
  const junction = buildSqlJunctionTableContract({
    tableName: input.junctionTable,
    sourceTable: input.sourceTable,
    targetTable: input.targetTable,
    sourceColumn: input.sourceColumn,
    targetColumn: input.targetColumn,
    sourcePropertyName: input.propertyName,
  });

  await knex.schema.createTable(input.junctionTable, (table: any) => {
    addSqlJunctionColumn(
      table,
      input.sourceColumn,
      sourcePkType,
      dbType,
    ).notNullable();
    addSqlJunctionColumn(
      table,
      input.targetColumn,
      targetPkType,
      dbType,
    ).notNullable();
    table.primary(
      [input.sourceColumn, input.targetColumn],
      junction.primaryKeyName,
    );
    table
      .foreign(input.sourceColumn)
      .references('id')
      .inTable(input.sourceTable)
      .onDelete(junction.onDelete)
      .onUpdate(junction.onUpdate)
      .withKeyName(junction.sourceForeignKeyName);
    table
      .foreign(input.targetColumn)
      .references('id')
      .inTable(input.targetTable)
      .onDelete(junction.onDelete)
      .onUpdate(junction.onUpdate)
      .withKeyName(junction.targetForeignKeyName);
    table.index([input.sourceColumn], junction.sourceIndexName);
    table.index([input.targetColumn], junction.targetIndexName);
    table.index(
      [input.targetColumn, input.sourceColumn],
      junction.reverseIndexName,
    );
  });
}

async function normalizeSqlRelationJunctionMetadata(
  queryBuilderService: IQueryBuilder,
  relation: any,
  standard: {
    junctionTableName: string;
    junctionSourceColumn: string;
    junctionTargetColumn: string;
  },
): Promise<void> {
  if (!relation?.id) return;
  const knex = queryBuilderService.getKnex();
  const updateData: any = {};
  if (relation.junctionTableName !== standard.junctionTableName) {
    updateData.junctionTableName = standard.junctionTableName;
  }
  if (relation.junctionSourceColumn !== standard.junctionSourceColumn) {
    updateData.junctionSourceColumn = standard.junctionSourceColumn;
  }
  if (relation.junctionTargetColumn !== standard.junctionTargetColumn) {
    updateData.junctionTargetColumn = standard.junctionTargetColumn;
  }
  if (Object.keys(updateData).length > 0) {
    await knex('enfyra_relation').where({ id: relation.id }).update(updateData);
  }

  if (relation.mappedById) return;
  await knex('enfyra_relation').where({ mappedById: relation.id }).update({
    junctionTableName: standard.junctionTableName,
    junctionSourceColumn: standard.junctionTargetColumn,
    junctionTargetColumn: standard.junctionSourceColumn,
  });
}

export async function getSqlJunctionMetadata(
  queryBuilderService: IQueryBuilder,
  input: {
    sourceTable: string;
    propertyName: string;
    targetTable: string;
  },
): Promise<{
  junctionTable: string;
  sourceColumn: string;
  targetColumn: string;
}> {
  const knex = queryBuilderService.getKnex();
  const relation = await knex('enfyra_relation as r')
    .leftJoin(
      'enfyra_table as sourceTable',
      'r.sourceTableId',
      'sourceTable.id',
    )
    .leftJoin(
      'enfyra_table as targetTable',
      'r.targetTableId',
      'targetTable.id',
    )
    .select(
      'r.junctionTableName as junctionTableName',
      'r.junctionSourceColumn as junctionSourceColumn',
      'r.junctionTargetColumn as junctionTargetColumn',
    )
    .where('sourceTable.name', input.sourceTable)
    .where('targetTable.name', input.targetTable)
    .where('r.propertyName', input.propertyName)
    .first();

  const standard = getSqlJunctionPhysicalNames({
    sourceTable: input.sourceTable,
    propertyName: input.propertyName,
    targetTable: input.targetTable,
  });
  const metadataCandidate = {
    junctionTableName:
      relation?.junctionTableName || standard.junctionTableName,
    junctionSourceColumn:
      relation?.junctionSourceColumn || standard.junctionSourceColumn,
    junctionTargetColumn:
      relation?.junctionTargetColumn || standard.junctionTargetColumn,
  };
  const metadataReady = await tableHasColumns(
    knex,
    metadataCandidate.junctionTableName,
    [
      metadataCandidate.junctionSourceColumn,
      metadataCandidate.junctionTargetColumn,
    ],
  );
  const standardReady = await tableHasColumns(
    knex,
    standard.junctionTableName,
    [standard.junctionSourceColumn, standard.junctionTargetColumn],
  );

  const selected =
    standardReady || !metadataReady
      ? {
          junctionTableName: standard.junctionTableName,
          junctionSourceColumn: standard.junctionSourceColumn,
          junctionTargetColumn: standard.junctionTargetColumn,
        }
      : metadataCandidate;

  await ensureSqlJunctionTable(queryBuilderService, {
    sourceTable: input.sourceTable,
    targetTable: input.targetTable,
    propertyName: input.propertyName,
    junctionTable: selected.junctionTableName,
    sourceColumn: selected.junctionSourceColumn,
    targetColumn: selected.junctionTargetColumn,
  });
  if (selected.junctionTableName === standard.junctionTableName) {
    await normalizeSqlRelationJunctionMetadata(
      queryBuilderService,
      relation,
      selected,
    );
  }

  return {
    junctionTable:
      selected.junctionTableName ||
      getJunctionTableName(
        input.sourceTable,
        input.propertyName,
        input.targetTable,
      ),
    sourceColumn:
      selected.junctionSourceColumn ||
      getForeignKeyColumnName(input.sourceTable),
    targetColumn:
      selected.junctionTargetColumn ||
      getForeignKeyColumnName(input.targetTable),
  };
}
