import type { Knex } from 'knex';
import type { MongoService } from '../../../engines/mongo';
import type { QueryBuilderService } from '@enfyra/kernel';
import { generateDefaultRecord } from '../utils/generate-default-record';
import { buildSqlJunctionTableContractFromRelation } from '../../../engines/knex/utils/sql-physical-schema-contract';
import { getPrimaryKeyTypeForTable } from '../../../engines/knex/utils/migration/pk-type.util';

export async function ensureSqlSingleRecord(input: {
  knex: Knex;
  tableName: string;
  columns: any[];
  collapseExtraRows?: boolean;
}): Promise<void> {
  const { knex, tableName, columns, collapseExtraRows } = input;
  const recordCount = await knex(tableName).count('* as count').first();
  const count = Number(recordCount?.count || 0);
  if (count === 0) {
    await knex(tableName).insert(generateDefaultRecord(columns || []));
    return;
  }
  if (!collapseExtraRows || count <= 1) return;
  const firstRecord = await knex(tableName).orderBy('id', 'asc').first('id');
  if (firstRecord?.id != null) {
    await knex(tableName).where('id', '!=', firstRecord.id).delete();
  }
}

export async function syncSqlGqlDefinition(input: {
  knex: Knex;
  tableId: number;
  isEnabled: boolean;
  isSystem: boolean;
}): Promise<void> {
  const { knex, tableId, isEnabled, isSystem } = input;
  const existingGql = await knex('gql_definition')
    .where({ tableId })
    .first();
  if (existingGql) {
    await knex('gql_definition')
      .where({ id: existingGql.id })
      .update({ isEnabled });
    return;
  }
  await knex('gql_definition').insert({
    tableId,
    isEnabled,
    isSystem,
  });
}

export async function ensureSqlM2mJunctionTables(input: {
  knex: Knex;
  tableMetadata: any;
  dbType: string;
  metadataCacheService?: any;
}): Promise<void> {
  const { knex, tableMetadata, dbType, metadataCacheService } = input;
  for (const relation of tableMetadata?.relations || []) {
    if (relation.type !== 'many-to-many' || relation.mappedBy || relation.mappedById) {
      continue;
    }
    const junction = buildSqlJunctionTableContractFromRelation(
      tableMetadata.name,
      relation,
    );
    if (!junction || (await knex.schema.hasTable(junction.tableName))) {
      continue;
    }
    const sourcePkType = await getPrimaryKeyTypeForTable(
      knex,
      junction.sourceTable,
      metadataCacheService,
    );
    const targetPkType = await getPrimaryKeyTypeForTable(
      knex,
      junction.targetTable,
      metadataCacheService,
    );
    await knex.schema.createTable(junction.tableName, (table) => {
      addSqlJunctionColumn(table, junction.sourceColumn, sourcePkType, dbType)
        .notNullable();
      addSqlJunctionColumn(table, junction.targetColumn, targetPkType, dbType)
        .notNullable();
      table.primary([junction.sourceColumn, junction.targetColumn], junction.primaryKeyName);
      table
        .foreign(junction.sourceColumn)
        .references('id')
        .inTable(junction.sourceTable)
        .onDelete(junction.onDelete)
        .onUpdate(junction.onUpdate)
        .withKeyName(junction.sourceForeignKeyName);
      table
        .foreign(junction.targetColumn)
        .references('id')
        .inTable(junction.targetTable)
        .onDelete(junction.onDelete)
        .onUpdate(junction.onUpdate)
        .withKeyName(junction.targetForeignKeyName);
      table.index([junction.sourceColumn], junction.sourceIndexName);
      table.index([junction.targetColumn], junction.targetIndexName);
      table.index(
        [junction.targetColumn, junction.sourceColumn],
        junction.reverseIndexName,
      );
    });
  }
}

function addSqlJunctionColumn(
  table: Knex.CreateTableBuilder,
  name: string,
  pkType: 'uuid' | 'int',
  dbType: string,
) {
  if (pkType === 'uuid') {
    return dbType === 'postgres' ? table.uuid(name) : table.string(name, 36);
  }
  return dbType === 'mysql' ? table.integer(name).unsigned() : table.integer(name);
}

export async function ensureMongoSingleRecord(input: {
  mongoService: MongoService;
  tableName: string;
  columns: any[];
  collapseExtraRows?: boolean;
}): Promise<void> {
  const { mongoService, tableName, columns, collapseExtraRows } = input;
  const db = mongoService.getDb();
  const count = await db.collection(tableName).countDocuments();
  if (count === 0) {
    await db.collection(tableName).insertOne(generateDefaultRecord(columns || []));
    return;
  }
  if (!collapseExtraRows || count <= 1) return;
  const firstRecord = await db
    .collection(tableName)
    .find()
    .sort({ _id: 1 })
    .limit(1)
    .toArray();
  if (firstRecord[0]?._id) {
    await db.collection(tableName).deleteMany({ _id: { $ne: firstRecord[0]._id } });
  }
}

export async function syncMongoGqlDefinition(input: {
  mongoService: MongoService;
  queryBuilderService: QueryBuilderService;
  tableId: any;
  isEnabled: boolean;
  isSystem: boolean;
}): Promise<void> {
  const { mongoService, queryBuilderService, tableId, isEnabled, isSystem } =
    input;
  const db = mongoService.getDb();
  const existingGql = await queryBuilderService.findOne({
    table: 'gql_definition',
    where: { table: tableId },
  });
  if (existingGql) {
    await db.collection('gql_definition').updateOne(
      { _id: existingGql._id },
      {
        $set: {
          isEnabled,
          updatedAt: new Date(),
        },
      },
    );
    return;
  }
  await db.collection('gql_definition').insertOne({
    table: tableId,
    isEnabled,
    isSystem,
    createdAt: new Date(),
    updatedAt: new Date(),
  });
}
