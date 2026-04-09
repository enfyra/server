import { Knex } from 'knex';
import {
  TableMetadata,
  ColumnMetadata,
  RelationMetadata,
  MetadataCache,
  JunctionTableInfo,
} from '../types/knex-types';
import {
  getJunctionTableName,
  getForeignKeyColumnName,
} from './sql-schema-naming.util';
export async function loadAllTableMetadata(
  knex: Knex,
): Promise<Map<string, TableMetadata>> {
  const tables = await knex('table_definition').select('*');
  const metadataMap = new Map<string, TableMetadata>();
  for (const table of tables) {
    const metadata = await loadTableMetadata(knex, table.name);
    if (metadata) {
      metadataMap.set(table.name, metadata);
    }
  }
  return metadataMap;
}
export async function loadTableMetadata(
  knex: Knex,
  tableName: string,
): Promise<TableMetadata | null> {
  const tableDef = await knex('table_definition')
    .where('name', tableName)
    .first();
  if (!tableDef) {
    return null;
  }
  const columns = await knex('column_definition')
    .where('tableId', tableDef.id)
    .select('*')
    .orderBy('id', 'asc');
  const relations = await knex('relation_definition as r')
    .select(
      'r.*',
      'sourceTable.name as sourceTableName',
      'targetTable.name as targetTableName',
    )
    .leftJoin(
      'table_definition as sourceTable',
      'r.sourceTableId',
      'sourceTable.id',
    )
    .leftJoin(
      'table_definition as targetTable',
      'r.targetTableId',
      'targetTable.id',
    )
    .where('r.sourceTableId', tableDef.id);
  const columnMetadata: ColumnMetadata[] = columns.map((col) => ({
    id: col.id,
    name: col.name,
    type: col.type,
    isPrimary: col.isPrimary || false,
    isGenerated: col.isGenerated || false,
    isNullable: col.isNullable !== false,
    isSystem: col.isSystem || false,
    defaultValue: col.defaultValue,
    options: col.options,
    isUpdatable: col.isUpdatable !== false,
    isHidden: col.isHidden || false,
    description: col.description,
    placeholder: col.placeholder,
    tableId: col.tableId,
  }));
  const relationMetadata: RelationMetadata[] = relations.map((rel) => ({
    id: rel.id,
    propertyName: rel.propertyName,
    type: rel.type,
    targetTable: rel.targetTableName,
    targetTableId: rel.targetTableId,
    sourceTable: rel.sourceTableName,
    sourceTableId: rel.sourceTableId,
    inversePropertyName: rel.inversePropertyName,
    isNullable: rel.isNullable !== false,
    isSystem: rel.isSystem || false,
    onDelete: rel.onDelete || 'SET NULL',
  }));
  return {
    id: tableDef.id,
    name: tableDef.name,
    isSystem: tableDef.isSystem || false,
    uniques: tableDef.uniques,
    indexes: tableDef.indexes,
    alias: tableDef.alias,
    description: tableDef.description,
    columns: columnMetadata,
    relations: relationMetadata,
  };
}
export function getPrimaryKeyColumn(
  metadata: TableMetadata,
): ColumnMetadata | null {
  return metadata.columns.find((col) => col.isPrimary) || null;
}
export function getPrimaryKeyType(
  metadata: TableMetadata,
): 'uuid' | 'integer' | 'bigint' {
  const pkColumn = getPrimaryKeyColumn(metadata);
  if (!pkColumn) return 'integer';
  if (pkColumn.type === 'uuid') return 'uuid';
  if (pkColumn.type === 'bigint') return 'bigint';
  return 'integer';
}
export function getManyToManyRelations(
  metadata: TableMetadata,
): RelationMetadata[] {
  return metadata.relations.filter((rel) => rel.type === 'many-to-many');
}
export function getJunctionTableInfo(
  relation: RelationMetadata,
  allMetadata: Map<string, TableMetadata>,
): JunctionTableInfo | null {
  if (relation.type !== 'many-to-many') {
    return null;
  }
  const junctionTableName = getJunctionTableName(
    relation.sourceTable,
    relation.propertyName,
    relation.targetTable,
  );
  const sourceMeta = allMetadata.get(relation.sourceTable);
  const targetMeta = allMetadata.get(relation.targetTable);
  if (!sourceMeta || !targetMeta) {
    return null;
  }
  return {
    tableName: junctionTableName,
    sourceTable: relation.sourceTable,
    targetTable: relation.targetTable,
    sourceColumn: getForeignKeyColumnName(relation.propertyName),
    targetColumn: getForeignKeyColumnName(relation.targetTable),
    sourcePropertyName: relation.propertyName,
  };
}
export function findRelation(
  metadata: TableMetadata,
  propertyName: string,
): RelationMetadata | null {
  return (
    metadata.relations.find((rel) => rel.propertyName === propertyName) || null
  );
}
export function findColumn(
  metadata: TableMetadata,
  columnName: string,
): ColumnMetadata | null {
  return metadata.columns.find((col) => col.name === columnName) || null;
}
export function isRelation(
  metadata: TableMetadata,
  fieldName: string,
): boolean {
  return metadata.relations.some((rel) => rel.propertyName === fieldName);
}
export function getForeignKeyColumn(relation: RelationMetadata): string | null {
  if (relation.type === 'many-to-many' || relation.type === 'one-to-many') {
    return null;
  }
  return getForeignKeyColumnName(relation.propertyName);
}
export function createMetadataCache(ttl: number = 60000): MetadataCache {
  return {
    tables: new Map(),
    lastUpdated: Date.now(),
    ttl,
  };
}
export function isCacheValid(cache: MetadataCache): boolean {
  return Date.now() - cache.lastUpdated < cache.ttl;
}
export async function refreshMetadataCache(
  knex: Knex,
  cache: MetadataCache,
): Promise<void> {
  const tables = await loadAllTableMetadata(knex);
  cache.tables = tables;
  cache.lastUpdated = Date.now();
}
export async function getTableMetadataWithCache(
  knex: Knex,
  tableName: string,
  cache?: MetadataCache,
): Promise<TableMetadata | null> {
  if (cache && isCacheValid(cache)) {
    return cache.tables.get(tableName) || null;
  }
  return await loadTableMetadata(knex, tableName);
}
