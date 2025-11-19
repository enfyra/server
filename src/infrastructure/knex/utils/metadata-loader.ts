/**
 * Metadata loader for loading table/column/relation metadata from database
 * This replaces TypeORM's EntityMetadata with DB-driven metadata
 */

import { Knex } from 'knex';
import {
  TableMetadata,
  ColumnMetadata,
  RelationMetadata,
  MetadataCache,
  JunctionTableInfo,
} from '../types/knex-types';
import { getJunctionTableName, getForeignKeyColumnName } from './naming-helpers';

/**
 * Load all table metadata from database
 */
export async function loadAllTableMetadata(knex: Knex): Promise<Map<string, TableMetadata>> {
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

/**
 * Load metadata for a specific table
 */
export async function loadTableMetadata(
  knex: Knex,
  tableName: string,
): Promise<TableMetadata | null> {
  // Load table definition
  const tableDef = await knex('table_definition')
    .where('name', tableName)
    .first();

  if (!tableDef) {
    return null;
  }

  // Load columns
  const columns = await knex('column_definition')
    .where('tableId', tableDef.id)
    .select('*')
    .orderBy('id', 'asc');

  // Load relations where this table is the source
  const relations = await knex('relation_definition as r')
    .select(
      'r.*',
      'sourceTable.name as sourceTableName',
      'targetTable.name as targetTableName',
    )
    .leftJoin('table_definition as sourceTable', 'r.sourceTableId', 'sourceTable.id')
    .leftJoin('table_definition as targetTable', 'r.targetTableId', 'targetTable.id')
    .where('r.sourceTableId', tableDef.id);

  // Map to our metadata format
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

/**
 * Get primary key column for a table
 */
export function getPrimaryKeyColumn(metadata: TableMetadata): ColumnMetadata | null {
  return metadata.columns.find((col) => col.isPrimary) || null;
}

/**
 * Get primary key type for a table
 */
export function getPrimaryKeyType(metadata: TableMetadata): 'uuid' | 'integer' | 'bigint' {
  const pkColumn = getPrimaryKeyColumn(metadata);
  if (!pkColumn) return 'integer';

  if (pkColumn.type === 'uuid') return 'uuid';
  if (pkColumn.type === 'bigint') return 'bigint';
  return 'integer';
}

/**
 * Get all many-to-many relations for a table
 */
export function getManyToManyRelations(metadata: TableMetadata): RelationMetadata[] {
  return metadata.relations.filter((rel) => rel.type === 'many-to-many');
}

/**
 * Get junction table info for a many-to-many relation
 */
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

/**
 * Find relation by property name
 */
export function findRelation(
  metadata: TableMetadata,
  propertyName: string,
): RelationMetadata | null {
  return metadata.relations.find((rel) => rel.propertyName === propertyName) || null;
}

/**
 * Find column by name
 */
export function findColumn(metadata: TableMetadata, columnName: string): ColumnMetadata | null {
  return metadata.columns.find((col) => col.name === columnName) || null;
}

/**
 * Check if a field is a relation or a regular column
 */
export function isRelation(metadata: TableMetadata, fieldName: string): boolean {
  return metadata.relations.some((rel) => rel.propertyName === fieldName);
}

/**
 * Get foreign key column name for a relation
 */
export function getForeignKeyColumn(relation: RelationMetadata): string | null {
  if (relation.type === 'many-to-many' || relation.type === 'one-to-many') {
    return null; // No FK column on source table
  }

  // Use propertyName for FK column (handles multiple FKs to same table)
  return getForeignKeyColumnName(relation.propertyName);
}

/**
 * Create metadata cache with TTL
 */
export function createMetadataCache(ttl: number = 60000): MetadataCache {
  return {
    tables: new Map(),
    lastUpdated: Date.now(),
    ttl,
  };
}

/**
 * Check if metadata cache is still valid
 */
export function isCacheValid(cache: MetadataCache): boolean {
  return Date.now() - cache.lastUpdated < cache.ttl;
}

/**
 * Refresh metadata cache
 */
export async function refreshMetadataCache(
  knex: Knex,
  cache: MetadataCache,
): Promise<void> {
  const tables = await loadAllTableMetadata(knex);
  cache.tables = tables;
  cache.lastUpdated = Date.now();
}

/**
 * Get table metadata with caching
 */
export async function getTableMetadataWithCache(
  knex: Knex,
  tableName: string,
  cache?: MetadataCache,
): Promise<TableMetadata | null> {
  if (cache && isCacheValid(cache)) {
    return cache.tables.get(tableName) || null;
  }

  // Load fresh
  return await loadTableMetadata(knex, tableName);
}

