/**
 * Common types and interfaces for Knex-based database operations
 * All types are based on data from table_definition, column_definition, relation_definition
 */

// ==================== COLUMN TYPES ====================

export interface ColumnMetadata {
  id: number;
  name: string;
  type: string;
  isPrimary: boolean;
  isGenerated: boolean;
  isNullable: boolean;
  isSystem: boolean;
  defaultValue?: any;
  options?: any[] | any;
  isUpdatable: boolean;
  isHidden?: boolean;
  description?: string;
  placeholder?: string;
  tableId: number;
}

// ==================== RELATION TYPES ====================

export interface RelationMetadata {
  id: number;
  propertyName: string;
  type: 'one-to-one' | 'many-to-one' | 'one-to-many' | 'many-to-many';
  targetTable: string;
  targetTableId: number;
  sourceTable: string;
  sourceTableId: number;
  inversePropertyName?: string;
  isNullable: boolean;
  isSystem: boolean;
  description?: string;
  
  // Foreign key column name (for many-to-one, one-to-one)
  foreignKeyColumn?: string;
  
  // For many-to-many relations
  junctionTableName?: string;
  junctionSourceColumn?: string;
  junctionTargetColumn?: string;
}

// ==================== TABLE TYPES ====================

export interface TableMetadata {
  id: number;
  name: string;
  isSystem: boolean;
  uniques?: string[][];
  indexes?: string[][];
  alias?: string;
  description?: string;
  columns: ColumnMetadata[];
  relations: RelationMetadata[];
}

// ==================== QUERY TYPES ====================

export interface QueryFilter {
  [key: string]: any;
  _and?: QueryFilter[];
  _or?: QueryFilter[];
  _not?: QueryFilter;
}

export interface QueryOptions {
  tableName: string;
  fields?: string | string[];
  filter?: QueryFilter;
  sort?: string | string[];
  page?: number;
  limit?: number;
  meta?: string;
  aggregate?: any;
  deep?: Record<string, any>;
}

export interface QueryResult<T = any> {
  data: T[];
  meta?: {
    totalCount?: number;
    filterCount?: number;
    page?: number;
    limit?: number;
  };
}

// ==================== JOIN TYPES ====================

export interface JoinInfo {
  tableName: string;
  alias: string;
  parentAlias: string;
  propertyPath: string;
  type: 'leftJoin' | 'innerJoin';
  onColumn: string;
  parentColumn: string;
}

export interface SelectField {
  alias: string;
  column: string;
  asName?: string;
}

// ==================== FILTER PARSING ====================

export interface FilterPart {
  operator: 'AND' | 'OR';
  sql: string;
  params: Record<string, any>;
}

export interface ParsedFilter {
  parts: FilterPart[];
  joins: JoinInfo[];
}

// ==================== CASCADE TYPES ====================

export interface CascadeConfig {
  cascadeInsert: boolean;
  cascadeUpdate: boolean;
  cascadeDelete: boolean;
}

// ==================== METADATA CACHE ====================

export interface MetadataCache {
  tables: Map<string, TableMetadata>;
  lastUpdated: number;
  ttl: number; // Time to live in milliseconds
}

// ==================== HELPER TYPES ====================

export type PrimaryKeyType = 'uuid' | 'integer' | 'bigint';

export interface ForeignKeyInfo {
  columnName: string;
  referencedTable: string;
  referencedColumn: string;
  onDelete: 'CASCADE' | 'SET NULL' | 'RESTRICT' | 'NO ACTION';
  onUpdate: 'CASCADE' | 'SET NULL' | 'RESTRICT' | 'NO ACTION';
}

export interface JunctionTableInfo {
  tableName: string;
  sourceTable: string;
  targetTable: string;
  sourceColumn: string;
  targetColumn: string;
  sourcePropertyName: string;
}

