

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
  
  foreignKeyColumn?: string;
  
  junctionTableName?: string;
  junctionSourceColumn?: string;
  junctionTargetColumn?: string;
}


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


export interface FilterPart {
  operator: 'AND' | 'OR';
  sql: string;
  params: Record<string, any>;
}

export interface ParsedFilter {
  parts: FilterPart[];
  joins: JoinInfo[];
}


export interface CascadeConfig {
  cascadeInsert: boolean;
  cascadeUpdate: boolean;
  cascadeDelete: boolean;
}


export interface MetadataCache {
  tables: Map<string, TableMetadata>;
  lastUpdated: number;
  ttl: number;
}


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

