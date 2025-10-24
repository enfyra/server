/**
 * Shared types for database initialization scripts
 */

export interface ColumnDef {
  name: string;
  type: string;
  isPrimary?: boolean;
  isGenerated?: boolean;
  isNullable?: boolean;
  isSystem?: boolean;
  defaultValue?: any;
  options?: any[] | any; // For enum type
  isUpdatable?: boolean;
  isUnique?: boolean;
  isHidden?: boolean;
  description?: string;
  placeholder?: string;
}

export interface RelationDef {
  propertyName: string;
  type: 'one-to-one' | 'many-to-one' | 'one-to-many' | 'many-to-many';
  targetTable: string;
  inversePropertyName?: string;
  mappedBy?: string;
  isNullable?: boolean;
  isSystem?: boolean;
  isInverseEager?: boolean;
}

export interface TableDef {
  name: string;
  isSystem?: boolean;
  uniques?: string[][];
  indexes?: string[][];
  columns: ColumnDef[];
  relations?: RelationDef[];
}

export interface JunctionTableDef {
  tableName: string;
  sourceTable: string;
  targetTable: string;
  sourceColumn: string;
  targetColumn: string;
  sourcePropertyName: string;
}

export interface KnexTableSchema {
  tableName: string;
  definition: TableDef;
  junctionTables: JunctionTableDef[];
}

