export type SqlRelationOnDelete = 'CASCADE' | 'SET NULL' | 'RESTRICT';

export type SqlPhysicalIndexSource =
  | 'metadata'
  | 'relation-fk'
  | 'id-suffix-column'
  | 'system-timestamp'
  | 'temporal-column';

export interface SqlPhysicalIndexContract {
  name: string;
  logicalColumns: string[];
  physicalColumns: string[];
  source: SqlPhysicalIndexSource;
}

export interface SqlPhysicalUniqueContract {
  name: string;
  logicalColumns: string[];
  physicalColumns: string[];
}

export interface SqlForeignKeyContract {
  tableName: string;
  propertyName: string;
  columnName: string;
  constraintName: string;
  targetTable: string;
  targetColumn: string;
  onDelete: SqlRelationOnDelete;
  onUpdate: 'CASCADE';
  nullable: boolean;
}

export interface SqlJunctionTableContract {
  tableName: string;
  sourceTable: string;
  targetTable: string;
  sourceColumn: string;
  targetColumn: string;
  primaryKeyName: string;
  sourceForeignKeyName: string;
  targetForeignKeyName: string;
  sourceIndexName: string;
  targetIndexName: string;
  reverseIndexName: string;
  onDelete: 'CASCADE';
  onUpdate: 'CASCADE';
}
