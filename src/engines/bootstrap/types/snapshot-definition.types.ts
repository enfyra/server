import type { ColumnTypeDeclaration } from '../../../shared/types/column-type.types';

export type SnapshotRelationType =
  | 'one-to-one'
  | 'many-to-one'
  | 'one-to-many'
  | 'many-to-many';

export type SnapshotOnDelete = 'CASCADE' | 'RESTRICT' | 'SET NULL';

/**
 * A column declares both backends explicitly and in the same shape. The snapshot
 * materializes exactly one `type` field per database, so nothing downstream
 * branches on the backend.
 */
export interface SnapshotColumnDefinition {
  name: string;
  sqlType: ColumnTypeDeclaration;
  mongoType: ColumnTypeDeclaration;
  isPrimary?: boolean;
  isGenerated?: boolean;
  isNullable?: boolean;
  isSystem?: boolean;
  isUpdatable?: boolean;
  isPublished?: boolean;
  isEncrypted?: boolean;
  defaultValue?: unknown;
  options?: unknown;
  description?: string;
  placeholder?: string;
}

export interface SnapshotRelationDefinition {
  propertyName: string;
  type: SnapshotRelationType;
  targetTable: string;
  inversePropertyName?: string;
  isNullable?: boolean;
  isSystem?: boolean;
  isGenerated?: boolean;
  isUpdatable?: boolean;
  isPublished?: boolean;
  onDelete?: SnapshotOnDelete;
  description?: string;
  foreignKeyColumn?: string;
  referencedColumn?: string;
  constraintName?: string;
  junctionTableName?: string;
  junctionSourceColumn?: string;
  junctionTargetColumn?: string;
  metadata?: unknown;
}

export interface SnapshotTableDefinition {
  name: string;
  description?: string;
  isSystem?: boolean;
  isSingleRecord?: boolean;
  uniques?: string[][];
  indexes?: string[][];
  alias?: string;
  metadata?: unknown;
  validateBody?: boolean;
  columns: SnapshotColumnDefinition[];
  relations?: SnapshotRelationDefinition[];
}

export interface SnapshotTableOptions {
  description?: string;
  system?: boolean;
  singleRecord?: boolean;
  alias?: string;
  metadata?: unknown;
  validateBody?: boolean;
}

export interface SnapshotJunctionOptions {
  table: string;
  source: string;
  target: string;
}
