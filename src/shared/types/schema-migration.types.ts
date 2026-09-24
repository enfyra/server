/**
 * Schema Migration Types
 *
 * For dangerous operations only:
 * - Remove: columns, relations, tables
 * - Modify: rename or change properties
 *
 * Adding is handled automatically by the snapshot target
 */

import type {
  ColumnTypeDeclaration,
  MongoColumnTypeMigration,
} from './column-type.types';

/**
 * One side of a column modification. Types are declared per backend the same way
 * the snapshot declares them, and the bootstrap projects the single `type` field
 * the active backend reads.
 */
export interface ColumnModificationDef {
  name: string;
  sqlType?: ColumnTypeDeclaration;
  mongoType?: ColumnTypeDeclaration;
  [key: string]: any;
}

/**
 * Column modification - from state to target state
 * Only fields present in "from" and "to" are compared/changed
 */
export interface ColumnModifyDef {
  from: ColumnModificationDef;
  to: ColumnModificationDef;
}

/**
 * Relation modification - from state to target state
 */
export interface RelationModifyDef {
  from: {
    propertyName: string;
    [key: string]: any;
  };
  to: {
    propertyName: string;
    [key: string]: any;
  };
}

export interface TableModifyDef {
  from: {
    [key: string]: any;
  };
  to: {
    [key: string]: any;
  };
}

export interface TableRenameDef {
  from: string;
  to: string;
  mergeKeys?: string[];
}

/**
 * Table migration definition
 */
export interface TableMigrationDef {
  /**
   * Unique identifier to find the table
   */
  _unique: {
    name: {
      _eq: string;
    };
  };

  tableToModify?: TableModifyDef;

  /**
   * Modify columns (rename, change metadata properties, or reapply a physical type contract).
   */
  columnsToModify?: ColumnModifyDef[];

  /**
   * Remove columns (WARNING: data loss)
   */
  columnsToRemove?: string[];

  /**
   * Modify relations
   */
  relationsToModify?: RelationModifyDef[];

  /**
   * Remove relations
   */
  relationsToRemove?: string[];
}

export interface SnapshotMigrationMetadataState {
  tables: Array<Record<string, any>>;
  columns: Array<Record<string, any> & { tableName: string }>;
  relations: Array<
    Record<string, any> & {
      sourceTableName: string;
      targetTableName?: string;
      mappedByPropertyName?: string;
      inversePropertyName?: string;
    }
  >;
}

/**
 * Schema migration file structure
 */
export interface SchemaMigrationDef {
  /**
   * Mongo-only metadata type migrations applied to every persisted column.
   */
  mongoColumnTypesToModify?: MongoColumnTypeMigration[];

  /**
   * Core metadata tables must be renamed before any normal metadata query.
   */
  coreTablesToRename?: TableRenameDef[];

  /**
   * Table renames applied after core metadata tables are available.
   */
  tablesToRename?: TableRenameDef[];

  /**
   * Physical tables/collections that no longer have metadata but may exist in old installs.
   */
  physicalTablesToDrop?: string[];

  /**
   * Physical tables/collections without metadata that need a direct rename.
   */
  physicalTablesToRename?: TableRenameDef[];

  /**
   * Table migrations
   */
  tables: TableMigrationDef[];

  /**
   * Tables to drop completely (WARNING: data loss)
   */
  tablesToDrop?: string[];
}

/**
 * One upgrade step: the declarations that carry a database from `fromVersion` to
 * `toVersion`. Steps are ordered and contiguous, and only the steps newer than the
 * version recorded in `enfyra_setting.enfyraVersion` are applied.
 */
export interface VersionedSchemaMigration {
  fromVersion: string;
  toVersion: string;
  schema: SchemaMigrationDef;
}

/**
 * Record-level counterpart of `VersionedSchemaMigration`: the records a step
 * rewrites, deletes, or backfills.
 */
export interface VersionedDataMigration {
  fromVersion: string;
  toVersion: string;
  data: Record<string, any>;
}

export interface MongoPhysicalMigrationOptions {
  preserveFieldsByCollection?: Record<string, string[]>;
}
