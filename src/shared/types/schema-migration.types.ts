/**
 * Schema Migration Types
 *
 * For dangerous operations only:
 * - Remove: columns, relations, tables
 * - Modify: rename or change properties
 *
 * Adding is handled automatically by snapshot.json diff
 */

/**
 * Column modification - from state to target state
 * Only fields present in "from" and "to" are compared/changed
 */
export interface ColumnModifyDef {
  from: {
    name: string;
    [key: string]: any;
  };
  to: {
    name: string;
    [key: string]: any;
  };
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

  /**
   * Modify columns (rename, change properties)
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

/**
 * Schema migration file structure
 */
export interface SchemaMigrationDef {
  /**
   * Table migrations
   */
  tables: TableMigrationDef[];

  /**
   * Tables to drop completely (WARNING: data loss)
   */
  tablesToDrop?: string[];
}
