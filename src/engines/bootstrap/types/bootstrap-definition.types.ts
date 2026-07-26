import type { SchemaMigrationDef } from '../../../shared/types/schema-migration.types';

export type BootstrapSnapshot = Record<string, any>;
export type BootstrapDefaultData = Record<string, any[]>;
export type BootstrapDataMigration = Record<string, any>;

export interface BootstrapDefinition {
  snapshot: BootstrapSnapshot;
  migration: SchemaMigrationDef | null;
  defaultData: BootstrapDefaultData;
  dataMigration: BootstrapDataMigration;
  dataTargetSnapshot: BootstrapSnapshot;
}

export interface BootstrapSchemaExecutionPlan {
  mode: 'install' | 'upgrade';
  database: 'sql' | 'mongodb';
  targetTableCount: number;
  observedMetadata: {
    tables: number;
    columns: number;
    relations: number;
  };
  operations: {
    tableRenames: readonly string[];
    physicalTableRenames: readonly string[];
    physicalTableDrops: readonly string[];
    tableMigrations: readonly string[];
    tableDrops: readonly string[];
  };
}
