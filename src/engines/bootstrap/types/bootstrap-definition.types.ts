import type {
  ColumnModifyDef,
  RelationModifyDef,
  SchemaMigrationDef,
  TableModifyDef,
  TableRenameDef,
} from '../../../shared/types/schema-migration.types';

export type BootstrapSnapshot = Record<string, any>;
export type BootstrapDefaultData = Record<string, any>;
export type BootstrapDataMigration = Record<string, any>;

export interface BootstrapSourceArtifacts {
  snapshot: BootstrapSnapshot;
  migration: SchemaMigrationDef | null;
  defaultData: BootstrapDefaultData;
  dataMigration: BootstrapDataMigration;
}

export interface BootstrapDefinition {
  snapshot: BootstrapSnapshot;
  migration: SchemaMigrationDef | null;
  defaultData: BootstrapDefaultData;
  dataMigration: BootstrapDataMigration;
  dataTargetSnapshot: BootstrapSnapshot;
}

export interface BootstrapSchemaExecutionPlan {
  mode: 'install' | 'upgrade';
  database: 'postgresql' | 'mysql' | 'mongodb';
  targetTableCount: number;
  observedMetadata: {
    tables: number;
    columns: number;
    relations: number;
  };
  operations: readonly BootstrapSchemaOperation[];
  phases: readonly BootstrapSchemaExecutionPhase[];
}

interface BootstrapSchemaOperationBase {
  id: string;
  label: string;
}

export type BootstrapSchemaOperation =
  | (BootstrapSchemaOperationBase & {
      kind: 'rename-core-table';
      rename: TableRenameDef;
    })
  | (BootstrapSchemaOperationBase & {
      kind: 'rename-table';
      rename: TableRenameDef;
    })
  | (BootstrapSchemaOperationBase & {
      kind: 'rename-physical-table';
      rename: TableRenameDef;
    })
  | (BootstrapSchemaOperationBase & {
      kind: 'drop-physical-table';
      tableName: string;
    })
  | (BootstrapSchemaOperationBase & {
      kind: 'drop-table';
      tableName: string;
    })
  | (BootstrapSchemaOperationBase & {
      kind: 'modify-table';
      tableName: string;
      modification: TableModifyDef;
    })
  | (BootstrapSchemaOperationBase & {
      kind: 'modify-column';
      tableName: string;
      modification: ColumnModifyDef;
    })
  | (BootstrapSchemaOperationBase & {
      kind: 'remove-column';
      tableName: string;
      columnName: string;
    })
  | (BootstrapSchemaOperationBase & {
      kind: 'modify-relation';
      tableName: string;
      modification: RelationModifyDef;
    })
  | (BootstrapSchemaOperationBase & {
      kind: 'remove-relation';
      tableName: string;
      propertyName: string;
    });

export type BootstrapSchemaCommandKind =
  | 'rename-core-table'
  | 'rename-table'
  | 'rename-physical-table'
  | 'drop-physical-table'
  | 'apply-physical-change'
  | 'apply-metadata-change'
  | 'cleanup-renamed-table';

export interface BootstrapSchemaCommand {
  backend: BootstrapSchemaExecutionPlan['database'];
  kind: BootstrapSchemaCommandKind;
  operation: BootstrapSchemaOperation;
}

export interface BootstrapSchemaExecutionNode {
  id: string;
  changeId: string;
  dependsOn: readonly string[];
  phase: number;
  checkpoint: 'core' | 'remaining';
  completesChange: boolean;
  command: BootstrapSchemaCommand;
}

export interface BootstrapSchemaExecutionPhase {
  index: number;
  nodes: readonly BootstrapSchemaExecutionNode[];
}

export type BootstrapSchemaOperationCompleted = (
  operation: BootstrapSchemaOperation,
) => void | Promise<void>;

export type BootstrapChangeStage =
  | 'schema'
  | 'defaults'
  | 'handlers'
  | 'data'
  | 'attestation'
  | 'finalize';

export interface BootstrapPlannedChange {
  id: string;
  stage: BootstrapChangeStage;
  label: string;
}

export interface BootstrapChangePlan {
  changes: readonly BootstrapPlannedChange[];
}
