import type { BootstrapSourceArtifacts } from '../../../src/engines/bootstrap/types/bootstrap-definition.types';

export type BootstrapRandomizedOperationKind =
  | 'table-add'
  | 'table-drop'
  | 'table-rename'
  | 'column-add'
  | 'column-remove'
  | 'column-rename'
  | 'column-modify'
  | 'relation-add'
  | 'relation-remove'
  | 'relation-rename'
  | 'index-modify'
  | 'unique-modify';

export interface BootstrapRandomizedOperation {
  kind: BootstrapRandomizedOperationKind;
  table: string;
  from?: string;
  to?: string;
}

export interface BootstrapRandomizedSentinel {
  parentLabel: string;
  parentCounter: number;
  childPayload: string;
  retiredValue: string;
}

export interface BootstrapRandomizedAssertions {
  sourceParentTable: string;
  targetParentTable: string;
  childTable: string;
  droppedTable: string;
  addedTable: string;
  renamedColumn: { from: string; to: string };
  modifiedColumn: string;
  removedParentColumn: string;
  removedChildColumn: string;
  addedColumn: string;
  renamedRelation: { from: string; to: string };
  removedRelation: string;
  addedRelation: string;
  healing: {
    table: string;
    metadataColumn: string;
    expectedDescription: string;
    physicalColumn: string;
    indexColumns: string[];
  };
  sentinel: BootstrapRandomizedSentinel;
}

export interface RandomizedBootstrapScenario {
  seed: number;
  prefix: string;
  source: BootstrapSourceArtifacts;
  target: BootstrapSourceArtifacts;
  operations: BootstrapRandomizedOperation[];
  assertions: BootstrapRandomizedAssertions;
}

export type BootstrapMatrixDatabase = 'postgres' | 'mysql' | 'mongodb';

export interface BootstrapRandomizedMatrixConfig {
  seed: number;
  cases: number;
  databases: BootstrapMatrixDatabase[];
}

export interface BootstrapMatrixPhysicalIndex {
  name: string;
  columns: string[];
  unique: boolean;
}
