import type {
  SchemaMutationNodeOutputs,
} from '../../../shared/types/schema-mutation-contract.types';
import type {
  RuntimeSchemaMutationContract,
  RuntimeSchemaMutationCommand,
} from './runtime-schema-mutation.types';

export type RuntimeSchemaJournalStage =
  | 'captured'
  | 'executing'
  | 'target_attested'
  | 'db_committed'
  | 'activation_pending'
  | 'activated'
  | 'completed'
  | 'rolled_back'
  | 'failed';

export interface RuntimeSchemaJournalEntry {
  mutationId: string;
  contractHash: string;
  backend: string;
  stage: RuntimeSchemaJournalStage;
  startedAt: string;
  updatedAt: string;
  completedNodeIds: readonly string[];
  error?: string;
}

export interface RuntimeSchemaExecutionResult {
  mutationId: string;
  contractHash: string;
  outputs: SchemaMutationNodeOutputs;
  affectedTables: readonly string[];
  recordId?: string | number;
  preview?: Record<string, unknown>;
}

export interface RuntimeSchemaCommandAdapterContext {
  contract: RuntimeSchemaMutationContract;
  command: RuntimeSchemaMutationCommand;
  nodeId: string;
  outputs: Map<string, Record<string, unknown>>;
}

export interface RuntimeSchemaCommandAdapter {
  execute(
    context: RuntimeSchemaCommandAdapterContext,
  ): Promise<Record<string, unknown> | void>;
}
