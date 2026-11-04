import type { RuntimeSchemaMutationContract } from '../../../modules/table-management/types/runtime-schema-mutation.types';

export interface MySqlBootstrapSnapshotContext {
  mutationId?: string;
  runtimeContract?: RuntimeSchemaMutationContract;
}

export interface MySqlBootstrapSnapshotRecoveryResult {
  rolledBackMutationIds: string[];
}
