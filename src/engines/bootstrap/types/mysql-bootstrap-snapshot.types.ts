export interface MySqlBootstrapSnapshotContext {
  mutationId?: string;
}

export interface MySqlBootstrapSnapshotRecoveryResult {
  rolledBackMutationIds: string[];
}
