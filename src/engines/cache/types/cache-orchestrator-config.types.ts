export interface CacheOrchestratorConfig {
  invalidationDebounceMs: number;
  invalidationDebounceMaxWaitMs: number;
  minimumClientReloadStatusMs: number;
  minimumFullReloadStatusMs: number;
  fullReloadBuilderConcurrency: number;
  signalRetryMaxAttempts: number;
  signalRetryBaseDelayMs: number;
  signalRetryMaxDelayMs: number;
  signalRetryBudgetMs: number;
  sharedGenerationReconcileMs: number;
}
