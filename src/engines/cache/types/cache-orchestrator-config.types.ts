export interface CacheOrchestratorConfig {
  invalidationDebounceMs: number;
  minimumClientReloadStatusMs: number;
  minimumFullReloadStatusMs: number;
  fullReloadBuilderConcurrency: number;
  signalRetryMaxAttempts: number;
  signalRetryBaseDelayMs: number;
  signalRetryMaxDelayMs: number;
}
