import type { CacheOrchestratorConfig } from '../types/cache-orchestrator-config.types';

export const CACHE_ORCHESTRATOR_DEFAULTS: CacheOrchestratorConfig = {
  invalidationDebounceMs: 50,
  minimumClientReloadStatusMs: 500,
  minimumFullReloadStatusMs: 200,
  fullReloadBuilderConcurrency: 3,
  signalRetryMaxAttempts: 3,
  signalRetryBaseDelayMs: 1000,
  signalRetryMaxDelayMs: 5000,
};
