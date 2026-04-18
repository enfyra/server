import { IOperationLog, IRollbackResult } from './mongo-operation-log.service';

export interface ISagaContext {
  txId: string;
  status:
    | 'active'
    | 'committing'
    | 'rolling_back'
    | 'completed'
    | 'aborted'
    | 'failed';
  lockedResources: Set<string>;
  operations: IOperationLog[];
  modifiedDocuments: Array<{ collection: string; id: string }>;
  metadata: {
    startedAt: Date;
    lastActivityAt: Date;
    maxDurationMs: number;
  };
}

export interface ISagaOptions {
  maxDurationMs?: number;
  lockTimeoutMs?: number;
  maxRetries?: number;
  waitTimeout?: number;
  autoRollbackOnError?: boolean;
}

export interface ISagaResult<T> {
  success: boolean;
  data?: T;
  error?: Error;
  txId: string;
  rollbackResult?: IRollbackResult;
  stats?: {
    durationMs: number;
    operationsCount: number;
    locksAcquired: number;
  };
}

export interface ISagaRecoveryMetrics {
  totalRuns: number;
  bootRuns: number;
  periodicRuns: number;
  skippedDueToRedisLock: number;
  lastRunAt: Date | null;
  lastCleaned: number;
  lastRecovered: number;
  lastError: string | null;
}
