import { asClass, asFunction } from 'awilix';
import type { Cradle } from '../cradle';
import {
  createEnfyraKernel,
  type ExecutorWorkerCrashRecord,
} from '@enfyra/kernel';
import { RuntimeScriptExecutorService } from '../../engines/cache';
import { Logger } from '../../shared/logger';
import { recordSystemError, recordUserLog } from '../../shared/runtime-log-buffer';

const executorLogger = new Logger('IsolatedExecutorService');

export function logExecutorWorkerCrash(
  record: ExecutorWorkerCrashRecord,
): void {
  const correlationId = record.activeTasks.find(
    (task) => task.correlationId,
  )?.correlationId;
  executorLogger.warn({
    systemError: true,
    message: 'Executor worker crashed',
    correlationId,
    data: record,
  });
}

export const kernelExecutorRegisters = {
  enfyraKernel: asFunction((cradle: Cradle) =>
    createEnfyraKernel({
      knexService: cradle.knexService,
      mongoService: cradle.mongoService,
      databaseConfigService: cradle.databaseConfigService,
      runtimeMetricsCollectorService: cradle.runtimeMetricsCollectorService,
      lazyRef: cradle.lazyRef,
      getPackageCacheService: () => ({
        getPackages: () => cradle.runtimeRegistryService.getPackages(),
      }),
      getPackageCdnLoaderService: () => cradle.packageCdnLoaderService,
      onExecutorWorkerCrash: logExecutorWorkerCrash,
      onExecutionFinished: (record) => {
        const block = record.diagnostics.scriptBlocks.find((item) => item.scriptId) ?? record.diagnostics.scriptBlocks[0];
        const metadata = { component: 'Script', correlationId: record.diagnostics.correlationId, sourceKind: block?.type, sourceId: block?.scriptId, statusCode: record.statusCode, truncated: record.logsTruncated };
        recordUserLog(record.logs, metadata);
        if (record.error && (!record.statusCode || record.statusCode >= 500)) {
          recordSystemError(record.error.message ?? 'Script execution failed', { ...metadata, ...record.error, details: record.diagnostics });
        }
      },
    }),
  ).singleton(),
  queryBuilderService: asFunction((cradle: Cradle) => cradle.enfyraKernel.queryBuilderService).singleton(),
  isolatedExecutorService: asFunction((cradle: Cradle) => cradle.enfyraKernel.isolatedExecutorService)
    .singleton()
    .disposer((service) => service.onDestroy()),
  kernelExecutorEngineService: asFunction((cradle: Cradle) => cradle.enfyraKernel.executorEngineService).singleton(),
  executorEngineService: asClass(RuntimeScriptExecutorService).singleton(),
} as const;
