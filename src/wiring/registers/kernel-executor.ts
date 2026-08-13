import { asClass, asFunction } from 'awilix';
import type { Cradle } from '../cradle';
import {
  createEnfyraKernel,
} from '@enfyra/kernel';
import { RuntimeScriptExecutorService } from '../../engines/cache';

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
    }),
  ).singleton(),
  queryBuilderService: asFunction((cradle: Cradle) => cradle.enfyraKernel.queryBuilderService).singleton(),
  isolatedExecutorService: asFunction((cradle: Cradle) => cradle.enfyraKernel.isolatedExecutorService)
    .singleton()
    .disposer((service) => service.onDestroy()),
  kernelExecutorEngineService: asFunction((cradle: Cradle) => cradle.enfyraKernel.executorEngineService).singleton(),
  executorEngineService: asClass(RuntimeScriptExecutorService).singleton(),
} as const;
