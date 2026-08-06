import { asClass, asFunction } from 'awilix';
import {
  createEnfyraKernel,
} from '@enfyra/kernel';
import { RuntimeScriptExecutorService } from '../../engines/cache';

export const kernelExecutorRegisters = {
  enfyraKernel: asFunction((cradle: any) =>
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
  queryBuilderService: asFunction((cradle: any) => cradle.enfyraKernel.queryBuilderService).singleton(),
  isolatedExecutorService: asFunction((cradle: any) => cradle.enfyraKernel.isolatedExecutorService)
    .singleton()
    .disposer((service: any) => service.onDestroy()),
  kernelExecutorEngineService: asFunction((cradle: any) => cradle.enfyraKernel.executorEngineService).singleton(),
  executorEngineService: asClass(RuntimeScriptExecutorService).singleton(),
} as const;
