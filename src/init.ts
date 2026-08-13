/**
 * Boot orchestration — thin facade.
 *
 * Step implementations live in `src/wiring/init/phases.ts` grouped by phase.
 * This file only sequences the phases (order matters!) and re-exports shutdown.
 */
import type { AwilixContainer } from 'awilix';
import type { Cradle } from './container';
import {
  phaseStorageEngines,
  phaseRedisAndNamespace,
  phaseSaga,
  phaseProvisionGate,
  phaseLegacyAssessment,
  phaseFirstRun,
  phaseReloadRepair,
  phaseCoreCaches,
  phaseRuntimeSideEffects,
  phaseParallelCacheReload,
  phasePublishActivatedSnapshots,
  phaseFlowAndGraphql,
  phaseDeferredParallel,
  phaseReady,
} from './wiring/init/phases';

export async function init(container: AwilixContainer<Cradle>): Promise<void> {
  const c = container.cradle;

  await initBootstrap(container);

  await phaseReloadRepair(c);
  await phaseCoreCaches(c);
  await phaseRuntimeSideEffects(c);
  await phaseParallelCacheReload(c);
  await phasePublishActivatedSnapshots(c);
  await phaseFlowAndGraphql(c);
  await phaseDeferredParallel(c);
  await phaseReady(c);
}

export async function initBootstrap(container: AwilixContainer<Cradle>): Promise<void> {
  const c = container.cradle;

  await phaseStorageEngines(c);
  await phaseRedisAndNamespace(c);
  await phaseSaga(c);
  await phaseProvisionGate(c);
  await phaseLegacyAssessment(c);
  await phaseFirstRun(c);
}

export async function shutdown(container: AwilixContainer<Cradle>): Promise<void> {
  const redis = container.cradle.redis;
  let shutdownError: unknown;
  const operations = [
    () => container.cradle.flowExecutionQueueService?.onDestroy?.(),
    () => container.cradle.queryBuilderService?.flushBatchInserts?.(),
    () => container.dispose(),
  ];

  for (const operation of operations) {
    try {
      await operation();
    } catch (error) {
      shutdownError ??= error;
    }
  }

  if (shutdownError) {
    redis.disconnect();
    throw shutdownError;
  }

  try {
    await redis.quit();
  } catch (error) {
    redis.disconnect();
    throw error;
  }
}
