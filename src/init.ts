/**
 * Boot orchestration — thin facade.
 *
 * Step implementations live in `src/wiring/init/phases.ts` grouped by phase.
 * This file only sequences the phases (order matters!) and re-exports shutdown.
 */
import type { AwilixContainer } from 'awilix';
import type { Cradle } from './container';
import type { StartupStep } from './shared/types/startup.types';
import { runStartupSteps } from './shared/startup-log';
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
  phaseBootstrapScripts,
  phaseFlowAndGraphql,
  phaseDeferredParallel,
  phaseReady,
} from './wiring/init/phases';

function bootstrapSteps(c: Cradle): StartupStep[] {
  return [
    {
      label: 'connecting storage',
      run: () => {
        c.runtimeLogWriterService.start();
        return phaseStorageEngines(c);
      },
    },
    { label: 'connecting Redis', run: () => phaseRedisAndNamespace(c) },
    { label: 'recovering transactions', run: () => phaseSaga(c) },
    { label: 'checking provision state', run: () => phaseProvisionGate(c) },
    { label: 'assessing metadata', run: () => phaseLegacyAssessment(c) },
    { label: 'initializing schema', run: () => phaseFirstRun(c) },
  ];
}

export async function init(
  container: AwilixContainer<Cradle>,
  finalSteps: readonly StartupStep[] = [],
): Promise<void> {
  const c = container.cradle;
  await runStartupSteps([
    ...bootstrapSteps(c),
    {
      label: 'checking runtime logs',
      run: () => c.runtimeLogWriterService.assertReady(),
    },
    { label: 'repairing interrupted reloads', run: () => phaseReloadRepair(c) },
    { label: 'loading metadata', run: () => phaseCoreCaches(c) },
    {
      label: 'initializing runtime services',
      run: () => phaseRuntimeSideEffects(c),
    },
    { label: 'loading runtime caches', run: () => phaseParallelCacheReload(c) },
    {
      label: 'activating caches',
      run: () => phasePublishActivatedSnapshots(c),
    },
    {
      label: 'executing bootstrap scripts',
      run: () => phaseBootstrapScripts(c),
    },
    {
      label: 'initializing flows and GraphQL',
      run: () => phaseFlowAndGraphql(c),
    },
    {
      label: 'initializing background services',
      run: () => phaseDeferredParallel(c),
    },
    { label: 'publishing readiness', run: () => phaseReady(c) },
    ...finalSteps,
  ]);
}

export async function initBootstrap(
  container: AwilixContainer<Cradle>,
): Promise<void> {
  await runStartupSteps(bootstrapSteps(container.cradle));
}

export async function shutdown(
  container: AwilixContainer<Cradle>,
): Promise<void> {
  const redis = container.cradle.redis;
  let shutdownError: unknown;
  const operations = [
    () => container.cradle.flowExecutionQueueService?.onDestroy?.(),
    () => container.cradle.queryBuilderService?.flushBatchInserts?.(),
    () => container.cradle.runtimeLogWriterService.onDestroy(),
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
