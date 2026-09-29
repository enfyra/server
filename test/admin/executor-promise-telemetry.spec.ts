import { beforeEach, describe, expect, it, vi } from 'vitest';
import { asValue, createContainer } from 'awilix';
import { acknowledgeRuntimeLog, peekRuntimeLogs } from '../../src/shared/runtime-log-buffer';
import { kernelExecutorRegisters } from '../../src/wiring/registers/kernel-executor';

vi.mock('@enfyra/kernel', () => ({ createEnfyraKernel: vi.fn((options) => options) }));

const clear = () => {
  for (const event of peekRuntimeLogs(3000)) acknowledgeRuntimeLog(event.record.eventId);
};

describe('executor promise diagnostics', () => {
  beforeEach(clear);

  it('persists bounded internal metadata without adding it to the public error', () => {
    const diagnostics = {
      phase: 'handler', failurePhase: 'handler', lastHostCall: 'pkgStreamCall', lastCompletedHostCall: 'helpersCall',
      pendingHostCalls: { pkgStreamCall: 1 }, pendingHostOperations: { 'packageStream.next': 1 },
      failureLastHostOperation: 'undici.request', failurePendingHostOperations: { 'undici.request': 1 },
      pendingTrackingTruncated: false, hostCallsStarted: 3, hostCallsCompleted: 2,
      workerPid: 42, heapRatio: 0.86, activeTasks: 2,
    };
    const container = createContainer();
    container.register({
      enfyraKernel: kernelExecutorRegisters.enfyraKernel,
      knexService: asValue({}), mongoService: asValue({}),
      databaseConfigService: asValue({}), runtimeMetricsCollectorService: asValue({}),
      lazyRef: asValue({}), runtimeRegistryService: asValue({ getPackages: () => [] }),
      packageCdnLoaderService: asValue({}),
    });
    const configured = container.resolve<any>('enfyraKernel');
    const record = {
      diagnostics: { correlationId: 'req_test', scriptBlocks: [{ type: 'handler', scriptId: '541' }] },
      logs: [], logsTruncated: false, statusCode: 200,
      error: { message: 'Promise was abandoned' },
      promiseDiagnostics: diagnostics,
    };
    configured.onExecutionFinished(record);
    const [event] = peekRuntimeLogs();
    expect(event.record.details).toMatchObject({ promiseDiagnostics: diagnostics });
    expect(record.error).not.toHaveProperty('promiseDiagnostics');
    expect(event.record.correlationId).toBe('req_test');
  });
});
