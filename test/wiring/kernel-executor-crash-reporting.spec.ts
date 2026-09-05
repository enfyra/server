import { beforeEach, describe, expect, it, vi } from 'vitest';
import { Logger } from '../../src/shared/logger';
import { logExecutorWorkerCrash } from '../../src/wiring/registers/kernel-executor';

describe('kernel executor crash reporting', () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it('persists the structured crash record through the ESV logger', () => {
    const warn = vi.spyOn(Logger.prototype, 'warn').mockImplementation(() => {});
    const record = {
      event: 'executor_worker_crashed' as const,
      occurredAt: '2026-09-05T01:02:03.000Z',
      entryId: 7,
      pid: 123,
      exitCode: 1,
      exitSignal: null,
      lastHeapRatio: 0.91,
      lastRssBytes: 512_000_000,
      activeTasks: [{
        correlationId: 'req_worker_crash',
        timeoutMs: 600_000,
        isolateMemoryLimitMb: 1024,
        isolatePoolSize: 2,
        tasksPerIsolate: 8,
        scriptBlocks: [{ type: 'handler' as const, scriptId: '541' }],
      }],
    };

    logExecutorWorkerCrash(record);

    expect(warn).toHaveBeenCalledWith({
      systemError: true,
      message: 'Executor worker crashed',
      correlationId: 'req_worker_crash',
      data: record,
    });
  });
});
