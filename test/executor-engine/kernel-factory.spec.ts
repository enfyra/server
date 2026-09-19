import { createEnfyraKernel } from '@enfyra/kernel';

function dependencies(overrides: Record<string, unknown> = {}) {
  return {
    databaseConfigService: {
      getDbType: () => 'sqlite' as const,
      isMongoDb: () => false,
    },
    lazyRef: {},
    ...overrides,
  };
}

describe('Kernel factory lifecycle', () => {
  it('validates executor dependencies only when the executor is requested', async () => {
    const kernel = createEnfyraKernel(dependencies());

    expect(kernel.queryBuilderService).toBeDefined();
    expect(() => kernel.isolatedExecutorService).toThrow(
      'Executor dependencies packageCacheService and packageCdnLoaderService are required',
    );

    await kernel.onDestroy();
  });

  it('flushes query batches before destroying the executor', async () => {
    const kernel = createEnfyraKernel(dependencies({
      packageCacheService: { getPackages: async () => [] },
      packageCdnLoaderService: { getPackageSources: () => [] },
    }));
    const order: string[] = [];
    vi.spyOn(kernel.queryBuilderService, 'flushBatchInserts')
      .mockImplementation(async () => {
        order.push('flush');
      });
    vi.spyOn(kernel.isolatedExecutorService, 'onDestroy')
      .mockImplementation(async () => {
        order.push('executor');
      });

    await kernel.onDestroy();

    expect(order).toEqual(['flush', 'executor']);
  });

  it('rejects service access after destruction', async () => {
    const kernel = createEnfyraKernel(dependencies());

    await kernel.onDestroy();

    expect(() => kernel.isolatedExecutorService).toThrow(
      'Enfyra kernel has been destroyed',
    );
    expect(() => kernel.executorEngineService).toThrow(
      'Enfyra kernel has been destroyed',
    );
  });
});
