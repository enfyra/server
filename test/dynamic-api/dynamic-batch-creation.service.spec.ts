import { describe, expect, it, vi } from 'vitest';
import { DynamicBatchCreationService } from '../../src/modules/dynamic-api/services/dynamic-batch-creation.service';

function createService(routerHandles = false) {
  const mutationPreparationService = {
    prepareCreateBody: vi.fn(async (row: Record<string, unknown>) => row),
  };
  const queryBuilderService = {
    insert: vi.fn().mockResolvedValue(undefined),
  };
  const runtimeMetadataSchemaRouterService = {
    handles: vi.fn(() => routerHandles),
  };
  const service = new DynamicBatchCreationService(
    mutationPreparationService as never,
    {} as never,
    {} as never,
    { getStrategy: vi.fn(() => ({ kind: 'generic' })) } as never,
    runtimeMetadataSchemaRouterService as never,
    queryBuilderService as never,
  );

  return {
    mutationPreparationService,
    queryBuilderService,
    runtimeMetadataSchemaRouterService,
    service,
  };
}

describe('DynamicBatchCreationService', () => {
  it('rejects batch create for schema-routed tables before preparing rows', async () => {
    const { service, mutationPreparationService, queryBuilderService } =
      createService(true);

    await expect(
      service.createBatch('enfyra_column', {}, [{ name: 'slug' }]),
    ).rejects.toThrow(/Batch create is not supported/);

    expect(mutationPreparationService.prepareCreateBody).not.toHaveBeenCalled();
    expect(queryBuilderService.insert).not.toHaveBeenCalled();
  });

  it('prepares every row before inserting it in batch mode', async () => {
    const { service, mutationPreparationService, queryBuilderService } =
      createService();
    mutationPreparationService.prepareCreateBody.mockImplementation(
      async (row: Record<string, unknown>) => ({ ...row, normalized: true }),
    );

    await expect(
      service.createBatch('enfyra_route', {}, [{ name: 'a' }, { name: 'b' }]),
    ).resolves.toEqual({ accepted: true, batch: true, count: 2 });

    expect(mutationPreparationService.prepareCreateBody).toHaveBeenCalledTimes(
      2,
    );
    expect(queryBuilderService.insert).toHaveBeenNthCalledWith(
      1,
      'enfyra_route',
      { name: 'a', normalized: true },
      { batch: true },
    );
    expect(queryBuilderService.insert).toHaveBeenNthCalledWith(
      2,
      'enfyra_route',
      { name: 'b', normalized: true },
      { batch: true },
    );
  });
});
