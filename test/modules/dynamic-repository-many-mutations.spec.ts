import { describe, expect, it, vi } from 'vitest';
import { DynamicRepository } from '../../src/modules/dynamic-api';

function makeRepo(tableName = 'project_task') {
  const queryBuilderService = {
    getPkField: vi.fn(() => 'id'),
    find: vi.fn().mockResolvedValue({
      data: [
        { id: 11, title: 'First' },
        { id: 12, title: 'Second' },
      ],
      count: 2,
    }),
    createMany: vi.fn().mockResolvedValue([11, 12]),
    updateMany: vi.fn().mockResolvedValue(2),
    deleteMany: vi.fn().mockResolvedValue(2),
    runWithPolicy: vi.fn(async (_check: unknown, callback: () => unknown) =>
      callback(),
    ),
  };
  const eventEmitter = { emit: vi.fn() };
  const runtimeRegistryService = {
    lookupTableByName: vi.fn(() => ({
      name: tableName,
      columns: [{ name: 'id', isPrimary: true }],
      relations: [],
    })),
    getMaxQueryDepth: vi.fn(() => 10),
  };
  const repo = new DynamicRepository({
    context: { $query: {} } as any,
    tableName,
    queryBuilderService: queryBuilderService as any,
    runtimeMetadataSchemaRouterService: {
      handles: vi.fn(() => false),
    } as any,
    policyService: {
      checkMutationSafety: vi.fn().mockResolvedValue({ allowed: true }),
    } as any,
    tableValidationService: {
      assertTableValid: vi.fn().mockResolvedValue(undefined),
    } as any,
    guardValidationService: {} as any,
    runtimeRegistryService: runtimeRegistryService as any,
    eventEmitter: eventEmitter as any,
    bcryptService: { hash: vi.fn() } as any,
  });
  vi.spyOn(repo as any, 'ensureInit').mockResolvedValue(undefined);
  vi.spyOn(repo as any, 'find').mockResolvedValue({
    data: [
      { id: 11, title: 'Updated first' },
      { id: 12, title: 'Updated second' },
    ],
    count: 2,
  });
  vi.spyOn(repo as any, 'reload').mockResolvedValue(undefined);
  vi.spyOn(repo as any, 'emitTableMutation').mockImplementation(() => {});
  (repo as any).tableMetadata = {
    columns: [{ name: 'id', isPrimary: true }],
    relations: [],
  };
  return { repo, queryBuilderService };
}

describe('DynamicRepository many mutations', () => {
  it('creates many records through the kernel batch primitive and reloads once', async () => {
    const { repo, queryBuilderService } = makeRepo();

    await expect(
      repo.createMany({
        data: [{ title: 'First' }, { title: 'Second' }],
      }),
    ).resolves.toEqual({
      data: [
        { id: 11, title: 'Updated first' },
        { id: 12, title: 'Updated second' },
      ],
      count: 2,
    });

    expect(queryBuilderService.createMany).toHaveBeenCalledWith(
      'project_task',
      [{ title: 'First' }, { title: 'Second' }],
    );
    expect((repo as any).reload).toHaveBeenCalledTimes(1);
    expect((repo as any).reload).toHaveBeenCalledWith({ ids: [11, 12] });
    expect((repo as any).emitTableMutation).toHaveBeenCalledWith(
      'create',
      [11, 12],
      undefined,
    );
  });

  it('validates every target before updating many records and reloads once', async () => {
    const { repo, queryBuilderService } = makeRepo();

    await expect(
      repo.updateMany({ ids: [11, 12], data: { title: 'Updated' } }),
    ).resolves.toMatchObject({ count: 2 });

    expect(queryBuilderService.find).toHaveBeenCalledWith({
      table: 'project_task',
      fields: '*',
      filter: { id: { _in: [11, 12] } },
      limit: -1,
    });
    expect(queryBuilderService.updateMany).toHaveBeenCalledWith(
      'project_task',
      [11, 12],
      { title: 'Updated' },
      'id',
    );
    expect((repo as any).reload).toHaveBeenCalledWith({ ids: [11, 12] });
  });

  it('rejects a missing target before deleting any record', async () => {
    const { repo, queryBuilderService } = makeRepo();
    queryBuilderService.find.mockResolvedValueOnce({
      data: [{ id: 11, title: 'First' }],
      count: 1,
    });

    await expect(repo.deleteMany({ ids: [11, 12] })).rejects.toThrow(
      'id 12 is not exists!',
    );

    expect(queryBuilderService.deleteMany).not.toHaveBeenCalled();
    expect((repo as any).reload).not.toHaveBeenCalled();
  });

  it('rejects a table with specialised mutation lifecycle', async () => {
    const { repo, queryBuilderService } = makeRepo('enfyra_user');

    await expect(
      repo.updateMany({ ids: [11], data: { roles: ['admin'] } }),
    ).rejects.toThrow('Bulk mutations are not supported for enfyra_user');

    expect(queryBuilderService.updateMany).not.toHaveBeenCalled();
  });
});
