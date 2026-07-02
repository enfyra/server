import { describe, expect, it, vi } from 'vitest';
import { DynamicRepository } from '../../src/modules/dynamic-api';

function makeRepo(
  overrides: Partial<ConstructorParameters<typeof DynamicRepository>[0]> = {},
) {
  const queryBuilderService = {
    getPkField: vi.fn(() => '_id'),
    find: vi.fn().mockResolvedValue({ data: [], count: 0 }),
  };
  const metadataCacheService = {
    lookupTableByName: vi.fn().mockResolvedValue({
      name: 'enfyra_route',
      columns: [{ name: '_id', isPrimary: true }],
      relations: [],
    }),
    getMetadata: vi.fn().mockResolvedValue({
      version: 1,
      tables: new Map(),
    }),
  };
  const runtimeRegistryService = {
    requireMetadata: vi.fn(() => ({
      version: 1,
      tables: new Map([
        [
          'enfyra_route',
          {
            name: 'enfyra_route',
            columns: [{ name: '_id', isPrimary: true }],
            relations: [],
          },
        ],
      ]),
      tablesList: [],
      timestamp: new Date(),
    })),
    lookupTableByName: vi.fn(() => ({
      name: 'enfyra_route',
      columns: [{ name: '_id', isPrimary: true }],
      relations: [],
    })),
  };
  const settingCacheService = {
    getMaxQueryDepth: vi.fn().mockResolvedValue(10),
  };
  return new DynamicRepository({
    context: { $query: {} } as any,
    tableName: 'enfyra_route',
    queryBuilderService: queryBuilderService as any,
    tableHandlerService: {} as any,
    policyService: {} as any,
    tableValidationService: {} as any,
    metadataCacheService: metadataCacheService as any,
    settingCacheService: settingCacheService as any,
    runtimeRegistryService: runtimeRegistryService as any,
    eventEmitter: {} as any,
    ...overrides,
  });
}

describe('DynamicRepository route method relations', () => {
  it('keeps Mongo string/ObjectId-like publicMethods when they are available', () => {
    const repo = makeRepo();
    const getId = '507f1f77bcf86cd799439011';
    const postId = '507f1f77bcf86cd799439012';
    const deleteId = '507f1f77bcf86cd799439013';
    const body = {
      availableMethods: [{ _id: getId }, { _id: postId }],
      publicMethods: [{ _id: getId }, postId, deleteId],
    };

    (repo as any).filterMethodsSubsetOfAvailable(body, null, 'publicMethods');

    expect(body.publicMethods).toEqual([{ _id: getId }, postId]);
  });

  it('uses existing availableMethods on update when only publicMethods changes', () => {
    const repo = makeRepo();
    const patchId = '507f1f77bcf86cd799439014';
    const deleteId = '507f1f77bcf86cd799439015';
    const body = {
      publicMethods: [{ id: patchId }, { id: deleteId }],
    };
    const existing = {
      availableMethods: [{ id: patchId }],
    };

    (repo as any).filterMethodsSubsetOfAvailable(
      body,
      existing,
      'publicMethods',
    );

    expect(body.publicMethods).toEqual([{ id: patchId }]);
  });

  it('checks existence with a direct find-style filter', async () => {
    const queryBuilderService = {
      getPkField: vi.fn(() => '_id'),
      find: vi.fn().mockResolvedValue({
        data: [{ _id: '507f1f77bcf86cd799439011' }],
        count: 1,
      }),
    };
    const repo = makeRepo({ queryBuilderService: queryBuilderService as any });
    const filter = { path: { _eq: '/health' } };

    await expect(repo.exists(filter)).resolves.toBe(true);

    expect(queryBuilderService.find).toHaveBeenCalledWith(
      expect.objectContaining({
        table: 'enfyra_route',
        filter,
        fields: ['_id'],
        limit: 1,
        sort: '_id',
      }),
    );
  });

  it('returns false when no record matches the exists filter', async () => {
    const queryBuilderService = {
      getPkField: vi.fn(() => '_id'),
      find: vi.fn().mockResolvedValue({ data: [], count: 0 }),
    };
    const repo = makeRepo({ queryBuilderService: queryBuilderService as any });

    await expect(repo.exists({ path: { _eq: '/missing' } })).resolves.toBe(
      false,
    );
  });

  it('accepts an exists filter wrapped in a filter property', async () => {
    const queryBuilderService = {
      getPkField: vi.fn(() => '_id'),
      find: vi.fn().mockResolvedValue({ data: [], count: 0 }),
    };
    const repo = makeRepo({ queryBuilderService: queryBuilderService as any });
    const filter = { path: { _eq: '/missing' } };

    await expect(repo.exists({ filter })).resolves.toBe(false);

    expect(queryBuilderService.find).toHaveBeenCalledWith(
      expect.objectContaining({
        filter,
      }),
    );
  });

  it('rejects exists without a filter', async () => {
    const repo = makeRepo();

    await expect(repo.exists()).rejects.toThrow(
      'exists requires a non-empty filter',
    );
  });

  it('rejects exists when the filter contains undefined values', async () => {
    const repo = makeRepo();

    await expect(
      repo.exists({ filter: { email: { _eq: undefined } } }),
    ).rejects.toThrow('exists filter cannot contain undefined values');
  });
});
