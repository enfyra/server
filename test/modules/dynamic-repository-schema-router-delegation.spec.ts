import { describe, expect, it, vi } from 'vitest';
import { DynamicRepository } from '../../src/modules/dynamic-api';

function makeRepo(
  tableName: string,
  overrides: {
    routerHandles?: boolean;
    routerResult?: Record<string, unknown>;
  } = {},
) {
  const routerHandles = overrides.routerHandles ?? true;
  const routerResult = overrides.routerResult ?? {
    recordId: 42,
    ownerTableId: 10,
    affectedTables: ['user'],
  };
  const runtimeMetadataSchemaRouterService = {
    handles: vi.fn(() => routerHandles),
    create: vi.fn().mockResolvedValue(routerResult),
    update: vi.fn().mockResolvedValue(routerResult),
    delete: vi.fn().mockResolvedValue(routerResult),
  };
  const queryBuilderService = {
    getPkField: vi.fn(() => 'id'),
    find: vi.fn().mockResolvedValue({ data: [], count: 0 }),
    findOne: vi.fn().mockResolvedValue({ id: 42, name: 'x' }),
    create: vi.fn().mockResolvedValue({ id: 42 }),
    insert: vi.fn().mockResolvedValue({ id: 42 }),
    update: vi.fn().mockResolvedValue(undefined),
    delete: vi.fn().mockResolvedValue(undefined),
    runWithPolicy: vi.fn(async (_check: any, fn: any) => fn()),
  };
  const runtimeRegistryService = {
    requireMetadata: vi.fn(() => ({
      version: 1,
      tables: new Map([
        [
          tableName,
          {
            name: tableName,
            columns: [{ name: 'id', isPrimary: true }],
            relations: [],
          },
        ],
      ]),
      tablesList: [],
      timestamp: new Date(),
    })),
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
    tableHandlerService: {} as any,
    runtimeMetadataSchemaRouterService:
      runtimeMetadataSchemaRouterService as any,
    policyService: {
      checkMutationSafety: vi.fn().mockResolvedValue({ allowed: true }),
    } as any,
    tableValidationService: {
      assertTableValid: vi.fn().mockResolvedValue(undefined),
    } as any,
    runtimeRegistryService: runtimeRegistryService as any,
    eventEmitter: { emit: vi.fn() } as any,
  });
  vi.spyOn(repo as any, 'ensureInit').mockResolvedValue(undefined);
  vi.spyOn(repo as any, 'reload').mockResolvedValue(undefined);
  vi.spyOn(repo as any, 'find').mockResolvedValue({
    data: [{ id: 42 }],
    count: 1,
  });
  (repo as any).tableMetadata = { columns: [] };
  return {
    repo,
    runtimeMetadataSchemaRouterService,
    queryBuilderService,
  };
}

describe('DynamicRepository schema router delegation', () => {
  it('delegates enfyra_column create to the router, never generic CRUD', async () => {
    const { repo, runtimeMetadataSchemaRouterService, queryBuilderService } =
      makeRepo('enfyra_column');
    await repo.create({ data: { name: 'slug', type: 'varchar', table: 10 } });
    expect(runtimeMetadataSchemaRouterService.create).toHaveBeenCalledOnce();
    expect(queryBuilderService.insert).not.toHaveBeenCalled();
  });

  it('delegates enfyra_column update to the router, never generic CRUD', async () => {
    const { repo, runtimeMetadataSchemaRouterService, queryBuilderService } =
      makeRepo('enfyra_column');
    await repo.update({ id: 42, data: { name: 'heading' } });
    expect(runtimeMetadataSchemaRouterService.update).toHaveBeenCalledOnce();
    expect(queryBuilderService.update).not.toHaveBeenCalled();
  });

  it('delegates enfyra_relation delete to the router, never generic CRUD', async () => {
    const { repo, runtimeMetadataSchemaRouterService, queryBuilderService } =
      makeRepo('enfyra_relation');
    await repo.delete({ id: 7 });
    expect(runtimeMetadataSchemaRouterService.delete).toHaveBeenCalledOnce();
    expect(queryBuilderService.delete).not.toHaveBeenCalled();
  });

  it('returns preview from the router without executing writes', async () => {
    const preview = { _preview: true, requiredConfirmHash: 'abc' };
    const { repo, runtimeMetadataSchemaRouterService, queryBuilderService } =
      makeRepo('enfyra_column', {
        routerResult: { preview, ownerTableId: 10 },
      });
    const result = await repo.create({
      data: { name: 'slug', type: 'varchar', table: 10 },
    });
    expect(result).toEqual({ data: [preview] });
    expect(queryBuilderService.insert).not.toHaveBeenCalled();
    expect(runtimeMetadataSchemaRouterService.create).toHaveBeenCalledOnce();
  });

  it('does not route non-schema tables through the router', async () => {
    const { repo, runtimeMetadataSchemaRouterService, queryBuilderService } =
      makeRepo('enfyra_route', { routerHandles: false });
    await repo.create({ data: { name: 'test' } });
    expect(runtimeMetadataSchemaRouterService.create).not.toHaveBeenCalled();
    expect(queryBuilderService.insert).toHaveBeenCalled();
  });
});
