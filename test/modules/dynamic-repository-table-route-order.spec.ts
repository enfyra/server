import { describe, expect, it, vi } from 'vitest';
import { DynamicRepository } from '../../src/modules/dynamic-api';
import { GuardValidationService } from '../../src/modules/dynamic-api/services/guard-validation.service';
import { CascadeHandler } from '../../src/engines/knex/utils/cascade-handler';

/**
 * Regression tests for table-route side-effect ordering.
 *
 * Table-specific writes (storage-default cleanup, flow jobs cleanup, user
 * revocation) must keep their pre-refactor lifecycle position:
 * - storage-default cleanup and flow jobs cleanup run right after the DB
 *   write, BEFORE the result load + cache reload that snapshots state;
 * - user revocation runs after the cache reload (unchanged).
 *
 * These tests pin the order so a future pipeline refactor cannot silently
 * move a side effect across the reload boundary again.
 */
function makeRepo(
  tableName: string,
  overrides: Partial<ConstructorParameters<typeof DynamicRepository>[0]> = {},
) {
  const queryBuilderService = {
    getPkField: vi.fn(() => 'id'),
    getDatabaseType: vi.fn(() => 'postgres'),
    isSql: vi.fn(() => true),
    transaction: vi.fn(async (fn: () => Promise<unknown>) => fn()),
    find: vi.fn(async (query: any) => {
      const ids = query?.filter?.id?._in;
      if (Array.isArray(ids)) {
        return {
          data: ids.map((id) => ({ id, name: 'My Flow' })),
          count: ids.length,
        };
      }
      return { data: [], count: 0 };
    }),
    findOne: vi.fn().mockResolvedValue({ id: 42, name: 'x' }),
    insert: vi.fn().mockResolvedValue({ id: 42 }),
    update: vi.fn().mockResolvedValue(undefined),
    delete: vi.fn().mockResolvedValue(undefined),
    runWithPolicy: vi.fn(async (_check: any, fn: any) => fn()),
    runWithFieldPermissionCheck: vi.fn(async (_check: unknown, fn: () => Promise<unknown>) => fn()),
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
  const flowQueueMaintenanceService = {
    removeFlowJobs: vi.fn().mockResolvedValue(undefined),
  };
  const repo = new DynamicRepository({
    context: { $query: {} } as any,
    tableName,
    queryBuilderService: queryBuilderService as any,
    tableHandlerService: {} as any,
    runtimeMetadataSchemaRouterService: {
      handles: vi.fn(() => false),
    } as any,
    policyService: {
      checkMutationSafety: vi.fn().mockResolvedValue({ allowed: true }),
    } as any,
    guardValidationService: {} as any,
    tableValidationService: {
      assertTableValid: vi.fn().mockResolvedValue(undefined),
    } as any,
    runtimeRegistryService: runtimeRegistryService as any,
    eventEmitter: { emit: vi.fn() } as any,
    flowQueueMaintenanceService: flowQueueMaintenanceService as any,
    bcryptService: {
      hash: vi.fn(async (value: string) => `hashed:${value}`),
    } as any,
    ...overrides,
  });
  vi.spyOn(repo as any, 'ensureInit').mockResolvedValue(undefined);
  vi.spyOn(repo as any, 'find').mockResolvedValue({
    data: [{ id: 42, name: 'My Flow' }],
    count: 1,
  });
  (repo as any).tableMetadata = { columns: [] };
  return { repo, queryBuilderService, flowQueueMaintenanceService };
}

describe('DynamicRepository table-route side-effect ordering', () => {
  it.each([
    { publicMethods: [{ id: 4 }], isPublic: true },
    { publicMethods: [], isPublic: false },
  ])(
    'syncs publicMethods=$publicMethods before reload after SQL consumes relations',
    async ({ publicMethods, isPublic }) => {
      const { repo, queryBuilderService } = makeRepo('enfyra_route');
      const legacyRoute = { id: 42, availableMethods: [], publicMethods: [{ id: 4 }] };
      const configs = [
        { id: 1037, method: { id: 4 }, available: true, isPublic: !isPublic, skipRoleGuard: false },
        { id: 1038, method: { id: 5 }, available: true, isPublic: false, skipRoleGuard: true },
      ];
      const cascade = new CascadeHandler(
        {} as any,
        {
          getMetadata: () => ({
            tables: new Map([['enfyra_route', {
              relations: [{ propertyName: 'publicMethods', type: 'many-to-many' }],
            }]]),
          }),
        } as any,
        {} as any,
      );
      queryBuilderService.find.mockImplementation(async ({ table }) => ({
        data: table === 'enfyra_route_method_config' ? configs : [legacyRoute],
        count: table === 'enfyra_route_method_config' ? configs.length : 1,
      }));
      queryBuilderService.update.mockImplementation(async (table, id, data) => {
        if (table === 'enfyra_route') {
          legacyRoute.publicMethods = data.publicMethods;
          await cascade.syncManyToManyRelations(table, data);
          return;
        }
        Object.assign(configs.find((config) => config.id === id)!, data);
      });
      let state = 'outside';
      queryBuilderService.transaction.mockImplementation(async (write) => {
        state = 'writing';
        const result = await write();
        expect(configs[0].isPublic).toBe(isPublic);
        state = 'committed';
        return result;
      });
      const reload = vi.spyOn(repo as any, 'reload').mockImplementation(async () => {
        expect(state).toBe('committed');
        expect(configs[0]).toMatchObject({ available: true, isPublic, skipRoleGuard: false });
        expect(configs[1]).toMatchObject({ available: true, isPublic: false, skipRoleGuard: true });
      });

      await repo.update({ id: 42, data: { publicMethods } });

      expect(legacyRoute.publicMethods).toEqual(publicMethods);
      expect(reload).toHaveBeenCalledOnce();
    },
  );

  it.each(['enfyra_route', 'enfyra_route_method_config'])(
    'does not enter the compatibility transaction for ordinary updates to %s',
    async (tableName) => {
      const { repo, queryBuilderService } = makeRepo(tableName);
      const patch = tableName === 'enfyra_route'
        ? { description: 'Updated' }
        : { isPublic: false };
      vi.spyOn(repo as any, 'reload').mockResolvedValue(undefined);

      await repo.update({ id: 42, data: patch });

      expect(queryBuilderService.transaction).not.toHaveBeenCalled();
      expect(queryBuilderService.update.mock.calls).toEqual([[tableName, 42, patch]]);
    },
  );

  it('does not synchronize a legacy access field removed by caller field permissions', async () => {
    const { repo, queryBuilderService } = makeRepo('enfyra_route', {
      enforceFieldPermission: true,
      runtimeRegistryService: {
        lookupTableByName: vi.fn().mockResolvedValue({
          relations: [{ propertyName: 'publicMethods', isPublished: false }],
        }),
        getFieldPermissionPoliciesFor: vi.fn().mockReturnValue([]),
      } as any,
    });
    queryBuilderService.runWithFieldPermissionCheck.mockImplementation(async (_check, fn) => fn());
    const legacyRoute = { id: 42, publicMethods: [{ id: 4 }] };
    queryBuilderService.find.mockResolvedValue({ data: [legacyRoute], count: 1 });
    vi.spyOn(repo as any, 'reload').mockResolvedValue(undefined);

    await repo.update({ id: 42, data: { publicMethods: [{ id: 4 }] } });

    expect(queryBuilderService.update.mock.calls).toEqual([['enfyra_route', 42, {}]]);
    expect(queryBuilderService.find).toHaveBeenCalledTimes(1);
  });

  it('does not publish a SQL route patch if updating the canonical config fails', async () => {
    const { repo, queryBuilderService } = makeRepo('enfyra_route');
    const route = { id: 42, publicMethods: [{ id: 4 }] };
    const config = { id: 100, method: { id: 4 }, available: true, isPublic: true };
    queryBuilderService.find.mockImplementation(async ({ table }) => ({
      data: table === 'enfyra_route_method_config' ? [config] : [route],
      count: 1,
    }));
    queryBuilderService.transaction.mockImplementation(async (write) => {
      const original = route.publicMethods;
      try {
        return await write();
      } catch (error) {
        route.publicMethods = original;
        throw error;
      }
    });
    queryBuilderService.update.mockImplementation(async (table, _id, data) => {
      if (table !== 'enfyra_route') throw new Error('method config write failed');
      route.publicMethods = data.publicMethods;
    });
    const reload = vi.spyOn(repo as any, 'reload').mockResolvedValue(undefined);

    await expect(repo.update({ id: 42, data: { publicMethods: [] } })).rejects.toThrow();

    expect(queryBuilderService.update).toHaveBeenCalledWith(
      'enfyra_route_method_config', 100, { isPublic: false },
    );
    expect(route.publicMethods).toEqual([{ id: 4 }]);
    expect(reload).not.toHaveBeenCalled();
  });

  it('rejects nested guard rules before the cascade insert boundary', async () => {
    const guardValidationService = new GuardValidationService({
      queryBuilderService: {} as any,
    });
    const { repo, queryBuilderService } = makeRepo('enfyra_guard', {
      guardValidationService,
    });

    await expect(
      repo.create({
        data: {
          name: 'unsafe nested guard',
          isGlobal: true,
          rules: [{ type: 'rate_limit_by_route', config: {} }],
        },
      }),
    ).rejects.toThrow(/Nested guard writes/);
    expect(queryBuilderService.insert).not.toHaveBeenCalled();
  });

  it('clears other default storage configs BEFORE the cache reload snapshots them', async () => {
    const { repo, queryBuilderService } = makeRepo('enfyra_storage_config');
    const callLog: string[] = [];

    queryBuilderService.update.mockImplementation(async () => {
      callLog.push('update');
      return undefined;
    });
    queryBuilderService.find.mockImplementation(async (opts: any) => {
      if (opts?.filter?.isDefault) {
        callLog.push('clear-find');
        return { data: [{ id: 7 }], count: 1 };
      }
      callLog.push('find');
      return { data: [], count: 0 };
    });
    vi.spyOn(repo as any, 'reload').mockImplementation(async () => {
      callLog.push('reload');
    });

    await repo.update({ id: 42, data: { isDefault: true } });

    // cleanup ran at all
    expect(callLog).toContain('clear-find');
    // the cleanup write (last update) must land before the reload that
    // snapshots storage state, otherwise the cache keeps multiple defaults
    expect(callLog.indexOf('reload')).toBeGreaterThan(
      callLog.lastIndexOf('update'),
    );
  });

  it('removes flow jobs even when the cache reload fails', async () => {
    const { repo, queryBuilderService, flowQueueMaintenanceService } =
      makeRepo('enfyra_flow');
    vi.spyOn(repo as any, 'reload').mockRejectedValue(new Error('reload boom'));

    await expect(repo.delete({ id: 42 })).rejects.toThrow('reload boom');

    expect(queryBuilderService.delete).toHaveBeenCalledWith('enfyra_flow', 42);
    // cleanup must have run before reload; a reload failure must not leak
    // orphan BullMQ jobs for an already-deleted flow
    expect(flowQueueMaintenanceService.removeFlowJobs).toHaveBeenCalledWith({
      id: 42,
      name: 'My Flow',
    });
  });

  it('does not remove queued flow executions when a flow is updated', async () => {
    const { repo, flowQueueMaintenanceService } = makeRepo('enfyra_flow');

    await repo.update({ id: 42, data: { description: 'Updated' } });

    expect(flowQueueMaintenanceService.removeFlowJobs).not.toHaveBeenCalled();
  });

  it('handles user passwords and folder slugs without system hooks', async () => {
    const { repo: userRepo, queryBuilderService: userQuery } = makeRepo('enfyra_user');
    await userRepo.create({ data: { password: 'plain-password' } });
    expect(userQuery.insert).toHaveBeenCalledWith('enfyra_user', {
      password: 'hashed:plain-password',
    });

    const { repo: folderRepo, queryBuilderService: folderQuery } = makeRepo('enfyra_folder');
    await folderRepo.create({ data: { name: 'Project Files' } });
    expect(folderQuery.insert).toHaveBeenCalledWith('enfyra_folder', {
      name: 'Project Files',
      slug: 'project-files',
    });
  });
});
