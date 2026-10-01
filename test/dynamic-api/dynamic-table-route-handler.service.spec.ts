import { describe, expect, it, vi } from 'vitest';
import { DynamicTableRouteHandlerService } from '../../src/modules/dynamic-api/services/dynamic-table-route-handler.service';

function createService(queryBuilderOverrides: Record<string, unknown> = {}) {
  return new DynamicTableRouteHandlerService({
    bcryptService: { hash: vi.fn() } as any,
    flowQueueMaintenanceService: { removeFlowJobs: vi.fn() } as any,
    guardValidationService: {
      assertGuardCreate: vi.fn(),
      assertGuardUpdate: vi.fn(),
      assertGuardRuleBody: vi.fn(),
      assertGuardRuleUpdate: vi.fn(),
    } as any,
    queryBuilderService: {
      getPkField: vi.fn(() => 'id'),
      find: vi.fn(),
      update: vi.fn(),
      delete: vi.fn(),
      insertWithOptions: vi.fn(),
      ...queryBuilderOverrides,
    } as any,
    runtimeMetadataSchemaRouterService: { handles: vi.fn(() => false) } as any,
    userRevocationService: { publish: vi.fn() } as any,
  });
}

describe('DynamicTableRouteHandlerService', () => {
  it('keeps event and webhook trigger validation', () => {
    const service = createService();

    expect(() => service.assertFlowTriggerBody({ type: 'event' })).toThrow(
      'Event trigger requires table reference',
    );
    expect(() =>
      service.assertFlowTriggerBody({ type: 'event', table: 'articles' }),
    ).toThrow('Event trigger requires tableEvent (create|update|delete)');
    expect(() => service.assertFlowTriggerBody({ type: 'webhook' })).toThrow(
      'Webhook trigger requires route reference',
    );
  });

  it('creates a disabled method config for every method when a route is created', async () => {
    const insertWithOptions = vi.fn(async () => ({}));
    const service = createService({
      find: vi
        .fn()
        .mockResolvedValueOnce({ data: [{ id: 10 }, { id: 11 }] })
        .mockResolvedValueOnce({ data: [] })
        .mockResolvedValueOnce({ data: [] }),
      insertWithOptions,
    });

    await service.createRouteMethodConfigsForRoute(1, true);

    expect(insertWithOptions).toHaveBeenCalledTimes(2);
    expect(insertWithOptions).toHaveBeenNthCalledWith(1, {
      table: 'enfyra_route_method_config',
      data: {
        route: { id: 1 },
        method: { id: 10 },
        available: false,
        isPublic: false,
        skipRoleGuard: false,
        timeout: 30_000,
        requestBodyType: 'none',
        isSystem: true,
      },
    });
  });

  it('synchronizes legacy route method flags into canonical configs', async () => {
    const update = vi.fn(async () => ({}));
    const service = createService({
      find: vi
        .fn()
        .mockResolvedValueOnce({
          data: [
            {
              id: 1,
              availableMethods: [{ id: 10 }],
              publicMethods: [{ id: 11 }],
              skipRoleGuardMethods: [{ id: 12 }],
            },
          ],
        })
        .mockResolvedValueOnce({
          data: [
            { id: 100, method: { id: 10 } },
            { id: 101, method: { id: 11 } },
            { id: 102, method: { id: 12 } },
          ],
        }),
      update,
    });

    await service.syncRouteMethodConfigFlags(1, {});

    expect(update.mock.calls.map(([, , data]) => data)).toEqual([
      { available: true, isPublic: false, skipRoleGuard: false },
      { available: false, isPublic: true, skipRoleGuard: false },
      { available: false, isPublic: false, skipRoleGuard: true },
    ]);
  });

  it('creates a disabled method config for every route when a method is created', async () => {
    const insertWithOptions = vi.fn(async () => ({}));
    const service = createService({
      find: vi.fn(async () => ({
        data: [
          { id: 1, isSystem: true },
          { id: 2, isSystem: false },
        ],
      })),
      insertWithOptions,
    });

    await service.createRouteMethodConfigsForMethod(10);

    expect(insertWithOptions).toHaveBeenCalledTimes(2);
    expect(insertWithOptions.mock.calls.map(([options]) => options.data)).toEqual([
      expect.objectContaining({
        route: { id: 1 },
        method: { id: 10 },
        isSystem: true,
      }),
      expect.objectContaining({
        route: { id: 2 },
        method: { id: 10 },
        isSystem: false,
      }),
    ]);
  });

  it('treats only duplicate-key races as an existing config', async () => {
    const duplicate = Object.assign(new Error('duplicate'), { code: '23505' });
    const insertWithOptions = vi
      .fn()
      .mockRejectedValueOnce(duplicate)
      .mockRejectedValueOnce(new Error('storage failed'));
    const service = createService({
      find: vi
        .fn()
        .mockResolvedValueOnce({ data: [{ id: 10 }] })
        .mockResolvedValueOnce({ data: [] })
        .mockResolvedValueOnce({ data: [] })
        .mockResolvedValueOnce({ data: [{ id: 10 }] })
        .mockResolvedValueOnce({ data: [] })
        .mockResolvedValueOnce({ data: [] }),
      insertWithOptions,
    });

    await expect(
      service.createRouteMethodConfigsForRoute(1, false),
    ).resolves.toBeUndefined();
    await expect(
      service.createRouteMethodConfigsForRoute(1, false),
    ).rejects.toThrow('storage failed');
  });

  it('links handlers to the canonical config and migrates legacy timeout writes', async () => {
    const update = vi.fn(async () => ({}));
    const service = createService({
      find: vi.fn(async () => ({ data: [{ id: 100 }] })),
      update,
    });
    const body = {
      route: { id: 1 },
      method: { id: 10 },
      timeout: 0,
    };

    await service.normalizeRouteHandlerConfig(body);
    await service.syncRouteHandlerTimeout(body);

    expect(body.routeMethodConfig).toEqual({ id: 100 });
    expect(update).toHaveBeenCalledWith('enfyra_route_method_config', 100, {
      timeout: 60_000,
    });
  });

  it('protects permanent matrix cell identity', () => {
    const service = createService();

    expect(() => service.assertRouteMethodConfigCreateAllowed()).toThrow(
      /created automatically/,
    );
    expect(() => service.assertRouteMethodConfigUpdate({ route: { id: 2 } })).toThrow(
      /Route and method cannot be changed/,
    );
    expect(() => service.assertRouteMethodConfigUpdate({ timeout: 45_000 })).not.toThrow();
    expect(() => service.assertRouteMethodConfigDeleteAllowed()).toThrow(
      /permanent matrix cells/,
    );
  });

  it('keeps only available route methods across Mongo-like and patch inputs', () => {
    const service = createService();
    const getId = '507f1f77bcf86cd799439011';
    const postId = '507f1f77bcf86cd799439012';
    const deleteId = '507f1f77bcf86cd799439013';
    const body = {
      availableMethods: [{ _id: getId }, { _id: postId }],
      publicMethods: [{ _id: getId }, postId, deleteId],
    };

    service.normalizeRouteMethods(body, null, 'publicMethods');

    expect(body.publicMethods).toEqual([{ _id: getId }, postId]);

    const patchId = '507f1f77bcf86cd799439014';
    const patch = { publicMethods: [{ id: patchId }, { id: deleteId }] };
    service.normalizeRouteMethods(
      patch,
      { availableMethods: [{ id: patchId }] },
      'publicMethods',
    );

    expect(patch.publicMethods).toEqual([{ id: patchId }]);
  });
});
