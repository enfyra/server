import { describe, expect, it, vi } from 'vitest';
import { registerAdminRoutes } from '../../src/http/routes/admin.routes';
import { CACHE_EVENTS } from '../../src/shared/utils/cache-events.constants';

function createHarness() {
  const handlers = new Map<string, any>();
  const register = (path: string, handler: any) => handlers.set(path, handler);
  const queryBuilderService = {
    getPkField: vi.fn(() => 'id'),
    find: vi.fn().mockResolvedValue({
      data: [
        { id: 10, headerKey: 'x-enfyra-pat', scheme: 'raw', isSystem: true },
        { id: 11, headerKey: 'authorization', scheme: 'bearer', isSystem: true },
        { id: 12, headerKey: 'x-api-key', scheme: 'raw', isSystem: false },
      ],
    }),
    update: vi.fn().mockResolvedValue(undefined),
  };
  const eventEmitter = {
    emitAsync: vi.fn().mockResolvedValue(undefined),
  };
  const app = {
    get: register,
    post: register,
    patch: register,
    delete: register,
  };

  registerAdminRoutes(app as any, {
    cradle: { queryBuilderService, eventEmitter },
  } as any);

  return { handlers, queryBuilderService, eventEmitter };
}

describe('authentication header reorder endpoint', () => {
  it('persists the drag order and emits an auth-header cache invalidation', async () => {
    const { handlers, queryBuilderService, eventEmitter } = createHarness();
    const response = { json: vi.fn() };

    await handlers.get('/admin/auth-header/reorder')?.(
      {
        body: {
          updates: [
            { id: 12, priority: 0 },
            { id: 10, priority: 1 },
            { id: 11, priority: 2 },
          ],
        },
        scope: { cradle: {} },
      },
      response,
    );

    expect(queryBuilderService.update).toHaveBeenNthCalledWith(
      1,
      'enfyra_auth_header',
      12,
      { priority: 0 },
    );
    expect(queryBuilderService.update).toHaveBeenNthCalledWith(
      2,
      'enfyra_auth_header',
      10,
      { priority: 1 },
    );
    expect(queryBuilderService.update).toHaveBeenNthCalledWith(
      3,
      'enfyra_auth_header',
      11,
      { priority: 2 },
    );
    expect(eventEmitter.emitAsync).toHaveBeenCalledWith(
      CACHE_EVENTS.INVALIDATE,
      expect.objectContaining({
        table: 'enfyra_auth_header',
        action: 'reload',
        scope: 'partial',
        ids: [12, 10, 11],
      }),
    );
    expect(response.json).toHaveBeenCalledWith({
      success: true,
      data: { updated: 3, ids: [12, 10, 11] },
    });
  });

  it('rejects unknown or duplicate mapping ids', async () => {
    const { handlers } = createHarness();
    const response = { json: vi.fn() };
    const handler = handlers.get('/admin/auth-header/reorder');

    await expect(
      handler?.(
        {
          body: { updates: [{ id: 99, priority: 0 }] },
          scope: { cradle: {} },
        },
        response,
      ),
    ).rejects.toThrow('Authentication header not found: 99');

    await expect(
      handler?.(
        {
          body: {
            updates: [
              { id: 10, priority: 0 },
              { id: 10, priority: 1 },
            ],
          },
          scope: { cradle: {} },
        },
        response,
      ),
    ).rejects.toThrow('Duplicate auth header id in reorder payload: 10');
  });
});
