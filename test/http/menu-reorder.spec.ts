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
        { id: 2, type: 'Dropdown Menu', path: '/settings', isSystem: false, parent: null },
        { id: 7, type: 'Menu', path: '/settings/profile', isSystem: false, parent: { id: 2 } },
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

describe('menu reorder endpoint', () => {
  it('preserves an existing parent when parent is omitted', async () => {
    const { handlers, queryBuilderService, eventEmitter } = createHarness();
    const response = { json: vi.fn() };

    await handlers.get('/admin/menu/reorder')?.(
      {
        body: { updates: [{ id: 7, order: 3 }] },
        scope: { cradle: {} },
      },
      response,
    );

    expect(queryBuilderService.update).toHaveBeenCalledWith(
      'enfyra_menu',
      7,
      { order: 3, parent: 2 },
    );
    expect(eventEmitter.emitAsync).toHaveBeenCalledWith(
      CACHE_EVENTS.INVALIDATE,
      expect.objectContaining({
        table: 'enfyra_menu',
        action: 'reload',
        scope: 'partial',
        ids: [7],
      }),
    );
    expect(response.json).toHaveBeenCalledWith({
      success: true,
      data: { updated: 1, ids: [7] },
    });
  });

  it('moves a menu to the root when parent is explicitly null', async () => {
    const { handlers, queryBuilderService } = createHarness();
    const response = { json: vi.fn() };

    await handlers.get('/admin/menu/reorder')?.(
      {
        body: { updates: [{ id: 7, order: 0, parent: null }] },
        scope: { cradle: {} },
      },
      response,
    );

    expect(queryBuilderService.update).toHaveBeenCalledWith(
      'enfyra_menu',
      7,
      { order: 0, parent: null },
    );
  });
});
