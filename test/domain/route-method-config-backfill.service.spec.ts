import { describe, expect, it, vi } from 'vitest';
import { RouteMethodConfigBackfillService } from '../../src/engines/bootstrap/services/route-method-config-backfill.service';

function state(configs: any[]) {
  return {
    routes: [{ id: 1, isSystem: false, availableMethods: [] }],
    methods: [{ id: 2 }],
    configs,
    handlers: [],
    permissions: [],
    preHooks: [],
    postHooks: [],
    guards: [],
  };
}

function service(configs: any[]) {
  const instance = new RouteMethodConfigBackfillService({
    queryBuilderService: { isMongoDb: () => false, getPkField: () => 'id' } as any,
    metadataCacheService: { getMetadata: async () => ({ tables: new Map() }) } as any,
  });
  vi.spyOn(instance as any, 'loadState').mockResolvedValue(state(configs));
  return instance;
}

describe('RouteMethodConfigBackfillService upgrade safety', () => {
  it('preserves existing operator bindings on a later version upgrade', async () => {
    const instance = service([{ id: 3, route: { id: 1 }, method: { id: 2 }, timeout: 45000 }]);
    const attachHandler = vi.spyOn(instance as any, 'attachHandler');
    const attachBindings = vi.spyOn(instance as any, 'attachRouteScopedBindings');
    const upsert = vi.spyOn(instance as any, 'upsertConfig').mockImplementation(async (_draft: any, existing: any) => existing);

    await instance.run();

    expect(upsert).toHaveBeenCalledOnce();
    expect(attachHandler).not.toHaveBeenCalled();
    expect(attachBindings).not.toHaveBeenCalled();
  });

  it('fills only new method cells without replacing existing operator values', async () => {
    const instance = service([{ id: 3, route: { id: 1 }, method: { id: 2 }, timeout: 45000 }]);
    vi.spyOn(instance as any, 'loadState').mockResolvedValue({
      ...state([{ id: 3, route: { id: 1 }, method: { id: 2 }, timeout: 45000 }]),
      routes: [{ id: 1, isSystem: false, availableMethods: [{ id: 5 }] }],
      methods: [{ id: 2 }, { id: 5 }],
    });
    const upsert = vi.spyOn(instance as any, 'upsertConfig').mockImplementation(async (draft: any, existing: any) =>
      existing ?? { id: 4, route: { id: draft.routeId }, method: { id: draft.methodId } },
    );
    const attachBindings = vi.spyOn(instance as any, 'attachRouteScopedBindings');

    await instance.run();

    expect(upsert).toHaveBeenCalledTimes(2);
    expect(upsert.mock.calls[0][1]).toEqual(expect.objectContaining({ timeout: 45000 }));
    expect(upsert.mock.calls[1][1]).toBeUndefined();
    expect(upsert.mock.calls[1][0]).toEqual(expect.objectContaining({
      available: false,
      isPublic: false,
      skipRoleGuard: false,
      timeout: 30_000,
    }));
    expect(attachBindings).not.toHaveBeenCalled();
  });

  it('refuses duplicate or orphan cells before writing anything', async () => {
    const instance = service([
      { id: 3, route: { id: 1 }, method: { id: 2 } },
      { id: 4, route: { id: 1 }, method: { id: 2 } },
    ]);
    const upsert = vi.spyOn(instance as any, 'upsertConfig');

    await expect(instance.run()).rejects.toThrow('duplicate or orphan cells');
    expect(upsert).not.toHaveBeenCalled();
  });
});
