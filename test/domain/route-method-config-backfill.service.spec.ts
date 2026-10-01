import { ObjectId } from 'mongodb';
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

  it('attaches newly seeded handlers on a later upgrade without changing existing bindings', async () => {
    const configs = [
      { id: 3, route: { id: 1 }, method: { id: 2 }, timeout: 45000 },
      { id: 6, route: { id: 5 }, method: { id: 2 }, timeout: 30000 },
    ];
    const instance = service(configs);
    vi.spyOn(instance as any, 'loadState').mockResolvedValue({
      ...state(configs),
      routes: [{ id: 1 }, { id: 5, isSystem: true }],
      handlers: [
        { id: 10, route: { id: 1 }, method: { id: 2 }, routeMethodConfig: { id: 3 } },
        { id: 11, route: { id: 5 }, method: { id: 2 }, routeMethodConfig: null },
      ],
    });
    vi.spyOn(instance as any, 'upsertConfig').mockImplementation(async (_draft: any, existing: any) => existing);
    const attachHandler = vi.spyOn(instance as any, 'attachHandler').mockResolvedValue(undefined);
    const attachBindings = vi.spyOn(instance as any, 'attachRouteScopedBindings');

    await instance.run();

    expect(attachHandler).toHaveBeenCalledExactlyOnceWith(11, configs[1]);
    expect(attachBindings).not.toHaveBeenCalled();
    expect(configs[0].timeout).toBe(45000);
  });

  it('attaches unbound handlers with Mongo identities without rewriting existing cells', async () => {
    const routeId = new ObjectId();
    const methodId = new ObjectId();
    const configId = new ObjectId();
    const handlerId = new ObjectId();
    const config = { _id: configId, route: routeId, method: methodId, timeout: 45000 };
    const instance = service([config]);
    vi.spyOn(instance as any, 'loadState').mockResolvedValue({
      ...state([config]),
      routes: [{ _id: routeId }],
      methods: [{ _id: methodId }],
      handlers: [{ _id: handlerId, route: routeId, method: methodId, routeMethodConfig: null }],
    });
    vi.spyOn(instance as any, 'upsertConfig').mockImplementation(async (_draft: any, existing: any) => existing);
    const attachHandler = vi.spyOn(instance as any, 'attachHandler').mockResolvedValue(undefined);

    await instance.run();

    expect(attachHandler).toHaveBeenCalledExactlyOnceWith(handlerId, config);
    expect(config.timeout).toBe(45000);
  });

  it('does not overwrite an existing operator handler binding', async () => {
    const config = { id: 3, route: { id: 1 }, method: { id: 2 }, timeout: 45000 };
    const instance = service([config]);
    vi.spyOn(instance as any, 'loadState').mockResolvedValue({
      ...state([config]),
      handlers: [{ id: 10, route: { id: 1 }, method: { id: 2 }, routeMethodConfig: { id: 99 } }],
    });
    vi.spyOn(instance as any, 'upsertConfig').mockImplementation(async (_draft: any, existing: any) => existing);
    const attachHandler = vi.spyOn(instance as any, 'attachHandler').mockResolvedValue(undefined);

    await instance.run();

    expect(attachHandler).not.toHaveBeenCalled();
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
