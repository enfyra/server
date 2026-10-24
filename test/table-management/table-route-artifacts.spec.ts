import { describe, expect, it, vi } from 'vitest';
import { ObjectId } from 'mongodb';
import {
  ensureMongoTableRouteArtifacts,
  ensureSqlTableRouteArtifacts,
} from '../../src/modules/table-management/services/table-route-artifacts.service';

function sqlFixture(existing = false) {
  const rows = new Map<string, any[]>([
    [
      'enfyra_method',
      [
        { id: 3, name: 'GET' },
        { id: 4, name: 'POST' },
        { id: 9, name: 'CUSTOM' },
      ],
    ],
    ['enfyra_route', existing ? [{ id: 20, path: '/items' }] : []],
  ]);
  let nextId = 100;
  const trx = vi.fn((table: string) => {
    let filter: Record<string, unknown> = {};
    const query: any = {
      where: (value: Record<string, unknown>) => {
        filter = value;
        return query;
      },
      first: async () =>
        rows
          .get(table)
          ?.find((row) =>
            Object.entries(filter).every(([key, value]) => row[key] === value),
          ),
      select: async () => rows.get(table) ?? [],
      insert: async (input: any) => {
        const records = Array.isArray(input) ? input : [input];
        rows.set(table, [
          ...(rows.get(table) ?? []),
          ...records.map((row) => ({ id: nextId++, ...row })),
        ]);
      },
    };
    return query;
  });
  return {
    rows,
    trx,
    input: {
      trx: trx as any,
      tableName: 'items',
      tableId: 10,
      logger: { warn: vi.fn() },
      runtimeRegistryService: {
        getTableMetadata: () => ({ relations: [] }),
      } as any,
    },
  };
}

function mongoFixture(existing = false) {
  const methods = [
    { _id: new ObjectId(), name: 'GET' },
    { _id: new ObjectId(), name: 'POST' },
    { _id: new ObjectId(), name: 'CUSTOM' },
  ];
  const rows = new Map<string, any[]>([['enfyra_method', methods]]);
  const collection = vi.fn((table: string) => ({
    find: () => ({ toArray: async () => rows.get(table) ?? [] }),
    insertOne: async (input: any) => {
      const record = { _id: new ObjectId(), ...input };
      rows.set(table, [...(rows.get(table) ?? []), record]);
      return { insertedId: record._id };
    },
    insertMany: async (input: any[]) => {
      const records = input.map((row) => ({ _id: new ObjectId(), ...row }));
      rows.set(table, [...(rows.get(table) ?? []), ...records]);
      return {
        insertedIds: Object.fromEntries(
          records.map((row, index) => [index, row._id]),
        ),
      };
    },
  }));
  return {
    rows,
    collection,
    input: {
      tableName: 'items',
      tableId: new ObjectId(),
      mongoService: { getDb: () => ({ collection }) } as any,
      queryBuilderService: {
        findOne: async () => (existing ? { _id: new ObjectId() } : null),
      } as any,
    },
  };
}

describe('auto table route artifacts', () => {
  it('creates private SQL method cells for all verbs and binds only built-in CRUD handlers', async () => {
    const { input, rows } = sqlFixture();
    await ensureSqlTableRouteArtifacts(input);
    const route = rows.get('enfyra_route')![0];
    const configs = rows.get('enfyra_route_method_config');
    expect(configs).toHaveLength(3);
    for (const config of configs!)
      expect(config).toMatchObject({
        routeId: route.id,
        available: true,
        isPublic: false,
        skipRoleGuard: false,
        timeout: 30_000,
        requestBodyType: 'none',
        isSystem: false,
      });
    const handlers = rows.get('enfyra_route_handler')!;
    expect(handlers).toHaveLength(2);
    for (const handler of handlers)
      expect(handler.routeMethodConfigId).toBe(
        configs!.find((config) => config.methodId === handler.methodId)!.id,
      );
    expect(
      handlers.every((handler) => typeof handler.compiledCode === 'string'),
    ).toBe(true);
  });

  it('creates private Mongo method cells and binds handlers by inserted config identity', async () => {
    const { input, rows } = mongoFixture();
    await ensureMongoTableRouteArtifacts(input);
    const route = rows.get('enfyra_route')![0];
    const configs = rows.get('enfyra_route_method_config');
    expect(configs).toHaveLength(3);
    for (const config of configs!)
      expect(config).toMatchObject({
        route: route._id,
        available: true,
        isPublic: false,
        skipRoleGuard: false,
        timeout: 30_000,
        requestBodyType: 'none',
        isSystem: false,
      });
    const handlers = rows.get('enfyra_route_handler')!;
    expect(handlers).toHaveLength(2);
    for (const handler of handlers)
      expect(handler.routeMethodConfig).toEqual(
        configs!.find((config) => config.method.equals(handler.method))!._id,
      );
  });

  it('does not alter an existing SQL auto-route', async () => {
    const { input, rows, trx } = sqlFixture(true);
    await ensureSqlTableRouteArtifacts(input);
    expect(trx).toHaveBeenCalledOnce();
    expect(rows.has('enfyra_route_method_config')).toBe(false);
  });

  it('does not alter an existing Mongo auto-route', async () => {
    const { input, collection } = mongoFixture(true);
    await ensureMongoTableRouteArtifacts(input);
    expect(collection).not.toHaveBeenCalled();
  });
});
