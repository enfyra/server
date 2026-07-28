import { RouteDefinitionProcessor } from '../../src/domain/bootstrap';
import { getSqlJunctionPhysicalNames } from '../../src/modules/table-management/utils/sql-junction-naming.util';
import { DatabaseConfigService } from '../../src/shared/services';
import { ObjectId } from 'mongodb';

function routeMethodJunction(propertyName: string) {
  return getSqlJunctionPhysicalNames({
    sourceTable: 'enfyra_route',
    propertyName,
    targetTable: 'enfyra_method',
  }).junctionTableName;
}

function makeKnex(methodRows: any[] = []) {
  const deletes: any[] = [];
  const inserts: any[] = [];
  const rawCalls: any[] = [];
  const knex = jest.fn((table: string) => {
    if (table === 'enfyra_method') {
      return {
        select: jest.fn().mockReturnThis(),
        whereIn: jest.fn((_field: string, values: string[]) =>
          Promise.resolve(
            methodRows.filter((method) => values.includes(method.name)),
          ),
        ),
      };
    }
    if (table === 'enfyra_relation as r') {
      let propertyName = 'availableMethods';
      const chain: any = {
        leftJoin: jest.fn(() => chain),
        select: jest.fn(() => chain),
        where: jest.fn((field: string, value: string) => {
          if (field === 'r.propertyName') propertyName = value;
          return chain;
        }),
        first: jest.fn(() =>
          Promise.resolve({
            junctionTableName: `j_${propertyName}`,
            junctionSourceColumn: 'sourceId',
            junctionTargetColumn: 'targetId',
          }),
        ),
      };
      return chain;
    }
    return {
      where: jest.fn((condition: any) => ({
        delete: jest.fn(async () => {
          deletes.push({ table, condition });
          return 1;
        }),
      })),
      insert: jest.fn(async (rows: any[]) => {
        inserts.push({ table, rows });
        return rows;
      }),
    };
  });
  (knex as any).raw = jest.fn(async (sql: string, bindings: any[]) => {
    rawCalls.push({ sql, bindings });
    return { rows: [] };
  });
  (knex as any).schema = {
    hasTable: jest.fn(async () => true),
    hasColumn: jest.fn(async () => true),
  };
  return { knex, deletes, inserts, rawCalls };
}

describe('RouteDefinitionProcessor SQL relation writes', () => {
  beforeEach(() => {
    DatabaseConfigService.overrideForTesting('postgres');
  });

  afterEach(() => {
    DatabaseConfigService.resetForTesting();
  });

  it('strips method relations from route row insert and syncs junction rows', async () => {
    const knex = makeKnex([
      { id: 1, name: 'GET' },
      { id: 2, name: 'POST' },
    ]);
    const queryBuilder = {
      find: jest.fn().mockResolvedValue({ data: [] }),
      findOne: jest.fn().mockResolvedValue(null),
      insert: jest.fn().mockResolvedValue({ id: 42 }),
      update: jest.fn().mockResolvedValue(undefined),
      getKnex: jest.fn(() => knex.knex),
    } as any;

    const processor = new RouteDefinitionProcessor({
      queryBuilderService: queryBuilder,
    });

    await processor.processWithQueryBuilder(
      [
        {
          path: '/custom',
          availableMethods: ['GET', 'POST'],
          skipRoleGuardMethods: ['GET'],
          isEnabled: true,
        },
      ],
      queryBuilder,
      'enfyra_route',
    );

    expect(queryBuilder.insert).toHaveBeenCalledWith(
      'enfyra_route',
      expect.not.objectContaining({
        availableMethods: expect.anything(),
        skipRoleGuardMethods: expect.anything(),
      }),
    );
    expect(knex.rawCalls).toContainEqual({
      sql: 'delete from ?? where ?? = ?',
      bindings: [routeMethodJunction('availableMethods'), 'sourceId', 42],
    });
    expect(knex.rawCalls).toContainEqual({
      sql: 'insert into ?? (??, ??) values (?, ?), (?, ?)',
      bindings: [
        routeMethodJunction('availableMethods'),
        'sourceId',
        'targetId',
        42,
        1,
        42,
        2,
      ],
    });
    expect(knex.rawCalls).toContainEqual({
      sql: 'insert into ?? (??, ??) values (?, ?)',
      bindings: [
        routeMethodJunction('skipRoleGuardMethods'),
        'sourceId',
        'targetId',
        42,
        1,
      ],
    });
  });

  it('uses primitive insert result as inserted id', async () => {
    const knex = makeKnex([{ id: 1, name: 'GET' }]);
    const queryBuilder = {
      find: jest.fn().mockResolvedValue({ data: [] }),
      findOne: jest.fn().mockResolvedValue(null),
      insert: jest.fn().mockResolvedValue(99),
      update: jest.fn().mockResolvedValue(undefined),
      getKnex: jest.fn(() => knex.knex),
    } as any;

    const processor = new RouteDefinitionProcessor({
      queryBuilderService: queryBuilder,
    });

    await processor.processWithQueryBuilder(
      [{ path: '/assets/:id', availableMethods: ['GET'] }],
      queryBuilder,
      'enfyra_route',
    );

    expect(knex.rawCalls).toContainEqual({
      sql: 'insert into ?? (??, ??) values (?, ?)',
      bindings: [
        routeMethodJunction('availableMethods'),
        'sourceId',
        'targetId',
        99,
        1,
      ],
    });
  });
});

describe('RouteDefinitionProcessor Mongo handler generation', () => {
  beforeEach(() => {
    DatabaseConfigService.overrideForTesting('mongodb');
  });

  afterEach(() => {
    DatabaseConfigService.resetForTesting();
  });

  it('accepts ObjectId arrays in availableMethods', async () => {
    const routeId = new ObjectId();
    const tableId = new ObjectId();
    const getId = new ObjectId();
    const postId = new ObjectId();
    const inserted: any[] = [];
    const methods = [
      { _id: getId, name: 'GET' },
      { _id: postId, name: 'POST' },
    ];
    const handlerCollection = {
      deleteMany: jest.fn(async () => ({ deletedCount: 0 })),
      findOne: jest.fn(async () => null),
      insertOne: jest.fn(async (data: any) => {
        inserted.push(data);
        return { insertedId: new ObjectId() };
      }),
    };
    const db = {
      collection: jest.fn((name: string) => {
        if (name === 'enfyra_route') {
          return {
            find: jest.fn(() => ({
              toArray: jest.fn(async () => [
                {
                  _id: routeId,
                  path: '/post',
                  mainTable: tableId,
                  isEnabled: true,
                  availableMethods: [getId, postId],
                },
              ]),
            })),
          };
        }
        if (name === 'enfyra_table') {
          return { findOne: jest.fn(async () => ({ _id: tableId, name: 'post' })) };
        }
        if (name === 'enfyra_method') {
          return {
            find: jest.fn((query: any) => {
              const requested = query?._id?.$in ?? [];
              const matching = methods.filter((method) =>
                requested.some(
                  (candidate: any) =>
                    candidate instanceof ObjectId &&
                    candidate.equals(method._id),
                ),
              );
              const cursor: any = {
                project: jest.fn(() => cursor),
                toArray: jest.fn(async () => matching),
              };
              return cursor;
            }),
            findOne: jest.fn(async ({ name: methodName }: any) =>
              methods.find((method) => method.name === methodName),
            ),
          };
        }
        if (name === 'enfyra_route_handler') return handlerCollection;
        throw new Error(`Unexpected collection ${name}`);
      }),
    };
    const processor = new RouteDefinitionProcessor({
      queryBuilderService: { getMongoDb: () => db } as any,
    });

    await processor.ensureMissingHandlers();

    expect(inserted).toHaveLength(2);
    expect(inserted.map((handler) => handler.method)).toEqual([getId, postId]);
    expect(inserted.every((handler) => handler.route.equals(routeId))).toBe(true);
  });
});
