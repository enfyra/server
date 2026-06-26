import { DataMigrationService } from '../../src/engines/bootstrap';
import { DatabaseConfigService } from '../../src/shared/services';
import { getSqlJunctionPhysicalNames } from '../../src/modules/table-management/utils/sql-junction-naming.util';

function routeMethodJunction(propertyName: string) {
  return getSqlJunctionPhysicalNames({
    sourceTable: 'enfyra_route',
    propertyName,
    targetTable: 'enfyra_method',
  }).junctionTableName;
}

function makeQueryBuilder(
  overrides: Partial<{
    find: jest.Mock;
    update: jest.Mock;
    delete: jest.Mock;
    isMongoDb: jest.Mock;
    getKnex: jest.Mock;
    getMongoDb: jest.Mock;
  }> = {},
) {
  const knexMock = makeKnex();
  return {
    find: jest.fn().mockResolvedValue({ data: [] }),
    update: jest.fn().mockResolvedValue(undefined),
    delete: jest.fn().mockResolvedValue(undefined),
    isMongoDb: jest.fn().mockReturnValue(false),
    getKnex: jest.fn(() => knexMock.knex),
    getMongoDb: jest.fn(),
    __knexMock: knexMock,
    ...overrides,
  } as any;
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
      let propertyName = 'publicMethods';
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

function makeMongoDb() {
  const deletes: any[] = [];
  const inserts: any[] = [];
  const routeTableId = '651111111111111111111111';
  const methodTableId = '652222222222222222222222';
  return {
    deletes,
    inserts,
    collection: jest.fn((name: string) => {
      if (name === 'enfyra_table') {
        return {
          findOne: jest.fn(async (filter: any) => {
            if (filter.name === 'enfyra_route') {
              return { _id: routeTableId, name: 'enfyra_route' };
            }
            if (filter.name === 'enfyra_method') {
              return { _id: methodTableId, name: 'enfyra_method' };
            }
            return null;
          }),
        };
      }
      if (name === 'enfyra_relation') {
        return {
          findOne: jest.fn(async (filter: any) =>
            filter.propertyName
              ? {
                  junctionTableName: `j_${filter.propertyName}`,
                  junctionSourceColumn: 'sourceId',
                  junctionTargetColumn: 'targetId',
                }
              : null,
          ),
        };
      }
      return {
        deleteMany: jest.fn(async (condition: any) => {
          deletes.push({ collection: name, condition });
          return { deletedCount: 1 };
        }),
        insertMany: jest.fn(async (rows: any[]) => {
          inserts.push({ collection: name, rows });
          return { insertedCount: rows.length };
        }),
      };
    }),
  };
}

function makeService(qb: any): DataMigrationService {
  const svc = new DataMigrationService({ queryBuilderService: qb });
  (svc as any).initOld = null;
  return svc;
}

describe('DataMigrationService.transformRecord', () => {
  it('captures non-empty publicMethods as relation update', () => {
    const svc = makeService(makeQueryBuilder());
    const { newRecord, relationUpdates } = (svc as any).transformRecord(
      'enfyra_route',
      {
        _unique: { path: { _eq: '/me' } },
        publicMethods: ['GET', 'POST'],
        isEnabled: true,
      },
    );
    expect(relationUpdates.publicMethods).toEqual(['GET', 'POST']);
    expect(newRecord.publicMethods).toBeUndefined();
    expect(newRecord.isEnabled).toBe(true);
  });

  it('captures empty array publicMethods as relation update (the bug fix)', () => {
    const svc = makeService(makeQueryBuilder());
    const { newRecord, relationUpdates } = (svc as any).transformRecord(
      'enfyra_route',
      { _unique: { path: { _eq: '/metadata' } }, publicMethods: [] },
    );
    expect(relationUpdates.publicMethods).toEqual([]);
    expect(newRecord.publicMethods).toBeUndefined();
  });

  it('captures empty array availableMethods as relation update', () => {
    const svc = makeService(makeQueryBuilder());
    const { newRecord, relationUpdates } = (svc as any).transformRecord(
      'enfyra_route',
      { _unique: { path: { _eq: '/test' } }, availableMethods: [] },
    );
    expect(relationUpdates.availableMethods).toEqual([]);
    expect(newRecord.availableMethods).toBeUndefined();
  });

  it('does not capture undefined relation field', () => {
    const svc = makeService(makeQueryBuilder());
    const { relationUpdates } = (svc as any).transformRecord('enfyra_route', {
      _unique: { path: { _eq: '/test' } },
      name: 'hello',
    });
    expect(relationUpdates.publicMethods).toBeUndefined();
    expect(relationUpdates.availableMethods).toBeUndefined();
  });

  it('does not capture null relation field', () => {
    const svc = makeService(makeQueryBuilder());
    const { relationUpdates } = (svc as any).transformRecord('enfyra_route', {
      _unique: { path: { _eq: '/test' } },
      publicMethods: null,
    });
    expect(relationUpdates.publicMethods).toBeUndefined();
  });
});

describe('DataMigrationService.updateRelations', () => {
  beforeEach(() => {
    DatabaseConfigService.overrideForTesting('mysql');
  });

  afterEach(() => {
    DatabaseConfigService.resetForTesting();
  });

  it('calls update with empty array to clear publicMethods (the bug fix)', async () => {
    const qb = makeQueryBuilder({
      find: jest.fn().mockResolvedValue({ data: [] }),
    });
    const svc = makeService(qb);

    await (svc as any).updateRelations('enfyra_route', 99, {
      publicMethods: [],
    });

    expect(qb.__knexMock.rawCalls).toContainEqual({
      sql: 'delete from ?? where ?? = ?',
      bindings: [routeMethodJunction('publicMethods'), 'sourceId', 99],
    });
  });

  it('calls update with method IDs when methods are provided', async () => {
    const knexMock = makeKnex([
      { id: 1, name: 'GET' },
      { id: 2, name: 'POST' },
    ]);
    const qb = makeQueryBuilder({
      getKnex: jest.fn(() => knexMock.knex),
    });
    const svc = makeService(qb);

    await (svc as any).updateRelations('enfyra_route', 10, {
      publicMethods: ['GET', 'POST'],
    });

    expect(knexMock.rawCalls).toContainEqual({
      sql: 'insert into ?? (??, ??) values (?, ?), (?, ?)',
      bindings: [
        routeMethodJunction('publicMethods'),
        'sourceId',
        'targetId',
        10,
        1,
        10,
        2,
      ],
    });
  });

  it('clears availableMethods when provided empty array', async () => {
    const qb = makeQueryBuilder({
      find: jest.fn().mockResolvedValue({ data: [] }),
    });
    const svc = makeService(qb);

    await (svc as any).updateRelations('enfyra_route', 5, {
      availableMethods: [],
    });

    expect(qb.__knexMock.rawCalls).toContainEqual({
      sql: 'delete from ?? where ?? = ?',
      bindings: [routeMethodJunction('availableMethods'), 'sourceId', 5],
    });
  });

  it('does nothing for non-enfyra_route tables', async () => {
    const qb = makeQueryBuilder();
    const svc = makeService(qb);

    await (svc as any).updateRelations('enfyra_user', 1, {
      publicMethods: [],
    });

    expect(qb.update).not.toHaveBeenCalled();
  });

  it('handles both publicMethods and availableMethods in one call', async () => {
    const knexMock = makeKnex([{ id: 3, name: 'POST' }]);
    const qb = makeQueryBuilder({
      getKnex: jest.fn(() => knexMock.knex),
    });
    const svc = makeService(qb);

    await (svc as any).updateRelations('enfyra_route', 7, {
      publicMethods: [],
      availableMethods: ['POST'],
    });

    expect(knexMock.rawCalls).toContainEqual({
      sql: 'delete from ?? where ?? = ?',
      bindings: [routeMethodJunction('publicMethods'), 'sourceId', 7],
    });
    expect(knexMock.rawCalls).toContainEqual({
      sql: 'insert into ?? (??, ??) values (?, ?)',
      bindings: [
        routeMethodJunction('availableMethods'),
        'sourceId',
        'targetId',
        7,
        3,
      ],
    });
  });

  it('writes Mongo route method relations directly to the metadata junction', async () => {
    DatabaseConfigService.overrideForTesting('mongodb');
    const mongoDb = makeMongoDb();
    const methodIds = ['653333333333333333333333', '654444444444444444444444'];
    const qb = makeQueryBuilder({
      getMongoDb: jest.fn(() => mongoDb),
      find: jest.fn().mockResolvedValue({
        data: methodIds.map((_id) => ({ _id })),
      }),
    });
    const svc = makeService(qb);

    await (svc as any).updateRelations(
      'enfyra_route',
      '655555555555555555555555',
      {
        availableMethods: ['GET', 'POST'],
      },
    );

    expect(qb.update).not.toHaveBeenCalled();
    expect(mongoDb.deletes).toEqual([
      {
        collection: 'j_availableMethods',
        condition: {
          sourceId: expect.any(Object),
        },
      },
    ]);
    expect(mongoDb.inserts).toHaveLength(1);
    expect(mongoDb.inserts[0].collection).toBe('j_availableMethods');
    expect(mongoDb.inserts[0].rows).toHaveLength(2);
    DatabaseConfigService.overrideForTesting('mysql');
  });
});

describe('DataMigrationService.migrateTable — end-to-end for publicMethods clear', () => {
  beforeEach(() => {
    DatabaseConfigService.overrideForTesting('mysql');
  });

  afterEach(() => {
    DatabaseConfigService.resetForTesting();
  });

  it('clears publicMethods on existing route when empty array specified', async () => {
    const qb = makeQueryBuilder({
      find: jest
        .fn()
        .mockResolvedValueOnce({ data: [{ id: 42 }] })
        .mockResolvedValueOnce({ data: [] }),
    });
    const svc = makeService(qb);

    await (svc as any).migrateTable('enfyra_route', [
      { _unique: { path: { _eq: '/metadata' } }, publicMethods: [] },
    ]);

    expect(qb.update).not.toHaveBeenCalled();
    expect(qb.__knexMock.rawCalls).toContainEqual({
      sql: 'delete from ?? where ?? = ?',
      bindings: [routeMethodJunction('publicMethods'), 'sourceId', 42],
    });
  });

  it('skips record not found in DB', async () => {
    const qb = makeQueryBuilder({
      find: jest.fn().mockResolvedValue({ data: [] }),
    });
    const svc = makeService(qb);

    await (svc as any).migrateTable('enfyra_route', [
      { _unique: { path: { _eq: '/nonexistent' } }, publicMethods: [] },
    ]);

    expect(qb.update).not.toHaveBeenCalled();
  });

  it('does not call updateRelations when no relation fields in record', async () => {
    const qb = makeQueryBuilder({
      find: jest.fn().mockResolvedValue({ data: [{ id: 1 }] }),
    });
    const svc = makeService(qb);

    await (svc as any).migrateTable('enfyra_route', [
      { _unique: { path: { _eq: '/me' } }, isEnabled: true },
    ]);

    expect(qb.update).toHaveBeenCalledTimes(1);
  });

  it('updates existing own-password field permission through data migration', async () => {
    const uniqueFilter = {
      _and: [
        { action: { _eq: 'update' } },
        { role: { _eq: null } },
        {
          column: {
            name: { _eq: 'password' },
            table: { name: { _eq: 'enfyra_user' } },
          },
        },
        {
          description: {
            _eq: 'Allow authenticated user to update own password via /me',
          },
        },
      ],
    };
    const qb = makeQueryBuilder({
      find: jest.fn().mockResolvedValue({ data: [{ id: 7 }] }),
    });
    const svc = makeService(qb);

    await (svc as any).migrateTable('enfyra_field_permission', [
      { _unique: uniqueFilter, isSystem: true },
    ]);

    expect(qb.find).toHaveBeenCalledWith({
      table: 'enfyra_field_permission',
      filter: uniqueFilter,
      limit: 1,
      fields: ['id'],
    });
    expect(qb.update).toHaveBeenCalledWith(
      'enfyra_field_permission',
      { where: [{ field: 'id', operator: '=', value: 7 }] },
      { isSystem: true },
    );
  });
});
