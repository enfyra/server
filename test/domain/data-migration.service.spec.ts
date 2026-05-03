import { DataMigrationService } from '../../src/engines/bootstrap';
import { DatabaseConfigService } from '../../src/shared/services';

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
    if (table === 'method_definition') {
      return {
        select: jest.fn().mockReturnThis(),
        whereIn: jest.fn((_field: string, values: string[]) =>
          Promise.resolve(
            methodRows.filter((method) => values.includes(method.method)),
          ),
        ),
      };
    }
    if (table === 'relation_definition as r') {
      let propertyName = 'publishedMethods';
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
      if (name === 'table_definition') {
        return {
          findOne: jest.fn(async (filter: any) => {
            if (filter.name === 'route_definition') {
              return { _id: routeTableId, name: 'route_definition' };
            }
            if (filter.name === 'method_definition') {
              return { _id: methodTableId, name: 'method_definition' };
            }
            return null;
          }),
        };
      }
      if (name === 'relation_definition') {
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
  it('captures non-empty publishedMethods as relation update', () => {
    const svc = makeService(makeQueryBuilder());
    const { newRecord, relationUpdates } = (svc as any).transformRecord(
      'route_definition',
      {
        _unique: { path: { _eq: '/me' } },
        publishedMethods: ['GET', 'POST'],
        isEnabled: true,
      },
    );
    expect(relationUpdates.publishedMethods).toEqual(['GET', 'POST']);
    expect(newRecord.publishedMethods).toBeUndefined();
    expect(newRecord.isEnabled).toBe(true);
  });

  it('captures empty array publishedMethods as relation update (the bug fix)', () => {
    const svc = makeService(makeQueryBuilder());
    const { newRecord, relationUpdates } = (svc as any).transformRecord(
      'route_definition',
      { _unique: { path: { _eq: '/metadata' } }, publishedMethods: [] },
    );
    expect(relationUpdates.publishedMethods).toEqual([]);
    expect(newRecord.publishedMethods).toBeUndefined();
  });

  it('captures empty array availableMethods as relation update', () => {
    const svc = makeService(makeQueryBuilder());
    const { newRecord, relationUpdates } = (svc as any).transformRecord(
      'route_definition',
      { _unique: { path: { _eq: '/test' } }, availableMethods: [] },
    );
    expect(relationUpdates.availableMethods).toEqual([]);
    expect(newRecord.availableMethods).toBeUndefined();
  });

  it('does not capture undefined relation field', () => {
    const svc = makeService(makeQueryBuilder());
    const { relationUpdates } = (svc as any).transformRecord(
      'route_definition',
      { _unique: { path: { _eq: '/test' } }, name: 'hello' },
    );
    expect(relationUpdates.publishedMethods).toBeUndefined();
    expect(relationUpdates.availableMethods).toBeUndefined();
  });

  it('does not capture null relation field', () => {
    const svc = makeService(makeQueryBuilder());
    const { relationUpdates } = (svc as any).transformRecord(
      'route_definition',
      { _unique: { path: { _eq: '/test' } }, publishedMethods: null },
    );
    expect(relationUpdates.publishedMethods).toBeUndefined();
  });
});

describe('DataMigrationService.updateRelations', () => {
  beforeEach(() => {
    DatabaseConfigService.overrideForTesting('mysql');
  });

  afterEach(() => {
    DatabaseConfigService.resetForTesting();
  });

  it('calls update with empty array to clear publishedMethods (the bug fix)', async () => {
    const qb = makeQueryBuilder({
      find: jest.fn().mockResolvedValue({ data: [] }),
    });
    const svc = makeService(qb);

    await (svc as any).updateRelations('route_definition', 99, {
      publishedMethods: [],
    });

    expect(qb.__knexMock.rawCalls).toContainEqual({
      sql: 'delete from ?? where ?? = ?',
      bindings: ['j_publishedMethods', 'sourceId', 99],
    });
  });

  it('calls update with method IDs when methods are provided', async () => {
    const knexMock = makeKnex([
      { id: 1, method: 'GET' },
      { id: 2, method: 'POST' },
    ]);
    const qb = makeQueryBuilder({
      getKnex: jest.fn(() => knexMock.knex),
    });
    const svc = makeService(qb);

    await (svc as any).updateRelations('route_definition', 10, {
      publishedMethods: ['GET', 'POST'],
    });

    expect(knexMock.rawCalls).toContainEqual({
      sql: 'insert into ?? (??, ??) values (?, ?), (?, ?)',
      bindings: ['j_publishedMethods', 'sourceId', 'targetId', 10, 1, 10, 2],
    });
  });

  it('clears availableMethods when provided empty array', async () => {
    const qb = makeQueryBuilder({
      find: jest.fn().mockResolvedValue({ data: [] }),
    });
    const svc = makeService(qb);

    await (svc as any).updateRelations('route_definition', 5, {
      availableMethods: [],
    });

    expect(qb.__knexMock.rawCalls).toContainEqual({
      sql: 'delete from ?? where ?? = ?',
      bindings: ['j_availableMethods', 'sourceId', 5],
    });
  });

  it('does nothing for non-route_definition tables', async () => {
    const qb = makeQueryBuilder();
    const svc = makeService(qb);

    await (svc as any).updateRelations('user_definition', 1, {
      publishedMethods: [],
    });

    expect(qb.update).not.toHaveBeenCalled();
  });

  it('handles both publishedMethods and availableMethods in one call', async () => {
    const knexMock = makeKnex([{ id: 3, method: 'POST' }]);
    const qb = makeQueryBuilder({
      getKnex: jest.fn(() => knexMock.knex),
    });
    const svc = makeService(qb);

    await (svc as any).updateRelations('route_definition', 7, {
      publishedMethods: [],
      availableMethods: ['POST'],
    });

    expect(knexMock.rawCalls).toContainEqual({
      sql: 'delete from ?? where ?? = ?',
      bindings: ['j_publishedMethods', 'sourceId', 7],
    });
    expect(knexMock.rawCalls).toContainEqual({
      sql: 'insert into ?? (??, ??) values (?, ?)',
      bindings: ['j_availableMethods', 'sourceId', 'targetId', 7, 3],
    });
  });

  it('writes Mongo route method relations directly to the metadata junction', async () => {
    DatabaseConfigService.overrideForTesting('mongodb');
    const mongoDb = makeMongoDb();
    const methodIds = [
      '653333333333333333333333',
      '654444444444444444444444',
    ];
    const qb = makeQueryBuilder({
      getMongoDb: jest.fn(() => mongoDb),
      find: jest.fn().mockResolvedValue({
        data: methodIds.map((_id) => ({ _id })),
      }),
    });
    const svc = makeService(qb);

    await (svc as any).updateRelations(
      'route_definition',
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

describe('DataMigrationService.migrateTable — end-to-end for publishedMethods clear', () => {
  beforeEach(() => {
    DatabaseConfigService.overrideForTesting('mysql');
  });

  afterEach(() => {
    DatabaseConfigService.resetForTesting();
  });

  it('clears publishedMethods on existing route when empty array specified', async () => {
    const qb = makeQueryBuilder({
      find: jest
        .fn()
        .mockResolvedValueOnce({ data: [{ id: 42 }] })
        .mockResolvedValueOnce({ data: [] }),
    });
    const svc = makeService(qb);

    await (svc as any).migrateTable('route_definition', [
      { _unique: { path: { _eq: '/metadata' } }, publishedMethods: [] },
    ]);

    expect(qb.update).not.toHaveBeenCalled();
    expect(qb.__knexMock.rawCalls).toContainEqual({
      sql: 'delete from ?? where ?? = ?',
      bindings: ['j_publishedMethods', 'sourceId', 42],
    });
  });

  it('skips record not found in DB', async () => {
    const qb = makeQueryBuilder({
      find: jest.fn().mockResolvedValue({ data: [] }),
    });
    const svc = makeService(qb);

    await (svc as any).migrateTable('route_definition', [
      { _unique: { path: { _eq: '/nonexistent' } }, publishedMethods: [] },
    ]);

    expect(qb.update).not.toHaveBeenCalled();
  });

  it('does not call updateRelations when no relation fields in record', async () => {
    const qb = makeQueryBuilder({
      find: jest.fn().mockResolvedValue({ data: [{ id: 1 }] }),
    });
    const svc = makeService(qb);

    await (svc as any).migrateTable('route_definition', [
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
            table: { name: { _eq: 'user_definition' } },
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

    await (svc as any).migrateTable('field_permission_definition', [
      { _unique: uniqueFilter, isSystem: true },
    ]);

    expect(qb.find).toHaveBeenCalledWith({
      table: 'field_permission_definition',
      filter: uniqueFilter,
      limit: 1,
      fields: ['id'],
    });
    expect(qb.update).toHaveBeenCalledWith(
      'field_permission_definition',
      { where: [{ field: 'id', operator: '=', value: 7 }] },
      { isSystem: true },
    );
  });
});
