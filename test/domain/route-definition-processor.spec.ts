import { RouteDefinitionProcessor } from '../../src/domain/bootstrap';
import { DatabaseConfigService } from '../../src/shared/services';

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
      { id: 1, method: 'GET' },
      { id: 2, method: 'POST' },
    ]);
    const queryBuilder = {
      find: jest.fn().mockResolvedValue({ data: [] }),
      findOne: jest.fn().mockResolvedValue(null),
      insert: jest.fn().mockResolvedValue({ id: 42 }),
      update: jest.fn().mockResolvedValue(undefined),
      getKnex: jest.fn(() => knex.knex),
    } as any;

    const processor = new RouteDefinitionProcessor({ queryBuilderService: queryBuilder });

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
      'route_definition',
    );

    expect(queryBuilder.insert).toHaveBeenCalledWith(
      'route_definition',
      expect.not.objectContaining({
        availableMethods: expect.anything(),
        skipRoleGuardMethods: expect.anything(),
      }),
    );
    expect(knex.rawCalls).toContainEqual({
      sql: 'delete from ?? where ?? = ?',
      bindings: ['j_availableMethods', 'sourceId', 42],
    });
    expect(knex.rawCalls).toContainEqual({
      sql: 'insert into ?? (??, ??) values (?, ?), (?, ?)',
      bindings: ['j_availableMethods', 'sourceId', 'targetId', 42, 1, 42, 2],
    });
    expect(knex.rawCalls).toContainEqual({
      sql: 'insert into ?? (??, ??) values (?, ?)',
      bindings: ['j_skipRoleGuardMethods', 'sourceId', 'targetId', 42, 1],
    });
  });

  it('uses primitive insert result as inserted id', async () => {
    const knex = makeKnex([{ id: 1, method: 'GET' }]);
    const queryBuilder = {
      find: jest.fn().mockResolvedValue({ data: [] }),
      findOne: jest.fn().mockResolvedValue(null),
      insert: jest.fn().mockResolvedValue(99),
      update: jest.fn().mockResolvedValue(undefined),
      getKnex: jest.fn(() => knex.knex),
    } as any;

    const processor = new RouteDefinitionProcessor({ queryBuilderService: queryBuilder });

    await processor.processWithQueryBuilder(
      [{ path: '/assets/:id', availableMethods: ['GET'] }],
      queryBuilder,
      'route_definition',
    );

    expect(knex.rawCalls).toContainEqual({
      sql: 'insert into ?? (??, ??) values (?, ?)',
      bindings: ['j_availableMethods', 'sourceId', 'targetId', 99, 1],
    });
  });
});
