import { describe, expect, it } from 'vitest';
import { DynamicReadAuthorizationService } from '../../src/modules/dynamic-api/services/dynamic-read-authorization.service';
import { DynamicRepositoryReadService } from '../../src/modules/dynamic-api/services/dynamic-repository-read.service';

function registry(tables: Map<string, any>, rules: any[] = []) {
  return {
    lookupTableByName: (name: string) => tables.get(name),
    requireMetadata: () => ({ tables, tablesList: [...tables.values()] }),
    getFieldPermissionPoliciesFor: () => (rules.length ? [{ rules }] : []),
    getMaxQueryDepth: () => 10,
  };
}

function payrollFixture(conditional: boolean) {
  const payroll = {
    name: 'payroll',
    columns: [
      { name: 'id', isPrimary: true, isPublished: true },
      { name: 'amount', isPublished: true },
      { name: 'ownerId', isPublished: true },
    ],
    relations: [],
  };
  const tables = new Map([['payroll', payroll]]);
  const rules = conditional
    ? [
        {
          tableName: 'payroll',
          columnName: 'amount',
          action: 'read',
          effect: 'deny',
          isEnabled: true,
          allowedUserIds: [],
          roleId: null,
          condition: { ownerId: { _eq: '@USER.id' } },
        },
      ]
    : [];
  const aggregate = () => ({ data: [{ total: 300 }] });
  const aggregateSpy = [] as unknown[];
  const service = new DynamicRepositoryReadService({
    context: { $user: { id: 'reader' }, $query: {} } as any,
    enforceFieldPermission: true,
    queryBuilderService: {
      find: async () => ({ data: [{ id: 1 }], meta: { filterCount: 1 } }),
      aggregate: async () => {
        aggregateSpy.push('called');
        return aggregate();
      },
      getPkField: () => 'id',
    } as any,
    readAuthorizationService: new DynamicReadAuthorizationService({
      runtimeRegistryService: registry(tables, rules) as any,
    }),
    runtimeRegistryService: registry(tables, rules) as any,
    tableName: 'payroll',
  });
  return { service, aggregateSpy };
}

describe('aggregate read authorization', () => {
  it('refuses a conditionally denied measure instead of evaluating it against null', async () => {
    const { service, aggregateSpy } = payrollFixture(true);
    await expect(
      service.aggregate({ measures: { total: { sum: 'amount' } } }),
    ).rejects.toThrow('permission');
    expect(aggregateSpy).toHaveLength(0);
  });

  it('still admits an unconditional measure', async () => {
    const { service, aggregateSpy } = payrollFixture(false);    await expect(
      service.aggregate({ measures: { total: { sum: 'amount' } } }),
    ).resolves.toBeDefined();
    expect(aggregateSpy).toHaveLength(1);
  });

  it('authorizes the aggregate filter and sort through the read field path', async () => {
    const payroll = {
      name: 'payroll',
      columns: [
        { name: 'id', isPrimary: true, isPublished: true },
        { name: 'amount', isPublished: true },
        { name: 'internal', isPublished: false },
      ],
      relations: [],
    };
    const tables = new Map([['payroll', payroll]]);
    const service = new DynamicRepositoryReadService({
      context: { $user: { id: 'reader' }, $query: {} } as any,
      enforceFieldPermission: true,
      queryBuilderService: {
        find: async () => ({ data: [] }),
        aggregate: async () => {
          throw new Error('aggregate must not run');
        },
        getPkField: () => 'id',
      } as any,
      readAuthorizationService: new DynamicReadAuthorizationService({
        runtimeRegistryService: registry(tables) as any,
      }),
      runtimeRegistryService: registry(tables) as any,
      tableName: 'payroll',
    });

    await expect(
      service.aggregate({
        filter: { internal: { _eq: 'secret' } },
        measures: { total: { sum: 'amount' } },
      }),
    ).rejects.toThrow("filter column 'internal' on table 'payroll'");
    await expect(
      service.aggregate({
        sort: [{ field: 'internal', direction: 'desc' }],
        measures: { total: { sum: 'amount' } },
      }),
    ).rejects.toThrow("filter column 'internal' on table 'payroll'");
  });
});

describe('aggregate-sort path normalization', () => {
  const items = {
    name: 'items',
    columns: [
      { name: 'id', isPrimary: true, isPublished: true },
      { name: 'secret', isPublished: false },
    ],
    relations: [],
  };
  const teams = {
    name: 'teams',
    columns: [{ name: 'id', isPrimary: true, isPublished: true }],
    relations: [
      {
        propertyName: 'children',
        targetTableName: 'items',
        type: 'one-to-many',
        isPublished: true,
      },
    ],
  };
  const tables = new Map([
    ['teams', teams],
    ['items', items],
  ]);
  const service = new DynamicReadAuthorizationService({
    runtimeRegistryService: registry(tables) as any,
  });
  const authorize = (sort: string) =>
    service.assertQueryAllowed({
      tableName: 'teams',
      context: { $user: { id: 'reader' }, $query: { sort } },
      enforceFieldPermission: true,
    });

  it('rejects the canonical aggregate-sort path', async () => {
    await expect(authorize('_max(children.secret)')).rejects.toThrow(
      "filter column 'secret' on table 'items'",
    );
  });

  it.each(['_max( children.secret)', '_max(children.secret )', '_max( children.secret )'])(
    'rejects a whitespace-padded aggregate-sort path %s',
    async (sort) => {
      await expect(authorize(sort)).rejects.toThrow(
        "filter column 'secret' on table 'items'",
      );
    },
  );

  it('rejects whitespace-padded aggregate-count relation paths', async () => {
    const deniedRelation = {
      name: 'teams',
      columns: [{ name: 'id', isPrimary: true, isPublished: true }],
      relations: [
        {
          propertyName: 'hidden',
          targetTableName: 'items',
          type: 'one-to-many',
          isPublished: false,
        },
      ],
    };
    const localTables = new Map([
      ['teams', deniedRelation],
      ['items', items],
    ]);
    const localService = new DynamicReadAuthorizationService({
      runtimeRegistryService: registry(localTables) as any,
    });
    await expect(
      localService.assertQueryAllowed({
        tableName: 'teams',
        context: { $user: { id: 'reader' }, $query: { sort: '_count( hidden)' } },
        enforceFieldPermission: true,
      }),
    ).rejects.toThrow("filter relation 'hidden' on table 'teams'");
  });
});
