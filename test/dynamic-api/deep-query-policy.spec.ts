import { describe, expect, it, vi } from 'vitest';
import { DynamicReadAuthorizationService } from '../../src/modules/dynamic-api/services/dynamic-read-authorization.service';
import { DynamicRepositoryReadService } from '../../src/modules/dynamic-api/services/dynamic-repository-read.service';
import type { RuntimeRegistryService } from '../../src/engines/cache/services/runtime-registry.service';

function fixture(condition: unknown = null, effect = 'deny', published = true) {
  const child = {
    name: 'children',
    columns: [
      { name: 'id', isPrimary: true },
      { name: 'score', isPublished: published },
    ],
    relations: [],
  };
  const parent = {
    name: 'parents',
    columns: [{ name: 'id', isPrimary: true }],
    relations: [
      {
        propertyName: 'children',
        targetTableName: 'children',
        type: 'one-to-many',
      },
    ],
  };
  const tables = new Map([
    ['parents', parent],
    ['children', child],
  ]);
  const registry = {
    lookupTableByName: (name: string) => tables.get(name),
    requireMetadata: () => ({ tables, tablesList: [...tables.values()] }),
    getMaxQueryDepth: () => 10,
    getFieldPermissionPoliciesFor: (_user: unknown, name: string) =>
      name === 'children'
        ? [
            {
              rules: [
                {
                  id: 1,
                  tableName: 'children',
                  columnName: 'score',
                  action: 'read',
                  effect,
                  isEnabled: true,
                  roleId: null,
                  allowedUserIds: [],
                  condition,
                },
              ],
            },
          ]
        : [],
  } as unknown as RuntimeRegistryService;
  const service = new DynamicReadAuthorizationService({
    runtimeRegistryService: registry,
  });
  return { registry, service };
}

describe('caller policy before nested query execution', () => {
  it.each([
    { filter: { score: { _gte: 10 } } },
    { sort: '-score' },
    { filter: { _or: [{ score: { _gte: 10 } }, { id: { _eq: 1 } }] } },
  ])('rejects denied deep influence %j with 403', async (entry) => {
    const { service } = fixture();
    await expect(
      service.stripDeniedFields({
        tableName: 'parents',
        fields: 'id,children.id',
        deep: { children: { fields: 'id', ...entry } },
        context: { $user: { id: 'reader' } },
        enforceFieldPermission: true,
      }),
    ).rejects.toMatchObject({ statusCode: 403 });
  });
  it.each([null, { score: { _gte: 10 } }])(
    'checks nested root filters and dotted sorts (condition=%j)',
    async (condition) => {
      const { service } = fixture(condition);
      for (const query of [
        { filter: { children: { score: { _gte: 10 } } } },
        { sort: '-children.score' },
      ]) {
        await expect(
          service.assertQueryAllowed({
            tableName: 'parents',
            context: { $user: { id: 'reader' }, $query: query },
            enforceFieldPermission: true,
          }),
        ).rejects.toMatchObject({ statusCode: 403 });
      }
    },
  );
  it('allows explicit read grants on an unpublished deep field', async () => {
    const { service } = fixture(null, 'allow', false);
    await expect(
      service.stripDeniedFields({
        tableName: 'parents',
        fields: 'id',
        deep: {
          children: {
            fields: 'id',
            filter: { score: { _gte: 10 } },
            sort: '-score',
          },
        },
        context: { $user: { id: 'reader' } },
        enforceFieldPermission: true,
      }),
    ).resolves.toMatchObject({
      deep: { children: { sort: '-score', filter: { score: { _gte: 10 } } } },
    });
  });
  it('preserves root and trusted repository capabilities', async () => {
    const { service } = fixture();
    for (const [user, enforce] of [
      [{ isRootAdmin: true }, true],
      [{ id: 'reader' }, false],
    ] as const) {
      await expect(
        service.stripDeniedFields({
          tableName: 'parents',
          fields: 'id',
          deep: { children: { sort: '-score' } },
          context: { $user: user },
          enforceFieldPermission: enforce,
        }),
      ).resolves.toMatchObject({ deep: { children: { sort: '-score' } } });
    }
  });
  it('authorizes explicit find options, not an unrelated empty request query', async () => {
    const { registry, service } = fixture();
    const queryBuilder = {
      find: vi.fn(async () => ({ data: [] })),
      getPkField: () => 'id',
    };
    const reader = new DynamicRepositoryReadService({
      tableName: 'children',
      context: { $user: { id: 'reader' }, $query: {} } as never,
      enforceFieldPermission: true,
      runtimeRegistryService: registry,
      readAuthorizationService: service,
      queryBuilderService: queryBuilder as never,
    });
    await expect(
      reader.find({ fields: 'id', filter: { score: { _gte: 10 } } }),
    ).rejects.toMatchObject({ statusCode: 403 });
    expect(queryBuilder.find).not.toHaveBeenCalled();
  });
});
