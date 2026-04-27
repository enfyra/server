import { describe, expect, it } from 'vitest';
import {
  getAccessibleMetadataTableNames,
  projectMetadataForUser,
} from '../../src/shared/utils/metadata-access.util';

function table(name: string, columns: any[] = [], relations: any[] = []) {
  return { id: name, name, columns, relations };
}

function col(name: string, extra: any = {}) {
  return { id: name, name, isPublished: true, ...extra };
}

function rel(propertyName: string, extra: any = {}) {
  return { id: propertyName, propertyName, isPublished: true, ...extra };
}

function metadata(tablesList: any[]) {
  return {
    tablesList,
    tables: new Map(tablesList.map((item) => [item.name, item])),
  };
}

function policyService() {
  return {
    checkRequestAccess(ctx: any) {
      const published = ctx.routeData?.publishedMethods?.some(
        (m: any) => m.method === ctx.method,
      );
      if (published) return { allow: true };
      if (!ctx.user) return { allow: false };
      if (ctx.user.isRootAdmin) return { allow: true };
      const roleId = String(ctx.user.role?.id ?? ctx.user.role?._id ?? '');
      const allowed = ctx.routeData?.routePermissions?.some((permission: any) => {
        const methodAllowed = permission.methods?.some(
          (item: any) => item.method === ctx.method,
        );
        return methodAllowed && String(permission.role?.id) === roleId;
      });
      return { allow: !!allowed };
    },
  } as any;
}

function routeCacheService(routes: any[]) {
  return { getRoutes: async () => routes };
}

function fieldPermissionCacheService(rules: any[]) {
  return {
    async ensureLoaded() {},
    async getPoliciesFor(user: any, tableName: string, action: string) {
      const roleId = String(user?.role?.id ?? '');
      const matched = rules.filter(
        (rule) =>
          rule.tableName === tableName &&
          rule.action === action &&
          (rule.roleId == null || String(rule.roleId) === roleId),
      );
      if (matched.length === 0) return [];
      return [
        {
          rules: matched.map((rule) => ({
            id: rule.id,
            isEnabled: true,
            effect: rule.effect,
            action: rule.action,
            tableName: rule.tableName,
            roleId: rule.roleId == null ? null : String(rule.roleId),
            allowedUserIds: [],
            columnName: rule.columnName ?? null,
            relationPropertyName: rule.relationPropertyName ?? null,
            condition: null,
          })),
          unconditionalAllowedColumns: new Set(
            matched
              .filter((rule) => rule.effect === 'allow' && rule.columnName)
              .map((rule) => rule.columnName),
          ),
          unconditionalAllowedRelations: new Set(
            matched
              .filter(
                (rule) => rule.effect === 'allow' && rule.relationPropertyName,
              )
              .map((rule) => rule.relationPropertyName),
          ),
          unconditionalDeniedColumns: new Set(),
          unconditionalDeniedRelations: new Set(),
        },
      ];
    },
  } as any;
}

describe('metadata access projection', () => {
  it('returns full metadata for root admin without applying route or field filters', async () => {
    const meta = metadata([
      table('secret_definition', [col('secret', { isPublished: false })]),
    ]);

    const data = await projectMetadataForUser({
      metadata: meta,
      user: { id: 1, isRootAdmin: true },
      policyService: policyService(),
      routeCacheService: routeCacheService([]),
      fieldPermissionCacheService: fieldPermissionCacheService([]),
    } as any);

    expect(data).toEqual(meta.tablesList);
  });

  it('allows anonymous users to see only metadata for published routes', async () => {
    const meta = metadata([
      table('public_post_definition', [col('title')]),
      table('private_post_definition', [col('title')]),
      table('user_definition', [col('email')]),
    ]);

    const data = await projectMetadataForUser({
      metadata: meta,
      user: null,
      policyService: policyService(),
      routeCacheService: routeCacheService([
        {
          path: '/public_post_definition',
          mainTable: { name: 'public_post_definition' },
          availableMethods: [{ method: 'GET' }],
          publishedMethods: [{ method: 'GET' }],
        },
        {
          path: '/private_post_definition',
          mainTable: { name: 'private_post_definition' },
          availableMethods: [{ method: 'GET' }],
          routePermissions: [
            { role: { id: 2 }, methods: [{ method: 'GET' }] },
          ],
        },
      ]),
      fieldPermissionCacheService: fieldPermissionCacheService([]),
    } as any);

    expect(data.map((item: any) => item.name)).toEqual([
      'public_post_definition',
    ]);
  });

  it('limits tables to route methods the user can access and includes own profile metadata', async () => {
    const meta = metadata([
      table('post_definition'),
      table('secret_definition'),
      table('user_definition'),
    ]);
    const user = { id: 10, role: { id: 2 } };

    const names = await getAccessibleMetadataTableNames({
      metadata: meta,
      user,
      policyService: policyService(),
      routeCacheService: routeCacheService([
        {
          path: '/post_definition',
          mainTable: { name: 'post_definition' },
          availableMethods: [{ method: 'GET' }],
          routePermissions: [
            { role: { id: 2 }, methods: [{ method: 'GET' }] },
          ],
        },
        {
          path: '/secret_definition',
          mainTable: { name: 'secret_definition' },
          availableMethods: [{ method: 'GET' }],
          routePermissions: [
            { role: { id: 3 }, methods: [{ method: 'GET' }] },
          ],
        },
      ]),
    } as any);

    expect([...names].sort()).toEqual([
      'post_definition',
      'user_definition',
    ]);
  });

  it('projects columns and relations by accessible action and field permissions', async () => {
    const meta = metadata([
      table(
        'post_definition',
        [
          col('id', { isPrimary: true, isPublished: false }),
          col('title'),
          col('privateNote', { isPublished: false }),
          col('writeOnlyToken', { isPublished: false }),
          col('deniedTitle'),
        ],
        [
          rel('author'),
          rel('internalAudit', { isPublished: false }),
        ],
      ),
    ]);
    const user = { id: 10, role: { id: 2 } };

    const data = await projectMetadataForUser({
      metadata: meta,
      user,
      policyService: policyService(),
      routeCacheService: routeCacheService([
        {
          path: '/post_definition',
          mainTable: { name: 'post_definition' },
          availableMethods: [{ method: 'GET' }, { method: 'POST' }],
          routePermissions: [
            {
              role: { id: 2 },
              methods: [{ method: 'GET' }, { method: 'POST' }],
            },
          ],
        },
      ]),
      fieldPermissionCacheService: fieldPermissionCacheService([
        {
          id: 1,
          tableName: 'post_definition',
          action: 'read',
          effect: 'allow',
          columnName: 'privateNote',
          roleId: 2,
        },
        {
          id: 2,
          tableName: 'post_definition',
          action: 'create',
          effect: 'allow',
          columnName: 'writeOnlyToken',
          roleId: 2,
        },
        {
          id: 3,
          tableName: 'post_definition',
          action: 'read',
          effect: 'deny',
          columnName: 'deniedTitle',
          roleId: 2,
        },
        {
          id: 4,
          tableName: 'post_definition',
          action: 'read',
          effect: 'allow',
          relationPropertyName: 'internalAudit',
          roleId: 2,
        },
      ]),
    } as any);

    const [projected] = data;
    expect(projected.columns.map((item: any) => item.name).sort()).toEqual([
      'deniedTitle',
      'id',
      'privateNote',
      'title',
      'writeOnlyToken',
    ]);
    const deniedTitle = projected.columns.find(
      (item: any) => item.name === 'deniedTitle',
    );
    expect(deniedTitle.metadataAccess).toMatchObject({
      read: false,
      create: true,
    });
    const writeOnlyToken = projected.columns.find(
      (item: any) => item.name === 'writeOnlyToken',
    );
    expect(writeOnlyToken.metadataAccess).toMatchObject({
      read: false,
      create: true,
    });
    expect(projected.relations.map((item: any) => item.propertyName).sort()).toEqual([
      'author',
      'internalAudit',
    ]);
  });

  it('does not expose create-only fields when the user only has read route access', async () => {
    const meta = metadata([
      table('post_definition', [
        col('id', { isPrimary: true, isPublished: false }),
        col('title'),
        col('draftToken', { isPublished: false }),
      ]),
    ]);

    const data = await projectMetadataForUser({
      metadata: meta,
      user: { id: 10, role: { id: 2 } },
      policyService: policyService(),
      routeCacheService: routeCacheService([
        {
          path: '/post_definition',
          mainTable: { name: 'post_definition' },
          availableMethods: [{ method: 'GET' }],
          routePermissions: [
            { role: { id: 2 }, methods: [{ method: 'GET' }] },
          ],
        },
      ]),
      fieldPermissionCacheService: fieldPermissionCacheService([
        {
          id: 1,
          tableName: 'post_definition',
          action: 'create',
          effect: 'allow',
          columnName: 'draftToken',
          roleId: 2,
        },
      ]),
    } as any);

    expect(data[0].columns.map((item: any) => item.name)).toEqual([
      'id',
      'title',
    ]);
  });

  it('does not expose a published field denied for read on a read-only route', async () => {
    const meta = metadata([
      table('post_definition', [
        col('id', { isPrimary: true }),
        col('title'),
        col('salary'),
      ]),
    ]);

    const data = await projectMetadataForUser({
      metadata: meta,
      user: { id: 10, role: { id: 2 } },
      policyService: policyService(),
      routeCacheService: routeCacheService([
        {
          path: '/post_definition',
          mainTable: { name: 'post_definition' },
          availableMethods: [{ method: 'GET' }],
          routePermissions: [
            { role: { id: 2 }, methods: [{ method: 'GET' }] },
          ],
        },
      ]),
      fieldPermissionCacheService: fieldPermissionCacheService([
        {
          id: 1,
          tableName: 'post_definition',
          action: 'read',
          effect: 'deny',
          columnName: 'salary',
          roleId: 2,
        },
      ]),
    } as any);

    expect(data[0].columns.map((item: any) => item.name)).toEqual([
      'id',
      'title',
    ]);
  });

  it('returns null for a single table request when the user cannot access that table', async () => {
    const meta = metadata([
      table('post_definition', [col('title')]),
      table('secret_definition', [col('secret')]),
    ]);

    const data = await projectMetadataForUser({
      metadata: meta,
      user: { id: 10, role: { id: 2 } },
      policyService: policyService(),
      routeCacheService: routeCacheService([
        {
          path: '/post_definition',
          mainTable: { name: 'post_definition' },
          availableMethods: [{ method: 'GET' }],
          routePermissions: [
            { role: { id: 2 }, methods: [{ method: 'GET' }] },
          ],
        },
      ]),
      fieldPermissionCacheService: fieldPermissionCacheService([]),
      tableName: 'secret_definition',
    } as any);

    expect(data).toBeNull();
  });
});
