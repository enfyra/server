import { EventEmitter2 } from 'eventemitter2';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { RouteCacheService } from '../../src/engines/cache/services/route-cache.service';
import { DatabaseConfigService } from '../../src/shared/services';
import { PolicyService } from '../../src/domain/policy';

const methods = Array.from({ length: 12 }, (_, index) => ({
  id: index + 1,
  name: index === 11 ? 'POST' : `METHOD_${index}`,
}));
const take = (rows: any[], options: any) =>
  structuredClone(
    options?.limit === -1 ? rows : rows.slice(0, options?.limit ?? 10),
  );

function fixture() {
  DatabaseConfigService.overrideForTesting('postgres');
  const hook = (id: number, isGlobal = false) => ({
    id,
    priority: id,
    isEnabled: true,
    isGlobal,
    methods,
    sourceCode: `@SHARE.steps.push(${id});`,
    scriptLanguage: 'javascript',
  });
  const localPre = Array.from({ length: 12 }, (_, index) => hook(index + 1));
  const localPost = Array.from({ length: 12 }, (_, index) => hook(index + 21));
  const globalPre = Array.from({ length: 12 }, (_, index) =>
    hook(index + 41, true),
  );
  const globalPost = Array.from({ length: 12 }, (_, index) =>
    hook(index + 61, true),
  );
  const permissions = Array.from({ length: 12 }, (_, index) => ({
    id: index + 1,
    isEnabled: true,
    methods,
    allowedUsers: Array.from({ length: 12 }, (_, user) => ({
      id: `user-${user}`,
    })),
  }));
  let omitLastBinding = false;
  const find = vi.fn(async (options: any) => {
    const { table, deep = {} } = options;
    const hooks = (rows: any[], scope: any) =>
      take(rows, scope).map((row) => ({
        ...row,
        methods: take(row.methods, scope?.deep?.methods),
      }));
    const grants = (scope: any) =>
      take(permissions, scope).map((row) => ({
        ...row,
        methods: take(row.methods, scope?.deep?.methods),
        allowedUsers: take(row.allowedUsers, scope?.deep?.allowedUsers),
      }));
    if (table === 'enfyra_method')
      return { data: take(methods, { limit: options.limit ?? -1 }) };
    if (table === 'enfyra_pre_hook' || table === 'enfyra_post_hook') {
      if (options.filter?.id) return { data: [{ id: 12, route: { id: 1 } }] };
      return {
        data: hooks(table === 'enfyra_pre_hook' ? globalPre : globalPost, {
          ...options,
          limit: options.limit ?? -1,
          deep,
        }),
      };
    }
    if (table !== 'enfyra_route') throw new Error(`Unexpected table: ${table}`);
    return {
      data: [
        {
          id: 1,
          path: '/items',
          preHooks: hooks(localPre, deep.preHooks),
          postHooks: hooks(localPost, deep.postHooks),
          routePermissions: grants(deep.routePermissions),
          handlers: take(
            methods.map((method) => ({ id: method.id, method })),
            deep.handlers,
          ),
          availableMethods: take(methods, deep.availableMethods),
          publicMethods: take(methods, deep.publicMethods),
          skipRoleGuardMethods: take(methods, deep.skipRoleGuardMethods),
          methodConfigs: take(
            methods.map((method) => ({
              id: method.id,
              method: { id: method.id },
              available: true,
              preHooks: localPre
                .slice(0, omitLastBinding ? 11 : 12)
                .map(({ id }) => ({ id })),
              postHooks: localPost.map(({ id }) => ({ id })),
              routePermissions: permissions,
              fileFields: Array.from({ length: 12 }, (_, index) => ({
                id: index + 1,
                sort: index,
              })),
            })),
            deep.methodConfigs,
          ).map((operation) => ({
            ...operation,
            preHooks: take(
              operation.preHooks,
              deep.methodConfigs?.deep?.preHooks,
            ),
            postHooks: take(
              operation.postHooks,
              deep.methodConfigs?.deep?.postHooks,
            ),
            routePermissions: grants(
              deep.methodConfigs?.deep?.routePermissions,
            ),
            fileFields: take(
              operation.fileFields,
              deep.methodConfigs?.deep?.fileFields,
            ),
          })),
        },
      ],
    };
  });
  const cache = new RouteCacheService({
    eventEmitter: new EventEmitter2(),
    metadataCacheService: { getMetadata: async () => ({ tablesList: [] }) },
    queryBuilderService: { isMongoDb: () => false, find },
  } as any);
  return {
    cache,
    find,
    omitLastBinding: () => {
      omitLastBinding = true;
    },
  };
}

afterEach(() => DatabaseConfigService.resetForTesting());

describe('route cache complete relation hydration', () => {
  it.each(['full', 'route', 'pre-hook', 'post-hook'])(
    'retains every hook and principal on %s reload',
    async (mode) => {
      const { cache, find } = fixture();
      await cache.reload(false);
      if (mode !== 'full') {
        await cache.partialReload(
          {
            table:
              mode === 'route'
                ? 'enfyra_route'
                : mode === 'pre-hook'
                  ? 'enfyra_pre_hook'
                  : 'enfyra_post_hook',
            action: 'reload',
            scope: 'partial',
            ids: [mode === 'route' ? 1 : 12],
            timestamp: Date.now(),
          },
          false,
        );
      }
      const data = await cache.getCacheAsync();
      expect(data.methods).toHaveLength(12);
      expect(data.globalPreHooks).toHaveLength(12);
      expect(data.globalPostHooks).toHaveLength(12);
      const route = data.routes[0];
      expect(route.methodConfigs).toHaveLength(12);
      const operation = route.methodConfigs.find(
        (item: any) => item.method.name === 'POST',
      );
      expect(operation).toBeDefined();
      expect(operation.preHooks.map((item: any) => item.id)).toEqual([
        ...Array.from({ length: 12 }, (_, index) => index + 1),
        ...Array.from({ length: 12 }, (_, index) => index + 41),
      ]);
      expect(operation.postHooks.map((item: any) => item.id)).toEqual([
        ...Array.from({ length: 12 }, (_, index) => index + 21),
        ...Array.from({ length: 12 }, (_, index) => index + 61),
      ]);
      expect(operation.fileFields).toHaveLength(12);
      for (const grants of [
        route.routePermissions,
        operation.routePermissions,
      ]) {
        expect(grants).toHaveLength(12);
        expect(grants[11].allowedUsers).toContainEqual({ id: 'user-11' });
      }
      const policy = new PolicyService({} as any);
      expect(
        policy.checkRequestAccess({
          method: 'POST',
          user: { id: 'user-11', roles: [] },
          routeData: { routePermissions: operation.routePermissions },
        }).allow,
      ).toBe(true);
      expect(
        policy.checkRequestAccess({
          method: 'POST',
          user: { id: 'outsider', roles: [] },
          routeData: { routePermissions: operation.routePermissions },
        }).allow,
      ).toBe(false);
      for (const [options] of find.mock.calls.filter(
        ([options]) => options.table === 'enfyra_route',
      )) {
        expect(options.deep.methodConfigs.deep.preHooks.limit).toBe(-1);
        expect(options.deep.methodConfigs.deep.postHooks.limit).toBe(-1);
        expect(options.deep.preHooks.limit).toBe(-1);
        expect(options.deep.postHooks.limit).toBe(-1);
      }
    },
  );

  it('does not activate a route hook without the method binding after a partial reload', async () => {
    const { cache, omitLastBinding } = fixture();
    await cache.reload(false);
    omitLastBinding();
    await cache.partialReload(
      {
        table: 'enfyra_route',
        action: 'reload',
        scope: 'partial',
        ids: [1],
        timestamp: Date.now(),
      },
      false,
    );
    const operation = (await cache.getRoutes())[0].methodConfigs.find(
      (item: any) => item.method.name === 'POST',
    );
    expect(operation.preHooks.map((item: any) => item.id)).not.toContain(12);
    expect(operation.preHooks).toHaveLength(23);
  });
});
