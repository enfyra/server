import { EventEmitter2 } from 'eventemitter2';
import { describe, expect, it, vi } from 'vitest';
import { GuardCacheBuilder } from '../../src/engines/cache/services/guard-cache-builder.service';
import { GuardEvaluatorService } from '../../src/engines/cache/services/guard-evaluator.service';
import { GqlDefinitionCacheService } from '../../src/engines/cache/services/gql-definition-cache.service';
import { RuntimeRegistryService } from '../../src/engines/cache/services/runtime-registry.service';
import { CACHE_IDENTIFIERS } from '../../src/shared/utils/cache-events.constants';
import { hasGraphqlOperationAccess } from '../../src/modules/graphql/utils/graphql-access.util';

const take = (rows: any[], options: any) =>
  structuredClone(
    options?.limit === -1 ? rows : rows.slice(0, options?.limit ?? 10),
  );
const users = Array.from({ length: 12 }, (_, index) => ({
  id: `user-${index}`,
}));
const methods = Array.from({ length: 12 }, (_, index) => ({
  id: index + 1,
  name: `METHOD_${index}`,
}));
const excluded = Array.from({ length: 12 }, (_, index) => ({
  id: index + 1,
  path: `/excluded-${index}`,
}));

describe('authorization cache complete relation hydration', () => {
  it.each([false, true])(
    'preserves guard method/exclusion/user scopes after reload (Mongo=%s)',
    async (isMongoDb) => {
      const find = vi.fn(async ({ table, deep = {} }: any) => ({
        data:
          table === 'enfyra_guard'
            ? [
                {
                  id: 1,
                  name: 'global',
                  position: 'post_auth',
                  isEnabled: true,
                  isGlobal: true,
                  methods: take(methods, deep.methods),
                  excludeRoutes: take(excluded, deep.excludeRoutes),
                },
                {
                  id: 2,
                  name: 'scoped',
                  position: 'post_auth',
                  isEnabled: true,
                  isGlobal: false,
                  route: { id: 20, path: '/scoped' },
                  methods: [],
                  routeMethodConfigs: take(
                    methods.map((method) => ({ id: method.id, method })),
                    deep.routeMethodConfigs,
                  ),
                },
              ]
            : [
                {
                  id: 1,
                  guard: { id: 1 },
                  type: 'ip_blacklist',
                  config: { ips: ['127.0.0.1'] },
                  isEnabled: true,
                  users: take(users, deep.users),
                },
              ],
      }));
      const cache = new GuardCacheBuilder({
        queryBuilderService: { find, isMongoDb: () => isMongoDb } as any,
        eventEmitter: new EventEmitter2(),
      });
      const registry = new RuntimeRegistryService();
      const evaluator = new GuardEvaluatorService({
        rateLimitService: {} as any,
      });
      for (let reload = 0; reload < 2; reload++) {
        await cache.reload(false);
        await registry.publishFromCache(CACHE_IDENTIFIERS.GUARD, cache);
        const guard = cache.getRawCache().postAuthGlobal[0];
        expect(guard.methods).toContain('METHOD_11');
        expect(guard.excludeRoutePaths).toContain('/excluded-11');
        expect(guard.rules[0].userIds).toContain('user-11');
        expect(
          registry.getGuardsForRoute('post_auth', '/excluded-11', 'METHOD_11'),
        ).toEqual([]);
        expect(
          registry
            .getGuardsForRoute('post_auth', '/scoped', 'METHOD_11')
            .map((item) => item.id),
        ).toContain(2);
        expect(
          (
            await evaluator.evaluateGuard(guard, {
              userId: 'user-11',
              clientIp: '127.0.0.1',
              routePath: '/items',
            })
          ).reject,
        ).toMatchObject({ errorCode: 'IP_BLOCKED' });
        expect(
          (
            await evaluator.evaluateGuard(guard, {
              userId: 'outsider',
              clientIp: '127.0.0.1',
              routePath: '/items',
            })
          ).reject,
        ).toBeNull();
      }
    },
  );

  it('preserves the twelfth GraphQL permission and user without granting outsiders or disabled subjects', async () => {
    const find = vi.fn(async ({ deep = {} }: any) => ({
      data: [
        {
          id: 1,
          isEnabled: true,
          table: { id: 1, name: 'posts' },
          publicOperations: [],
          permissions: take(
            Array.from({ length: 12 }, (_, index) => ({
              id: index + 1,
              isEnabled: index === 11,
              role: null,
              allowedUsers: take(
                index === 11 ? users : [{ id: 'disabled-user' }],
                deep.permissions?.deep?.allowedUsers,
              ),
              operations: take(
                [{ name: 'UPDATE' }],
                deep.permissions?.deep?.operations,
              ),
            })),
            deep.permissions,
          ),
        },
      ],
    }));
    const cache = new GqlDefinitionCacheService({
      queryBuilderService: { find } as any,
      eventEmitter: new EventEmitter2(),
    });
    for (let reload = 0; reload < 2; reload++) {
      await cache.reload(false);
      const definition = (await cache.getAllEnabledFromCache())[0];
      expect(definition.permissions).toHaveLength(12);
      expect(
        hasGraphqlOperationAccess({
          definition,
          operation: 'UPDATE',
          user: { id: 'user-11' },
        }).allowed,
      ).toBe(true);
      for (const user of [{ id: 'outsider' }, { id: 'disabled-user' }, null]) {
        expect(
          hasGraphqlOperationAccess({ definition, operation: 'UPDATE', user })
            .allowed,
        ).toBe(false);
      }
      expect(
        hasGraphqlOperationAccess({
          definition,
          operation: 'DELETE',
          user: { id: 'user-11' },
        }).allowed,
      ).toBe(false);
    }
  });
});
