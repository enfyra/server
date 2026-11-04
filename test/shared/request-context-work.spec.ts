import { afterEach, describe, expect, it, vi } from 'vitest';
import { DynamicContextFactory } from '../../src/shared/services/dynamic-context.factory';
import { routeDetectMiddleware } from '../../src/http/middlewares/route-detect.middleware';
import { PolicyService } from '../../src/domain/policy/policy.service';

const originalEnv = process.env;
afterEach(() => {
  process.env = originalEnv;
});

function factory() {
  return new DynamicContextFactory({
    bcryptService: {},
    apiTokenService: {},
    userCacheService: {},
    envService: {},
    databaseConfigService: {},
    knexService: {},
    mongoService: {},
    websocketContextFactory: { createGlobalProxy: () => ({}) },
  } as unknown as ConstructorParameters<typeof DynamicContextFactory>[0]);
}

describe('request context preparation work', () => {
  it('skips denied env values before reading them and returns independent current snapshots', () => {
    const denied = vi.fn(() => 'synthetic-denied');
    const source = { FIXTURE: 'first' };
    for (const name of [
      'DB_URI',
      'DB_REPLICA_URIS',
      'REDIS_URI',
      'REDIS_PASSWORD',
      'SECRET_KEY',
      'ADMIN_PASSWORD',
    ]) {
      Object.defineProperty(source, name, {
        get: denied,
        enumerable: true,
        configurable: true,
      });
    }
    process.env = source;
    const service = factory();
    const first = service.createBase().$env!;
    expect(first).toEqual({ FIXTURE: 'first' });
    expect(denied).not.toHaveBeenCalled();
    source.FIXTURE = 'second';
    const second = service.createBase().$env!;
    first.FIXTURE = 'mutated';
    expect(second).toEqual({ FIXTURE: 'second' });
    expect(source.FIXTURE).toBe('second');
  });

  it.each(['GET', 'POST', 'PATCH', 'DELETE'])(
    'keeps %s permission and hook ownership within one unified route',
    async (method) => {
      const configs = ['GET', 'POST', 'PATCH', 'DELETE'].map((name, index) => ({
        available: true,
        method: { name },
        isPublic: false,
        routePermissions: [
          { isEnabled: true, allowedUsers: [{ id: `reader-${index}` }] },
        ],
        handler: { id: index, logic: 'return 42;' },
        preHooks: [{ id: index + 10 }],
        postHooks: [{ id: index + 20 }],
      }));
      const route = { path: '/items', methodConfigs: configs };
      const middleware = routeDetectMiddleware(
        { requireRoutes: () => [route] } as never,
        { createReposProxy: () => ({}) } as never,
        { createStorageHelper: () => ({}) } as never,
        {} as never,
        {} as never,
        factory(),
      );
      const req = {
        method,
        path: '/items',
        url: '/items',
        headers: {},
        params: {},
        query: {},
      } as Record<string, any>;
      await middleware(req, {} as never, vi.fn());
      const index = ['GET', 'POST', 'PATCH', 'DELETE'].indexOf(method);
      expect(req.routeData.routeMethodConfig).toBe(configs[index]);
      expect(req.routeData.preHooks).toBe(configs[index]!.preHooks);
      expect(req.routeData.postHooks).toBe(configs[index]!.postHooks);
      const policy = new PolicyService(
        {} as ConstructorParameters<typeof PolicyService>[0],
      );
      expect(
        policy.checkRequestAccess({
          method,
          routeData: req.routeData,
          user: { id: `reader-${index}`, roles: [] },
        }).allow,
      ).toBe(true);
      expect(
        policy.checkRequestAccess({
          method,
          routeData: req.routeData,
          user: { id: `reader-${(index + 1) % 4}`, roles: [] },
        }).allow,
      ).toBe(false);
    },
  );

  it('copies route data only once while retaining exact available method ownership', async () => {
    const ownKeys = vi.fn(Reflect.ownKeys);
    const config = {
      available: true,
      method: { name: 'PATCH' },
      routePermissions: [{ id: 4 }],
      handler: { id: 5, logic: 'return 42;' },
      preHooks: [{ id: 6 }],
      postHooks: [{ id: 7 }],
    };
    const route = new Proxy(
      { path: '/items', methodConfigs: [config] },
      { ownKeys },
    );
    const middleware = routeDetectMiddleware(
      { requireRoutes: () => [route] } as never,
      { createReposProxy: () => ({}) } as never,
      { createStorageHelper: () => ({}) } as never,
      {} as never,
      {} as never,
      factory(),
    );
    const req = {
      method: 'PATCH',
      path: '/items/42',
      url: '/items/42',
      headers: {},
      body: {},
      params: {},
      query: {},
    } as Record<string, any>;
    const next = vi.fn();
    await middleware(req, {} as never, next);
    expect(next).toHaveBeenCalledExactlyOnceWith();
    expect(req.routeData.routeMethodConfig).toBe(config);
    expect(req.routeData.routePermissions).toBe(config.routePermissions);
    expect(req.routeData.handlerRecord).toBe(config.handler);
    expect(req.routeData.preHooks).toBe(config.preHooks);
    expect(req.routeData.postHooks).toBe(config.postHooks);
    expect(req.routeData.params).toEqual({ id: '42' });
    expect(ownKeys).toHaveBeenCalledTimes(1);
  });
});
