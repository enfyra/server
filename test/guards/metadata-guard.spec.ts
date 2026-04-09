import { ExecutionContext, HttpException } from '@nestjs/common';
import {
  PreAuthMetadataGuard,
  PostAuthMetadataGuard,
} from '../../src/shared/guards/metadata-guard.guard';
import type {
  GuardNode,
  GuardRuleNode,
  GuardCache,
} from '../../src/infrastructure/cache/services/guard-cache.service';

class MockRateLimitService {
  results = new Map<
    string,
    {
      allowed: boolean;
      remaining: number;
      resetAt: number;
      retryAfter: number;
      limit: number;
      window: number;
    }
  >();
  calledKeys: string[] = [];

  setResult(key: string, allowed: boolean) {
    this.results.set(key, {
      allowed,
      remaining: 0,
      resetAt: Date.now() + 60000,
      retryAfter: allowed ? 0 : 30,
      limit: 100,
      window: 60,
    });
  }

  async check(key: string, options: any) {
    this.calledKeys.push(key);
    const result = this.results.get(key);
    if (result) return result;
    return {
      allowed: true,
      remaining: options.maxRequests - 1,
      resetAt: Date.now() + options.perSeconds * 1000,
      retryAfter: 0,
      limit: options.maxRequests,
      window: options.perSeconds,
    };
  }
}

function makeGuard(overrides: Partial<GuardNode> = {}): GuardNode {
  return {
    id: 1,
    name: 'test-guard',
    position: 'pre_auth',
    combinator: 'and',
    priority: 0,
    isEnabled: true,
    isGlobal: false,
    parentId: null,
    routeId: null,
    routePath: null,
    methodIds: [],
    methods: [],
    children: [],
    rules: [],
    ...overrides,
  };
}

function makeRule(overrides: Partial<GuardRuleNode> = {}): GuardRuleNode {
  return {
    id: 1,
    type: 'rate_limit_by_ip',
    config: { maxRequests: 100, perSeconds: 60 },
    priority: 0,
    isEnabled: true,
    userIds: [],
    ...overrides,
  };
}

function createMockCache(
  preAuthGlobal: GuardNode[] = [],
  postAuthGlobal: GuardNode[] = [],
  preAuthByRoute = new Map<string, GuardNode[]>(),
  postAuthByRoute = new Map<string, GuardNode[]>(),
) {
  const cache: GuardCache = {
    preAuthGlobal,
    postAuthGlobal,
    preAuthByRoute,
    postAuthByRoute,
  };
  return {
    ensureGuardsLoaded: jest.fn().mockResolvedValue(undefined),
    getGuardsForRoute: jest.fn(
      (position: string, routePath: string, method: string) => {
        const globalGuards =
          position === 'pre_auth' ? cache.preAuthGlobal : cache.postAuthGlobal;
        const routeMap =
          position === 'pre_auth'
            ? cache.preAuthByRoute
            : cache.postAuthByRoute;
        const routeGuards = routeMap.get(routePath) || [];
        return [...globalGuards, ...routeGuards].filter(
          (g) => g.methods.length === 0 || g.methods.includes(method),
        );
      },
    ),
  };
}

function createMockContext(
  overrides: {
    routePath?: string;
    method?: string;
    ip?: string;
    userId?: string | null;
  } = {},
): ExecutionContext {
  const headers: Record<string, string> = {};
  const res = {
    setHeader: jest.fn((k: string, v: string) => {
      headers[k] = v;
    }),
  };
  const req = {
    method: overrides.method || 'GET',
    ip: overrides.ip || '1.2.3.4',
    baseUrl: overrides.routePath || '/test',
    user: overrides.userId ? { id: overrides.userId } : undefined,
    routeData: {
      route: { path: overrides.routePath || '/test' },
      context: { $req: { ip: overrides.ip || '1.2.3.4' } },
    },
  };
  return {
    switchToHttp: () => ({ getRequest: () => req, getResponse: () => res }),
  } as any;
}

describe('MetadataGuard — pre/post auth integration', () => {
  let rateLimitService: MockRateLimitService;

  beforeEach(() => {
    rateLimitService = new MockRateLimitService();
  });

  describe('PreAuthMetadataGuard', () => {
    it('should pass when no guards configured', async () => {
      const cache = createMockCache();
      const { GuardEvaluatorService } =
        await import('../../src/infrastructure/cache/services/guard-evaluator.service');
      const evaluator = new GuardEvaluatorService(rateLimitService as any);
      const guard = new PreAuthMetadataGuard(cache as any, evaluator);

      const result = await guard.canActivate(createMockContext());
      expect(result).toBe(true);
    });

    it('should block by IP blacklist before authentication', async () => {
      const blacklistGuard = makeGuard({
        position: 'pre_auth',
        isGlobal: true,
        rules: [
          makeRule({ type: 'ip_blacklist', config: { ips: ['1.2.3.4'] } }),
        ],
      });
      const cache = createMockCache([blacklistGuard]);
      const { GuardEvaluatorService } =
        await import('../../src/infrastructure/cache/services/guard-evaluator.service');
      const evaluator = new GuardEvaluatorService(rateLimitService as any);
      const guard = new PreAuthMetadataGuard(cache as any, evaluator);

      await expect(
        guard.canActivate(createMockContext({ ip: '1.2.3.4' })),
      ).rejects.toThrow(HttpException);
    });

    it('should NOT have userId in pre_auth context', async () => {
      const rateLimitGuard = makeGuard({
        position: 'pre_auth',
        isGlobal: true,
        rules: [
          makeRule({
            type: 'rate_limit_by_ip',
            config: { maxRequests: 100, perSeconds: 60 },
          }),
        ],
      });
      const cache = createMockCache([rateLimitGuard]);
      const { GuardEvaluatorService } =
        await import('../../src/infrastructure/cache/services/guard-evaluator.service');
      const evaluator = new GuardEvaluatorService(rateLimitService as any);
      const spy = jest.spyOn(evaluator, 'evaluateGuard');
      const guard = new PreAuthMetadataGuard(cache as any, evaluator);

      await guard.canActivate(createMockContext({ userId: 'user-123' }));
      expect(spy.mock.calls[0][1].userId).toBeNull();
    });

    it('should set response headers on rate limit rejection', async () => {
      rateLimitService.setResult('ip:1.2.3.4:/test', false);
      const rateLimitGuard = makeGuard({
        position: 'pre_auth',
        isGlobal: true,
        rules: [
          makeRule({
            type: 'rate_limit_by_ip',
            config: { maxRequests: 100, perSeconds: 60 },
          }),
        ],
      });
      const cache = createMockCache([rateLimitGuard]);
      const { GuardEvaluatorService } =
        await import('../../src/infrastructure/cache/services/guard-evaluator.service');
      const evaluator = new GuardEvaluatorService(rateLimitService as any);
      const guard = new PreAuthMetadataGuard(cache as any, evaluator);

      const ctx = createMockContext({ ip: '1.2.3.4' });
      try {
        await guard.canActivate(ctx);
        fail('Should have thrown');
      } catch (e: any) {
        expect(e.getStatus()).toBe(429);
        const res = ctx.switchToHttp().getResponse();
        expect(res.setHeader).toHaveBeenCalledWith(
          'Retry-After',
          expect.any(String),
        );
      }
    });
  });

  describe('PostAuthMetadataGuard', () => {
    it('should pass userId from req.user in post_auth', async () => {
      const rateLimitGuard = makeGuard({
        position: 'post_auth',
        isGlobal: true,
        rules: [
          makeRule({
            type: 'rate_limit_by_user',
            config: { maxRequests: 10, perSeconds: 60 },
          }),
        ],
      });
      const cache = createMockCache([], [rateLimitGuard]);
      const { GuardEvaluatorService } =
        await import('../../src/infrastructure/cache/services/guard-evaluator.service');
      const evaluator = new GuardEvaluatorService(rateLimitService as any);
      const spy = jest.spyOn(evaluator, 'evaluateGuard');
      const guard = new PostAuthMetadataGuard(cache as any, evaluator);

      await guard.canActivate(createMockContext({ userId: 'user-42' }));
      expect(spy.mock.calls[0][1].userId).toBe('user-42');
      expect(rateLimitService.calledKeys).toContain('user:user-42:/test');
    });

    it('should block user-scoped rate limit in post_auth', async () => {
      rateLimitService.setResult('user:user-42:/test', false);
      const rateLimitGuard = makeGuard({
        position: 'post_auth',
        isGlobal: true,
        rules: [
          makeRule({
            type: 'rate_limit_by_user',
            config: { maxRequests: 10, perSeconds: 60 },
            userIds: ['user-42'],
          }),
        ],
      });
      const cache = createMockCache([], [rateLimitGuard]);
      const { GuardEvaluatorService } =
        await import('../../src/infrastructure/cache/services/guard-evaluator.service');
      const evaluator = new GuardEvaluatorService(rateLimitService as any);
      const guard = new PostAuthMetadataGuard(cache as any, evaluator);

      await expect(
        guard.canActivate(createMockContext({ userId: 'user-42' })),
      ).rejects.toThrow(HttpException);
    });

    it('should skip user-scoped rule for different user', async () => {
      rateLimitService.setResult('user:user-42:/test', false);
      const rateLimitGuard = makeGuard({
        position: 'post_auth',
        isGlobal: true,
        rules: [
          makeRule({
            type: 'rate_limit_by_user',
            config: { maxRequests: 10, perSeconds: 60 },
            userIds: ['user-42'],
          }),
        ],
      });
      const cache = createMockCache([], [rateLimitGuard]);
      const { GuardEvaluatorService } =
        await import('../../src/infrastructure/cache/services/guard-evaluator.service');
      const evaluator = new GuardEvaluatorService(rateLimitService as any);
      const guard = new PostAuthMetadataGuard(cache as any, evaluator);

      const result = await guard.canActivate(
        createMockContext({ userId: 'user-99' }),
      );
      expect(result).toBe(true);
    });
  });

  describe('multiple guards on same route', () => {
    it('should evaluate all guards sequentially — first reject blocks', async () => {
      const guard1 = makeGuard({
        id: 1,
        name: 'whitelist',
        position: 'pre_auth',
        isGlobal: false,
        priority: 0,
        rules: [
          makeRule({
            id: 1,
            type: 'ip_whitelist',
            config: { ips: ['10.0.0.0/8'] },
          }),
        ],
      });
      const guard2 = makeGuard({
        id: 2,
        name: 'rate-limit',
        position: 'pre_auth',
        isGlobal: false,
        priority: 1,
        rules: [
          makeRule({
            id: 2,
            type: 'rate_limit_by_ip',
            config: { maxRequests: 100, perSeconds: 60 },
          }),
        ],
      });
      const routeMap = new Map<string, GuardNode[]>();
      routeMap.set('/api/orders', [guard1, guard2]);
      const cache = createMockCache([], [], routeMap);

      const { GuardEvaluatorService } =
        await import('../../src/infrastructure/cache/services/guard-evaluator.service');
      const evaluator = new GuardEvaluatorService(rateLimitService as any);
      const preAuthGuard = new PreAuthMetadataGuard(cache as any, evaluator);

      // IP not in whitelist → guard1 rejects, guard2 never runs
      try {
        await preAuthGuard.canActivate(
          createMockContext({ ip: '192.168.1.1', routePath: '/api/orders' }),
        );
        fail('Should have thrown');
      } catch (e: any) {
        expect(e.getStatus()).toBe(403);
      }
      expect(rateLimitService.calledKeys).toHaveLength(0);
    });

    it('should pass when all guards pass', async () => {
      const guard1 = makeGuard({
        id: 1,
        name: 'whitelist',
        position: 'pre_auth',
        isGlobal: false,
        priority: 0,
        rules: [
          makeRule({
            id: 1,
            type: 'ip_whitelist',
            config: { ips: ['10.0.0.0/8'] },
          }),
        ],
      });
      const guard2 = makeGuard({
        id: 2,
        name: 'rate-limit',
        position: 'pre_auth',
        isGlobal: false,
        priority: 1,
        rules: [
          makeRule({
            id: 2,
            type: 'rate_limit_by_ip',
            config: { maxRequests: 100, perSeconds: 60 },
          }),
        ],
      });
      const routeMap = new Map<string, GuardNode[]>();
      routeMap.set('/api/orders', [guard1, guard2]);
      const cache = createMockCache([], [], routeMap);

      const { GuardEvaluatorService } =
        await import('../../src/infrastructure/cache/services/guard-evaluator.service');
      const evaluator = new GuardEvaluatorService(rateLimitService as any);
      const preAuthGuard = new PreAuthMetadataGuard(cache as any, evaluator);

      // IP in whitelist + rate limit not exceeded → both pass
      const result = await preAuthGuard.canActivate(
        createMockContext({ ip: '10.0.0.1', routePath: '/api/orders' }),
      );
      expect(result).toBe(true);
      expect(rateLimitService.calledKeys).toHaveLength(1);
    });
  });

  describe('pre_auth + post_auth combination', () => {
    it('pre_auth IP blacklist blocks before post_auth runs', async () => {
      const preGuard = makeGuard({
        id: 1,
        name: 'ip-block',
        position: 'pre_auth',
        isGlobal: true,
        rules: [
          makeRule({
            id: 1,
            type: 'ip_blacklist',
            config: { ips: ['1.2.3.4'] },
          }),
        ],
      });
      const postGuard = makeGuard({
        id: 2,
        name: 'user-limit',
        position: 'post_auth',
        isGlobal: true,
        rules: [
          makeRule({
            id: 2,
            type: 'rate_limit_by_user',
            config: { maxRequests: 10, perSeconds: 60 },
          }),
        ],
      });
      const cache = createMockCache([preGuard], [postGuard]);

      const { GuardEvaluatorService } =
        await import('../../src/infrastructure/cache/services/guard-evaluator.service');
      const evaluator = new GuardEvaluatorService(rateLimitService as any);
      const preAuth = new PreAuthMetadataGuard(cache as any, evaluator);

      // pre_auth blocks
      await expect(
        preAuth.canActivate(createMockContext({ ip: '1.2.3.4' })),
      ).rejects.toThrow(HttpException);
      // post_auth would have passed but never reached
      expect(rateLimitService.calledKeys).toHaveLength(0);
    });

    it('pre_auth passes → post_auth rate limits authenticated user', async () => {
      rateLimitService.setResult('user:user-42:/test', false);
      const preGuard = makeGuard({
        id: 1,
        name: 'ip-limit',
        position: 'pre_auth',
        isGlobal: true,
        rules: [
          makeRule({
            id: 1,
            type: 'rate_limit_by_ip',
            config: { maxRequests: 100, perSeconds: 60 },
          }),
        ],
      });
      const postGuard = makeGuard({
        id: 2,
        name: 'user-limit',
        position: 'post_auth',
        isGlobal: true,
        rules: [
          makeRule({
            id: 2,
            type: 'rate_limit_by_user',
            config: { maxRequests: 10, perSeconds: 60 },
          }),
        ],
      });
      const cache = createMockCache([preGuard], [postGuard]);

      const { GuardEvaluatorService } =
        await import('../../src/infrastructure/cache/services/guard-evaluator.service');
      const evaluator = new GuardEvaluatorService(rateLimitService as any);
      const preAuth = new PreAuthMetadataGuard(cache as any, evaluator);
      const postAuth = new PostAuthMetadataGuard(cache as any, evaluator);

      const ctx = createMockContext({ ip: '1.2.3.4', userId: 'user-42' });
      // pre_auth passes (IP rate limit ok)
      const preResult = await preAuth.canActivate(ctx);
      expect(preResult).toBe(true);

      // post_auth blocks (user rate limit exceeded)
      await expect(postAuth.canActivate(ctx)).rejects.toThrow(HttpException);
    });

    it('global pre_auth + route-specific post_auth', async () => {
      const globalPreGuard = makeGuard({
        id: 1,
        name: 'global-ip-limit',
        position: 'pre_auth',
        isGlobal: true,
        rules: [
          makeRule({
            id: 1,
            type: 'rate_limit_by_ip',
            config: { maxRequests: 1000, perSeconds: 60 },
          }),
        ],
      });
      rateLimitService.setResult('user:admin:/api/admin', false);
      const routePostGuard = makeGuard({
        id: 2,
        name: 'admin-user-limit',
        position: 'post_auth',
        isGlobal: false,
        routePath: '/api/admin',
        rules: [
          makeRule({
            id: 2,
            type: 'rate_limit_by_user',
            config: { maxRequests: 5, perSeconds: 60 },
          }),
        ],
      });
      const postAuthByRoute = new Map<string, GuardNode[]>();
      postAuthByRoute.set('/api/admin', [routePostGuard]);
      const cache = createMockCache(
        [globalPreGuard],
        [],
        new Map(),
        postAuthByRoute,
      );

      const { GuardEvaluatorService } =
        await import('../../src/infrastructure/cache/services/guard-evaluator.service');
      const evaluator = new GuardEvaluatorService(rateLimitService as any);
      const preAuth = new PreAuthMetadataGuard(cache as any, evaluator);
      const postAuth = new PostAuthMetadataGuard(cache as any, evaluator);

      const ctx = createMockContext({
        ip: '10.0.0.1',
        routePath: '/api/admin',
        userId: 'admin',
      });
      // pre_auth: global IP rate limit passes
      expect(await preAuth.canActivate(ctx)).toBe(true);
      // post_auth: route-specific user rate limit exceeds
      await expect(postAuth.canActivate(ctx)).rejects.toThrow(HttpException);
    });
  });

  describe('no routeData', () => {
    it('should pass when req has no routeData', async () => {
      const cache = createMockCache([
        makeGuard({
          isGlobal: true,
          rules: [
            makeRule({ type: 'ip_blacklist', config: { ips: ['1.2.3.4'] } }),
          ],
        }),
      ]);
      const { GuardEvaluatorService } =
        await import('../../src/infrastructure/cache/services/guard-evaluator.service');
      const evaluator = new GuardEvaluatorService(rateLimitService as any);
      const guard = new PreAuthMetadataGuard(cache as any, evaluator);

      const ctx = {
        switchToHttp: () => ({
          getRequest: () => ({ method: 'GET', ip: '1.2.3.4' }),
          getResponse: () => ({ setHeader: jest.fn() }),
        }),
      } as any;
      const result = await guard.canActivate(ctx);
      expect(result).toBe(true);
    });
  });
});
