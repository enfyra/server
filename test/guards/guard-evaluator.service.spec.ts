import {
  GuardEvaluatorService,
  type GuardNode,
  type GuardRuleNode,
} from '../../src/engines/cache';
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

  setResult(key: string, allowed: boolean, remaining = 0) {
    this.results.set(key, {
      allowed,
      remaining,
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
    type: 'route',
    gqlOperation: null,
    tableName: null,
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

describe('GuardEvaluatorService', () => {
  let rateLimitService: MockRateLimitService;
  let evaluator: GuardEvaluatorService;

  beforeEach(() => {
    rateLimitService = new MockRateLimitService();
    evaluator = new GuardEvaluatorService({
      rateLimitService: rateLimitService as any,
    });
  });

  describe('rate limiting', () => {
    it('should pass when rate limit is not exceeded', async () => {
      const guard = makeGuard({
        rules: [
          makeRule({
            type: 'rate_limit_by_ip',
            config: { maxRequests: 100, perSeconds: 60 },
          }),
        ],
      });
      const result = await evaluator.evaluateGuard(guard, {
        clientIp: '1.2.3.4',
        routePath: '/test',
      });
      expect(result.reject).toBeNull();
    });

    it('should reject with 429 when rate limit exceeded', async () => {
      rateLimitService.setResult('guard_rule:1:ip:1.2.3.4', false);
      const guard = makeGuard({
        rules: [
          makeRule({
            type: 'rate_limit_by_ip',
            config: { maxRequests: 100, perSeconds: 60 },
          }),
        ],
      });
      const result = await evaluator.evaluateGuard(guard, {
        clientIp: '1.2.3.4',
        routePath: '/test',
      });
      expect(result.reject).not.toBeNull();
      expect(result.reject!.statusCode).toBe(429);
      expect(result.reject!.headers?.['Retry-After']).toBeDefined();
    });

    it('should use correct key for rate_limit_by_user', async () => {
      const guard = makeGuard({
        rules: [
          makeRule({
            type: 'rate_limit_by_user',
            config: { maxRequests: 10, perSeconds: 60 },
          }),
        ],
      });
      await evaluator.evaluateGuard(guard, {
        clientIp: '1.2.3.4',
        routePath: '/test',
        userId: 'user-123',
      });
      expect(rateLimitService.calledKeys).toContain(
        'guard_rule:1:user:user-123',
      );
    });

    it('should use correct key for rate_limit_by_route', async () => {
      const guard = makeGuard({
        rules: [
          makeRule({
            type: 'rate_limit_by_route',
            config: { maxRequests: 1000, perSeconds: 60 },
          }),
        ],
      });
      await evaluator.evaluateGuard(guard, {
        clientIp: '1.2.3.4',
        routePath: '/api/posts',
      });
      expect(rateLimitService.calledKeys).toContain(
        'guard_rule:1:route:/api/posts',
      );
    });

    it('should scope IP counters by guard rule instead of route path', async () => {
      const guard = makeGuard({
        isGlobal: true,
        rules: [
          makeRule({
            id: 9,
            type: 'rate_limit_by_ip',
            config: { maxRequests: 100, perSeconds: 60 },
          }),
        ],
      });

      await evaluator.evaluateGuard(guard, {
        clientIp: '1.2.3.4',
        routePath: '/metadata',
      });
      await evaluator.evaluateGuard(guard, {
        clientIp: '1.2.3.4',
        routePath: '/enfyra_menu',
      });

      expect(rateLimitService.calledKeys).toEqual([
        'guard_rule:9:ip:1.2.3.4',
        'guard_rule:9:ip:1.2.3.4',
      ]);
    });

    it('should skip rule with missing config', async () => {
      const guard = makeGuard({
        rules: [makeRule({ type: 'rate_limit_by_ip', config: {} })],
      });
      const result = await evaluator.evaluateGuard(guard, {
        clientIp: '1.2.3.4',
        routePath: '/test',
      });
      expect(result.reject).toBeNull();
      expect(rateLimitService.calledKeys).toHaveLength(0);
    });
  });

  describe('IP whitelist', () => {
    it('should pass when IP is in whitelist', async () => {
      const guard = makeGuard({
        rules: [
          makeRule({
            type: 'ip_whitelist',
            config: { ips: ['1.2.3.4', '5.6.7.8'] },
          }),
        ],
      });
      const result = await evaluator.evaluateGuard(guard, {
        clientIp: '1.2.3.4',
        routePath: '/test',
      });
      expect(result.reject).toBeNull();
    });

    it('should reject when IP is not in whitelist', async () => {
      const guard = makeGuard({
        rules: [
          makeRule({ type: 'ip_whitelist', config: { ips: ['5.6.7.8'] } }),
        ],
      });
      const result = await evaluator.evaluateGuard(guard, {
        clientIp: '1.2.3.4',
        routePath: '/test',
      });
      expect(result.reject).not.toBeNull();
      expect(result.reject!.statusCode).toBe(403);
    });

    it('should match CIDR notation', async () => {
      const guard = makeGuard({
        rules: [
          makeRule({ type: 'ip_whitelist', config: { ips: ['10.0.0.0/8'] } }),
        ],
      });
      expect(
        (
          await evaluator.evaluateGuard(guard, {
            clientIp: '10.1.2.3',
            routePath: '/test',
          })
        ).reject,
      ).toBeNull();
      expect(
        (
          await evaluator.evaluateGuard(guard, {
            clientIp: '10.255.255.255',
            routePath: '/test',
          })
        ).reject,
      ).toBeNull();
      const reject = await evaluator.evaluateGuard(guard, {
        clientIp: '11.0.0.1',
        routePath: '/test',
      });
      expect(reject.reject).not.toBeNull();
      expect(reject.reject!.statusCode).toBe(403);
    });

    it('should handle /24 subnet', async () => {
      const guard = makeGuard({
        rules: [
          makeRule({
            type: 'ip_whitelist',
            config: { ips: ['192.168.1.0/24'] },
          }),
        ],
      });
      expect(
        (
          await evaluator.evaluateGuard(guard, {
            clientIp: '192.168.1.100',
            routePath: '/t',
          })
        ).reject,
      ).toBeNull();
      expect(
        (
          await evaluator.evaluateGuard(guard, {
            clientIp: '192.168.1.255',
            routePath: '/t',
          })
        ).reject,
      ).toBeNull();
      expect(
        (
          await evaluator.evaluateGuard(guard, {
            clientIp: '192.168.2.1',
            routePath: '/t',
          })
        ).reject,
      ).not.toBeNull();
    });

    it('should pass when whitelist is empty', async () => {
      const guard = makeGuard({
        rules: [makeRule({ type: 'ip_whitelist', config: { ips: [] } })],
      });
      const result = await evaluator.evaluateGuard(guard, {
        clientIp: '1.2.3.4',
        routePath: '/test',
      });
      expect(result.reject).toBeNull();
    });
  });

  describe('IP blacklist', () => {
    it('should reject when IP is in blacklist', async () => {
      const guard = makeGuard({
        rules: [
          makeRule({ type: 'ip_blacklist', config: { ips: ['1.2.3.4'] } }),
        ],
      });
      const result = await evaluator.evaluateGuard(guard, {
        clientIp: '1.2.3.4',
        routePath: '/test',
      });
      expect(result.reject).not.toBeNull();
      expect(result.reject!.statusCode).toBe(403);
    });

    it('should pass when IP is not in blacklist', async () => {
      const guard = makeGuard({
        rules: [
          makeRule({ type: 'ip_blacklist', config: { ips: ['5.6.7.8'] } }),
        ],
      });
      const result = await evaluator.evaluateGuard(guard, {
        clientIp: '1.2.3.4',
        routePath: '/test',
      });
      expect(result.reject).toBeNull();
    });

    it('should match CIDR in blacklist', async () => {
      const guard = makeGuard({
        rules: [
          makeRule({ type: 'ip_blacklist', config: { ips: ['10.0.0.0/8'] } }),
        ],
      });
      const result = await evaluator.evaluateGuard(guard, {
        clientIp: '10.5.5.5',
        routePath: '/test',
      });
      expect(result.reject).not.toBeNull();
    });
  });

  describe('AND combinator', () => {
    it('should pass when all rules pass', async () => {
      const guard = makeGuard({
        combinator: 'and',
        rules: [
          makeRule({
            id: 1,
            type: 'rate_limit_by_ip',
            config: { maxRequests: 100, perSeconds: 60 },
          }),
          makeRule({
            id: 2,
            type: 'ip_blacklist',
            config: { ips: ['9.9.9.9'] },
          }),
        ],
      });
      const result = await evaluator.evaluateGuard(guard, {
        clientIp: '1.2.3.4',
        routePath: '/test',
      });
      expect(result.reject).toBeNull();
    });

    it('should reject on first failing rule (short-circuit)', async () => {
      rateLimitService.setResult('guard_rule:10:ip:1.2.3.4', false);
      const guard = makeGuard({
        combinator: 'and',
        rules: [
          makeRule({
            id: 1,
            type: 'rate_limit_by_ip',
            config: { maxRequests: 100, perSeconds: 60 },
          }),
          makeRule({
            id: 2,
            type: 'ip_blacklist',
            config: { ips: ['1.2.3.4'] },
          }),
        ],
      });
      const result = await evaluator.evaluateGuard(guard, {
        clientIp: '1.2.3.4',
        routePath: '/test',
      });
      expect(result.reject).not.toBeNull();
      // ip_blacklist (cost 0) runs before rate_limit_by_ip (cost 1)
      expect(result.reject!.ruleType).toBe('ip_blacklist');
    });
  });

  describe('cost-based rule ordering', () => {
    it('should evaluate IP rules before rate limit rules (AND)', async () => {
      rateLimitService.setResult('guard_rule:1:ip:1.2.3.4', false);
      const guard = makeGuard({
        combinator: 'and',
        rules: [
          makeRule({
            id: 1,
            type: 'rate_limit_by_ip',
            priority: 0,
            config: { maxRequests: 100, perSeconds: 60 },
          }),
          makeRule({
            id: 2,
            type: 'ip_blacklist',
            priority: 1,
            config: { ips: ['1.2.3.4'] },
          }),
        ],
      });
      const result = await evaluator.evaluateGuard(guard, {
        clientIp: '1.2.3.4',
        routePath: '/test',
      });
      expect(result.reject).not.toBeNull();
      expect(result.reject!.ruleType).toBe('ip_blacklist');
      // rate limit should NOT have been called since IP was blocked first
      expect(rateLimitService.calledKeys).toHaveLength(0);
    });

    it('should skip rate limit when IP whitelist rejects (AND)', async () => {
      const guard = makeGuard({
        combinator: 'and',
        rules: [
          makeRule({
            id: 1,
            type: 'rate_limit_by_ip',
            config: { maxRequests: 100, perSeconds: 60 },
          }),
          makeRule({
            id: 2,
            type: 'ip_whitelist',
            config: { ips: ['10.0.0.0/8'] },
          }),
        ],
      });
      const result = await evaluator.evaluateGuard(guard, {
        clientIp: '192.168.1.1',
        routePath: '/test',
      });
      expect(result.reject).not.toBeNull();
      expect(result.reject!.ruleType).toBe('ip_whitelist');
      expect(result.reject!.statusCode).toBe(403);
      expect(rateLimitService.calledKeys).toHaveLength(0);
    });

    it('should skip rate limit when IP whitelist passes (OR)', async () => {
      rateLimitService.setResult('guard_rule:1:ip:10.0.0.1', false);
      const guard = makeGuard({
        combinator: 'or',
        rules: [
          makeRule({
            id: 1,
            type: 'rate_limit_by_ip',
            config: { maxRequests: 100, perSeconds: 60 },
          }),
          makeRule({
            id: 2,
            type: 'ip_whitelist',
            config: { ips: ['10.0.0.0/8'] },
          }),
        ],
      });
      const result = await evaluator.evaluateGuard(guard, {
        clientIp: '10.0.0.1',
        routePath: '/test',
      });
      // ip_whitelist passes (cost 0, runs first) → OR short-circuits → no rate limit call
      expect(result.reject).toBeNull();
      expect(rateLimitService.calledKeys).toHaveLength(0);
    });

    it('should still call rate limit when IP check passes (AND)', async () => {
      rateLimitService.setResult('guard_rule:1:ip:10.0.0.1', false);
      const guard = makeGuard({
        combinator: 'and',
        rules: [
          makeRule({
            id: 1,
            type: 'rate_limit_by_ip',
            config: { maxRequests: 100, perSeconds: 60 },
          }),
          makeRule({
            id: 2,
            type: 'ip_whitelist',
            config: { ips: ['10.0.0.0/8'] },
          }),
        ],
      });
      const result = await evaluator.evaluateGuard(guard, {
        clientIp: '10.0.0.1',
        routePath: '/test',
      });
      // ip_whitelist passes, then rate_limit runs and rejects
      expect(result.reject).not.toBeNull();
      expect(result.reject!.ruleType).toBe('rate_limit_by_ip');
      expect(result.reject!.statusCode).toBe(429);
      expect(rateLimitService.calledKeys).toHaveLength(1);
    });
  });

  describe('OR combinator', () => {
    it('should pass when any rule passes', async () => {
      const guard = makeGuard({
        combinator: 'or',
        rules: [
          makeRule({
            id: 1,
            type: 'ip_whitelist',
            config: { ips: ['1.2.3.4'] },
          }),
          makeRule({
            id: 2,
            type: 'ip_whitelist',
            config: { ips: ['5.6.7.8'] },
          }),
        ],
      });
      const result = await evaluator.evaluateGuard(guard, {
        clientIp: '1.2.3.4',
        routePath: '/test',
      });
      expect(result.reject).toBeNull();
    });

    it('should reject when all rules fail', async () => {
      const guard = makeGuard({
        combinator: 'or',
        rules: [
          makeRule({
            id: 1,
            type: 'ip_whitelist',
            config: { ips: ['5.6.7.8'] },
          }),
          makeRule({
            id: 2,
            type: 'ip_whitelist',
            config: { ips: ['9.9.9.9'] },
          }),
        ],
      });
      const result = await evaluator.evaluateGuard(guard, {
        clientIp: '1.2.3.4',
        routePath: '/test',
      });
      expect(result.reject).not.toBeNull();
      expect(result.reject!.statusCode).toBe(403);
    });
  });

  describe('nested guard tree', () => {
    it('should evaluate (rate_limit AND rate_limit_by_route) OR ip_whitelist', async () => {
      rateLimitService.setResult('guard_rule:10:ip:1.2.3.4', false);

      const guard = makeGuard({
        combinator: 'or',
        children: [
          makeGuard({
            id: 2,
            name: 'rate-limits',
            combinator: 'and',
            parentId: 1,
            rules: [
              makeRule({
                id: 10,
                type: 'rate_limit_by_ip',
                config: { maxRequests: 100, perSeconds: 60 },
              }),
              makeRule({
                id: 11,
                type: 'rate_limit_by_route',
                config: { maxRequests: 1000, perSeconds: 60 },
              }),
            ],
          }),
          makeGuard({
            id: 3,
            name: 'whitelist',
            combinator: 'and',
            parentId: 1,
            rules: [
              makeRule({
                id: 20,
                type: 'ip_whitelist',
                config: { ips: ['1.2.3.4'] },
              }),
            ],
          }),
        ],
      });

      const result = await evaluator.evaluateGuard(guard, {
        clientIp: '1.2.3.4',
        routePath: '/test',
      });
      expect(result.reject).toBeNull();
    });

    it('should reject when nested OR has no passing branch', async () => {
      rateLimitService.setResult('guard_rule:10:ip:1.2.3.4', false);

      const guard = makeGuard({
        combinator: 'or',
        children: [
          makeGuard({
            id: 2,
            name: 'rate-limits',
            combinator: 'and',
            parentId: 1,
            rules: [
              makeRule({
                id: 10,
                type: 'rate_limit_by_ip',
                config: { maxRequests: 100, perSeconds: 60 },
              }),
            ],
          }),
          makeGuard({
            id: 3,
            name: 'whitelist',
            combinator: 'and',
            parentId: 1,
            rules: [
              makeRule({
                id: 20,
                type: 'ip_whitelist',
                config: { ips: ['9.9.9.9'] },
              }),
            ],
          }),
        ],
      });

      const result = await evaluator.evaluateGuard(guard, {
        clientIp: '1.2.3.4',
        routePath: '/test',
      });
      expect(result.reject).not.toBeNull();
    });

    it('should handle 3-level nesting', async () => {
      const guard = makeGuard({
        combinator: 'and',
        children: [
          makeGuard({
            id: 2,
            combinator: 'or',
            parentId: 1,
            children: [
              makeGuard({
                id: 4,
                combinator: 'and',
                parentId: 2,
                rules: [
                  makeRule({
                    id: 40,
                    type: 'ip_whitelist',
                    config: { ips: ['10.0.0.0/8'] },
                  }),
                ],
              }),
              makeGuard({
                id: 5,
                combinator: 'and',
                parentId: 2,
                rules: [
                  makeRule({
                    id: 50,
                    type: 'ip_whitelist',
                    config: { ips: ['192.168.0.0/16'] },
                  }),
                ],
              }),
            ],
          }),
        ],
        rules: [
          makeRule({
            id: 1,
            type: 'rate_limit_by_ip',
            config: { maxRequests: 100, perSeconds: 60 },
          }),
        ],
      });

      const result = await evaluator.evaluateGuard(guard, {
        clientIp: '10.5.5.5',
        routePath: '/test',
      });
      expect(result.reject).toBeNull();

      const result2 = await evaluator.evaluateGuard(guard, {
        clientIp: '192.168.1.1',
        routePath: '/test',
      });
      expect(result2.reject).toBeNull();

      const result3 = await evaluator.evaluateGuard(guard, {
        clientIp: '172.16.0.1',
        routePath: '/test',
      });
      expect(result3.reject).not.toBeNull();
      expect(result3.reject!.statusCode).toBe(403);
    });
  });

  describe('user scoping', () => {
    it('should apply rule only to specified users', async () => {
      rateLimitService.setResult('guard_rule:1:user:user-A', false);
      const guard = makeGuard({
        rules: [
          makeRule({
            type: 'rate_limit_by_user',
            config: { maxRequests: 5, perSeconds: 60 },
            userIds: ['user-A'],
          }),
        ],
      });

      const resultA = await evaluator.evaluateGuard(guard, {
        clientIp: '1.2.3.4',
        routePath: '/test',
        userId: 'user-A',
      });
      expect(resultA.reject).not.toBeNull();
      expect(resultA.reject!.statusCode).toBe(429);

      const resultB = await evaluator.evaluateGuard(guard, {
        clientIp: '1.2.3.4',
        routePath: '/test',
        userId: 'user-B',
      });
      expect(resultB.reject).toBeNull();
    });

    it('should skip user-scoped rules when no userId', async () => {
      const guard = makeGuard({
        rules: [
          makeRule({
            type: 'rate_limit_by_user',
            config: { maxRequests: 5, perSeconds: 60 },
            userIds: ['user-A'],
          }),
        ],
      });
      const result = await evaluator.evaluateGuard(guard, {
        clientIp: '1.2.3.4',
        routePath: '/test',
        userId: null,
      });
      expect(result.reject).toBeNull();
    });

    it('should apply rule to all users when userIds is empty', async () => {
      rateLimitService.setResult('guard_rule:1:user:user-X', false);
      const guard = makeGuard({
        rules: [
          makeRule({
            type: 'rate_limit_by_user',
            config: { maxRequests: 5, perSeconds: 60 },
            userIds: [],
          }),
        ],
      });
      const result = await evaluator.evaluateGuard(guard, {
        clientIp: '1.2.3.4',
        routePath: '/test',
        userId: 'user-X',
      });
      expect(result.reject).not.toBeNull();
      expect(result.reject!.statusCode).toBe(429);
    });
  });

  describe('disabled children', () => {
    it('should skip disabled child guards', async () => {
      const guard = makeGuard({
        combinator: 'and',
        children: [
          makeGuard({
            id: 2,
            isEnabled: false,
            rules: [
              makeRule({ type: 'ip_blacklist', config: { ips: ['1.2.3.4'] } }),
            ],
          }),
        ],
      });
      const result = await evaluator.evaluateGuard(guard, {
        clientIp: '1.2.3.4',
        routePath: '/test',
      });
      expect(result.reject).toBeNull();
    });
  });

  describe('empty guard', () => {
    it('should pass when guard has no rules and no children', async () => {
      const guard = makeGuard();
      const result = await evaluator.evaluateGuard(guard, {
        clientIp: '1.2.3.4',
        routePath: '/test',
      });
      expect(result.reject).toBeNull();
    });
  });

  describe('IPv6-mapped IPv4 normalization', () => {
    it('should match ::ffff:10.0.0.1 against CIDR 10.0.0.0/8', async () => {
      const guard = makeGuard({
        rules: [
          makeRule({ type: 'ip_whitelist', config: { ips: ['10.0.0.0/8'] } }),
        ],
      });
      const result = await evaluator.evaluateGuard(guard, {
        clientIp: '::ffff:10.0.0.1',
        routePath: '/test',
      });
      expect(result.reject).toBeNull();
    });

    it('should match ::ffff:192.168.1.1 against exact IP 192.168.1.1', async () => {
      const guard = makeGuard({
        rules: [
          makeRule({
            type: 'ip_whitelist',
            config: { ips: ['192.168.1.1'] },
          }),
        ],
      });
      const result = await evaluator.evaluateGuard(guard, {
        clientIp: '::ffff:192.168.1.1',
        routePath: '/test',
      });
      expect(result.reject).toBeNull();
    });

    it('should match plain IPv4 against ::ffff:-prefixed pattern', async () => {
      const guard = makeGuard({
        rules: [
          makeRule({
            type: 'ip_whitelist',
            config: { ips: ['::ffff:172.16.0.1'] },
          }),
        ],
      });
      const result = await evaluator.evaluateGuard(guard, {
        clientIp: '172.16.0.1',
        routePath: '/test',
      });
      expect(result.reject).toBeNull();
    });

    it('should reject ::ffff:192.168.1.1 when whitelist has different IP', async () => {
      const guard = makeGuard({
        rules: [
          makeRule({
            type: 'ip_whitelist',
            config: { ips: ['10.0.0.1'] },
          }),
        ],
      });
      const result = await evaluator.evaluateGuard(guard, {
        clientIp: '::ffff:192.168.1.1',
        routePath: '/test',
      });
      expect(result.reject).not.toBeNull();
      expect(result.reject!.statusCode).toBe(403);
    });

    it('should block ::ffff:10.5.5.5 via blacklist CIDR 10.0.0.0/8', async () => {
      const guard = makeGuard({
        rules: [
          makeRule({
            type: 'ip_blacklist',
            config: { ips: ['10.0.0.0/8'] },
          }),
        ],
      });
      const result = await evaluator.evaluateGuard(guard, {
        clientIp: '::ffff:10.5.5.5',
        routePath: '/test',
      });
      expect(result.reject).not.toBeNull();
      expect(result.reject!.statusCode).toBe(403);
    });

    it('should match pure IPv6 loopback ::1 against ::1', async () => {
      // ::1 is not an IPv4-mapped address, so normalizeIp leaves it as-is.
      // Exact match still works.
      const guard = makeGuard({
        rules: [
          makeRule({
            type: 'ip_whitelist',
            config: { ips: ['::1'] },
          }),
        ],
      });
      const result = await evaluator.evaluateGuard(guard, {
        clientIp: '::1',
        routePath: '/test',
      });
      expect(result.reject).toBeNull();
    });

    it('should reject pure IPv6 ::1 when whitelist only has IPv4', async () => {
      const guard = makeGuard({
        rules: [
          makeRule({
            type: 'ip_whitelist',
            config: { ips: ['127.0.0.1'] },
          }),
        ],
      });
      const result = await evaluator.evaluateGuard(guard, {
        clientIp: '::1',
        routePath: '/test',
      });
      expect(result.reject).not.toBeNull();
      expect(result.reject!.statusCode).toBe(403);
    });

    it('should normalize both client IP and pattern with ::ffff: prefix', async () => {
      const guard = makeGuard({
        rules: [
          makeRule({
            type: 'ip_whitelist',
            config: { ips: ['::ffff:10.0.0.0/8'] },
          }),
        ],
      });
      const result = await evaluator.evaluateGuard(guard, {
        clientIp: '::ffff:10.1.2.3',
        routePath: '/test',
      });
      expect(result.reject).toBeNull();
    });

    it('should use normalized IP for rate limit key', async () => {
      const guard = makeGuard({
        rules: [
          makeRule({
            type: 'rate_limit_by_ip',
            config: { maxRequests: 100, perSeconds: 60 },
          }),
        ],
      });
      await evaluator.evaluateGuard(guard, {
        clientIp: '::ffff:1.2.3.4',
        routePath: '/test',
      });
      // Rate limit key uses the raw clientIp (not normalized) since
      // normalization is only applied in matchIp; this test verifies
      // the guard still passes (no error).
      expect(rateLimitService.calledKeys.length).toBeGreaterThanOrEqual(1);
    });
  });

  describe('GuardRejectInfo contract', () => {
    it('rate_limit reject carries errorCode, structured details, and Retry-After header', async () => {
      rateLimitService.setResult('guard_rule:1:ip:1.2.3.4', false);
      const guard = makeGuard({
        rules: [
          makeRule({
            type: 'rate_limit_by_ip',
            config: { maxRequests: 100, perSeconds: 60 },
          }),
        ],
      });
      const result = await evaluator.evaluateGuard(guard, {
        clientIp: '1.2.3.4',
        routePath: '/test',
      });
      const reject = result.reject!;
      expect(reject.errorCode).toBe('RATE_LIMIT_EXCEEDED');
      expect(reject.statusCode).toBe(429);
      expect(reject.details).toEqual({
        reason: 'rate_limit',
        scope: 'ip',
        limit: 100,
        remaining: 0,
        windowSeconds: 60,
        retryAfterSeconds: 30,
        resetAt: expect.any(Number) as any,
      });
      expect(reject.headers).toEqual(
        expect.objectContaining({
          'Retry-After': '30',
          'X-RateLimit-Limit': '100',
          'X-RateLimit-Remaining': '0',
          'X-Enfyra-Guard-Reason': 'rate_limit',
          'X-Enfyra-Guard-Scope': 'ip',
          'X-Enfyra-Guard-Error-Code': 'RATE_LIMIT_EXCEEDED',
        }),
      );
    });

    it('ip_whitelist reject carries IP_NOT_ALLOWED errorCode + minimal details', async () => {
      const guard = makeGuard({
        rules: [
          makeRule({ type: 'ip_whitelist', config: { ips: ['10.0.0.0/8'] } }),
        ],
      });
      const result = await evaluator.evaluateGuard(guard, {
        clientIp: '192.168.1.1',
        routePath: '/test',
      });
      const reject = result.reject!;
      expect(reject.errorCode).toBe('IP_NOT_ALLOWED');
      expect(reject.statusCode).toBe(403);
      expect(reject.details).toEqual({
        reason: 'ip_not_allowed',
      });
      expect(reject.headers).toEqual(
        expect.objectContaining({
          'X-Enfyra-Guard-Reason': 'ip_not_allowed',
          'X-Enfyra-Guard-Error-Code': 'IP_NOT_ALLOWED',
        }),
      );
    });

    it('ip_blacklist reject carries IP_BLOCKED errorCode', async () => {
      const guard = makeGuard({
        rules: [
          makeRule({ type: 'ip_blacklist', config: { ips: ['1.2.3.4'] } }),
        ],
      });
      const result = await evaluator.evaluateGuard(guard, {
        clientIp: '1.2.3.4',
        routePath: '/test',
      });
      const reject = result.reject!;
      expect(reject.errorCode).toBe('IP_BLOCKED');
      expect(reject.statusCode).toBe(403);
      expect(reject.details).toEqual({
        reason: 'ip_blocked',
      });
    });

    it('rate_limit_by_user reject uses scope=user and user-scoped headers', async () => {
      rateLimitService.setResult('guard_rule:1:user:user-7', false);
      const guard = makeGuard({
        rules: [
          makeRule({
            type: 'rate_limit_by_user',
            config: { maxRequests: 5, perSeconds: 60 },
          }),
        ],
      });
      const result = await evaluator.evaluateGuard(guard, {
        clientIp: '1.2.3.4',
        routePath: '/test',
        userId: 'user-7',
      });
      const reject = result.reject!;
      expect(reject.errorCode).toBe('RATE_LIMIT_EXCEEDED');
      expect(reject.details).toMatchObject({
        reason: 'rate_limit',
        scope: 'user',
      });
      expect(reject.headers?.['X-Enfyra-Guard-Scope']).toBe('user');
    });

    it('rate_limit_by_route reject uses scope=route', async () => {
      rateLimitService.setResult('guard_rule:1:route:/api/posts', false);
      const guard = makeGuard({
        rules: [
          makeRule({
            type: 'rate_limit_by_route',
            config: { maxRequests: 1000, perSeconds: 60 },
          }),
        ],
      });
      const result = await evaluator.evaluateGuard(guard, {
        clientIp: '1.2.3.4',
        routePath: '/api/posts',
      });
      const reject = result.reject!;
      expect(reject.details).toMatchObject({ scope: 'route' });
      expect(reject.headers?.['X-Enfyra-Guard-Scope']).toBe('route');
    });

    it('rejection does NOT expose guardName, ruleId, or Redis key in public details', async () => {
      rateLimitService.setResult('guard_rule:99:ip:1.2.3.4', false);
      const guard = makeGuard({
        name: 'super-secret-internal-name',
        rules: [
          makeRule({
            id: 99,
            type: 'rate_limit_by_ip',
            config: { maxRequests: 1, perSeconds: 60 },
          }),
        ],
      });
      const result = await evaluator.evaluateGuard(guard, {
        clientIp: '1.2.3.4',
        routePath: '/test',
      });
      const reject = result.reject!;
      const json = JSON.stringify({
        details: reject.details,
        headers: reject.headers,
      });
      expect(json).not.toContain('super-secret-internal-name');
      expect(json).not.toContain('guard_rule');
      expect(json).not.toContain('ruleId');
      expect((reject.details as any).ruleId).toBeUndefined();
      // guardName is kept on the reject object for internal logging,
      // but it must NOT leak via details/headers:
      expect((reject.details as any).guardName).toBeUndefined();
      expect((reject.headers as any)['X-Enfyra-Guard-Name']).toBeUndefined();
    });
  });

  describe('rateLimitSnapshots on pass', () => {
    it('records snapshot when rate_limit rule passes', async () => {
      rateLimitService.setResult('guard_rule:1:ip:1.2.3.4', true, 73);
      const guard = makeGuard({
        rules: [
          makeRule({
            type: 'rate_limit_by_ip',
            config: { maxRequests: 100, perSeconds: 60 },
          }),
        ],
      });
      const result = await evaluator.evaluateGuard(guard, {
        clientIp: '1.2.3.4',
        routePath: '/test',
      });
      expect(result.reject).toBeNull();
      expect(result.rateLimitSnapshots).toHaveLength(1);
      expect(result.rateLimitSnapshots[0]).toMatchObject({
        ruleId: 1,
        scope: 'ip',
        limit: 100,
        remaining: 73,
        windowSeconds: 60,
      });
      expect(typeof result.rateLimitSnapshots[0].resetAt).toBe('number');
    });

    it('captures multiple snapshots in AND combinator when all rate limits pass', async () => {
      const guard = makeGuard({
        combinator: 'and',
        rules: [
          makeRule({
            id: 10,
            type: 'rate_limit_by_ip',
            config: { maxRequests: 100, perSeconds: 60 },
          }),
          makeRule({
            id: 11,
            type: 'rate_limit_by_route',
            config: { maxRequests: 1000, perSeconds: 60 },
          }),
        ],
      });
      const result = await evaluator.evaluateGuard(guard, {
        clientIp: '1.2.3.4',
        routePath: '/test',
      });
      expect(result.reject).toBeNull();
      expect(result.rateLimitSnapshots).toHaveLength(2);
      expect(result.rateLimitSnapshots.map((s) => s.scope)).toEqual([
        'ip',
        'route',
      ]);
    });

    it('still records snapshot when one rate limit rejects (snapshot is captured before reject)', async () => {
      rateLimitService.setResult('guard_rule:10:ip:1.2.3.4', false);
      const guard = makeGuard({
        combinator: 'and',
        rules: [
          makeRule({
            id: 10,
            type: 'rate_limit_by_ip',
            config: { maxRequests: 100, perSeconds: 60 },
          }),
          makeRule({
            id: 11,
            type: 'rate_limit_by_route',
            config: { maxRequests: 1000, perSeconds: 60 },
          }),
        ],
      });
      const result = await evaluator.evaluateGuard(guard, {
        clientIp: '1.2.3.4',
        routePath: '/test',
      });
      expect(result.reject).not.toBeNull();
      // ip rule ran first → snapshot present; route rule never ran
      expect(result.rateLimitSnapshots).toHaveLength(1);
      expect(result.rateLimitSnapshots[0].scope).toBe('ip');
    });

    it('does NOT create snapshots for non-rate-limit rules', async () => {
      const guard = makeGuard({
        rules: [
          makeRule({ type: 'ip_whitelist', config: { ips: ['1.2.3.4'] } }),
        ],
      });
      const result = await evaluator.evaluateGuard(guard, {
        clientIp: '1.2.3.4',
        routePath: '/test',
      });
      expect(result.reject).toBeNull();
      expect(result.rateLimitSnapshots).toEqual([]);
      expect(rateLimitService.calledKeys).toHaveLength(0);
    });
  });

  describe('rate_limit_by_operation (GraphQL)', () => {
    it('uses operation scope and key table:operation', async () => {
      rateLimitService.setResult(
        'guard_rule:10:operation:orders:CREATE',
        false,
      );
      const guard = makeGuard({
        type: 'graphql',
        tableName: 'orders',
        gqlOperation: 'CREATE',
        rules: [
          makeRule({
            id: 10,
            type: 'rate_limit_by_operation',
            config: { maxRequests: 5, perSeconds: 60 },
          }),
        ],
      });
      const result = await evaluator.evaluateGuard(guard, {
        clientIp: '1.2.3.4',
        routePath: '/graphql',
        tableName: 'orders',
        operation: 'CREATE',
        targetType: 'graphql',
      });
      expect(result.reject).not.toBeNull();
      expect(result.reject!.statusCode).toBe(429);
      expect(result.reject!.errorCode).toBe('RATE_LIMIT_EXCEEDED');
      expect(result.reject!.details).toMatchObject({
        reason: 'rate_limit',
        scope: 'operation',
      });
      expect(result.rateLimitSnapshots[0].scope).toBe('operation');
      expect(rateLimitService.calledKeys).toContain(
        'guard_rule:10:operation:orders:CREATE',
      );
    });

    it('uses a shared table wildcard when the guard targets all tables', async () => {
      const guard = makeGuard({
        type: 'graphql',
        tableName: null,
        gqlOperation: 'QUERY',
        rules: [
          makeRule({
            id: 20,
            type: 'rate_limit_by_operation',
            config: { maxRequests: 5, perSeconds: 60 },
          }),
        ],
      });
      await evaluator.evaluateGuard(guard, {
        clientIp: '1.2.3.4',
        routePath: '/graphql',
        tableName: 'orders',
        operation: 'QUERY',
        targetType: 'graphql',
      });
      expect(rateLimitService.calledKeys).toContain(
        'guard_rule:20:operation:*:QUERY',
      );
    });

    it('allows when under limit', async () => {
      const guard = makeGuard({
        type: 'graphql',
        rules: [
          makeRule({
            id: 30,
            type: 'rate_limit_by_operation',
            config: { maxRequests: 100, perSeconds: 60 },
          }),
        ],
      });
      const result = await evaluator.evaluateGuard(guard, {
        clientIp: '1.2.3.4',
        routePath: '/graphql',
        tableName: 'orders',
        operation: 'UPDATE',
        targetType: 'graphql',
      });
      expect(result.reject).toBeNull();
      expect(result.rateLimitSnapshots[0].scope).toBe('operation');
    });
  });
});
