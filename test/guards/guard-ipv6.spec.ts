import { describe, expect, it, vi } from 'vitest';
import { GuardEvaluatorService } from '../../src/engines/cache/services/guard-evaluator.service';
import type {
  GuardNode,
  GuardRuleNode,
} from '../../src/engines/cache/types/guard.types';
import type { RateLimitService } from '../../src/engines/cache/services/rate-limit.service';

const check = vi.fn(async () => ({
  allowed: true,
  remaining: 9,
  limit: 10,
  window: 60,
  resetAt: 60000,
  retryAfter: 0,
}));
const evaluator = new GuardEvaluatorService({
  rateLimitService: { check } as unknown as RateLimitService,
});
function guard(type: GuardRuleNode['type'], ips: string[] = []): GuardNode {
  return {
    id: 1,
    name: 'ip',
    type: 'route',
    position: 'pre_auth',
    combinator: 'and',
    isEnabled: true,
    isGlobal: false,
    priority: 0,
    parentId: null,
    routeId: null,
    routePath: null,
    gqlOperation: null,
    tableName: null,
    methods: [],
    methodIds: [],
    children: [],
    rules: [
      {
        id: 1,
        type,
        priority: 0,
        isEnabled: true,
        userIds: [],
        config: { ips, maxRequests: 10, perSeconds: 60 },
      },
    ],
  };
}

describe('Guard canonical IP identity', () => {
  it.each(['2001:db8::/32', '2001:0db8:0000:0000:0000:0000:0000:0005'])(
    'blocks IPv6 pattern %s',
    async (pattern) => {
      const result = await evaluator.evaluateGuard(
        guard('ip_blacklist', [pattern]),
        { clientIp: '2001:db8::5', routePath: '/' },
      );
      expect(result.reject?.errorCode).toBe('IP_BLOCKED');
    },
  );
  it('shares rate buckets across equivalent IPv6 addresses', async () => {
    check.mockClear();
    for (const clientIp of ['2001:db8::5', '2001:0DB8:0:0:0:0:0:5'])
      await evaluator.evaluateGuard(guard('rate_limit_by_ip'), {
        clientIp,
        routePath: '/',
      });
    expect(check.mock.calls[0]).toEqual(check.mock.calls[1]);
  });
  it('rejects malformed addresses instead of loosely parsing IPv4 octets', async () => {
    const result = await evaluator.evaluateGuard(
      guard('ip_whitelist', ['192.0.2.0/24']),
      { clientIp: '192.0.2.5junk', routePath: '/' },
    );
    expect(result.reject?.errorCode).toBe('IP_NOT_ALLOWED');
  });
});
