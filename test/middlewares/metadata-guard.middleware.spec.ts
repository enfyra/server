import { describe, expect, it, vi } from 'vitest';
import {
  GuardEvaluatorService,
  type GuardNode,
  type GuardRuleNode,
} from '../../src/engines/cache';
import {
  preAuthMetadataGuard,
  postAuthMetadataGuard,
} from '../../src/http/middlewares/metadata-guard.middleware';
import { GuardBlockedException } from '../../src/domain/exceptions';

class MockRateLimitService {
  results = new Map<string, any>();
  calledKeys: string[] = [];
  async check(key: string, options: any) {
    this.calledKeys.push(key);
    const r = this.results.get(key);
    if (r) return r;
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

function makeReq(routePath: string, ip = '1.2.3.4'): any {
  return {
    routeData: { path: routePath },
    method: 'POST',
    ip,
    user: undefined,
  };
}

function makeRes(): any {
  const headers: Record<string, string> = {};
  return {
    headersSent: false,
    setHeader: vi.fn((k: string, v: string) => {
      headers[k] = v;
    }),
    getHeaders: () => headers,
  };
}

function makeFakeDeps(opts: {
  rateLimitResults?: Map<string, any>;
  guards: GuardNode[];
  guardCacheBuilder?: any;
  runtimeRegistry?: any;
}) {
  const rateLimitService = new MockRateLimitService();
  if (opts.rateLimitResults) {
    for (const [k, v] of opts.rateLimitResults) {
      rateLimitService.results.set(k, v);
    }
  }

  const evaluator = new GuardEvaluatorService({
    rateLimitService: rateLimitService as any,
  });

  const guardCacheBuilder = opts.guardCacheBuilder ?? {
    ensureGuardsLoaded: async () => undefined,
  };

  const runtimeRegistry = opts.runtimeRegistry ?? {
    getGuardsForRoute: () => opts.guards,
  };

  const guardAlertService = { recordAlert: vi.fn() };

  return { evaluator, guardCacheBuilder, runtimeRegistry, rateLimitService, guardAlertService };
}

describe('metadata-guard.middleware contract', () => {
  it('throws GuardBlockedException for rate_limit rejection with errorCode+details', async () => {
    const rateLimitResults = new Map([
      [
        'guard_rule:1:ip:1.2.3.4',
        {
          allowed: false,
          remaining: 0,
          resetAt: 1785690473123,
          retryAfter: 27,
          limit: 100,
          window: 60,
        },
      ],
    ]);
    const guards = [
      makeGuard({
        rules: [
          makeRule({
            type: 'rate_limit_by_ip',
            config: { maxRequests: 100, perSeconds: 60 },
          }),
        ],
      }),
    ];
    const { evaluator, guardCacheBuilder, runtimeRegistry, guardAlertService } = makeFakeDeps({
      rateLimitResults,
      guards,
    });
    const mw = preAuthMetadataGuard(
      guardCacheBuilder,
      runtimeRegistry,
      evaluator,
      guardAlertService,
    );
    const req = makeReq('/api/orders', '1.2.3.4');
    const res = makeRes();
    const next = vi.fn();

    await mw(req, res, next);

    expect(next).toHaveBeenCalledTimes(1);
    const thrown = next.mock.calls[0][0];
    expect(thrown).toBeInstanceOf(GuardBlockedException);
    expect(thrown.statusCode).toBe(429);
    expect(thrown.errorCode).toBe('RATE_LIMIT_EXCEEDED');
    expect(thrown.details).toEqual({
      reason: 'rate_limit',
      scope: 'ip',
      limit: 100,
      remaining: 0,
      windowSeconds: 60,
      retryAfterSeconds: 27,
      resetAt: 1785690473123,
    });
  });

  it('sets Retry-After + X-RateLimit-* + X-Enfyra-Guard-* headers before throwing', async () => {
    const rateLimitResults = new Map([
      [
        'guard_rule:1:ip:1.2.3.4',
        {
          allowed: false,
          remaining: 0,
          resetAt: 1785690473123,
          retryAfter: 27,
          limit: 100,
          window: 60,
        },
      ],
    ]);
    const guards = [
      makeGuard({
        rules: [
          makeRule({
            type: 'rate_limit_by_ip',
            config: { maxRequests: 100, perSeconds: 60 },
          }),
        ],
      }),
    ];
    const { evaluator, guardCacheBuilder, runtimeRegistry, guardAlertService } = makeFakeDeps({
      rateLimitResults,
      guards,
    });
    const mw = preAuthMetadataGuard(
      guardCacheBuilder,
      runtimeRegistry,
      evaluator,
      guardAlertService,
    );
    const req = makeReq('/api/orders', '1.2.3.4');
    const res = makeRes();
    const next = vi.fn();

    await mw(req, res, next);

    expect(res.setHeader).toHaveBeenCalledWith('Retry-After', '27');
    expect(res.setHeader).toHaveBeenCalledWith('X-RateLimit-Limit', '100');
    expect(res.setHeader).toHaveBeenCalledWith('X-RateLimit-Remaining', '0');
    expect(res.setHeader).toHaveBeenCalledWith('X-RateLimit-Reset', '1785690473123');
    expect(res.setHeader).toHaveBeenCalledWith('X-Enfyra-Guard-Reason', 'rate_limit');
    expect(res.setHeader).toHaveBeenCalledWith('X-Enfyra-Guard-Scope', 'ip');
    expect(res.setHeader).toHaveBeenCalledWith('X-Enfyra-Guard-Error-Code', 'RATE_LIMIT_EXCEEDED');
  });

  it('does NOT call setHeader when headers already sent', async () => {
    const rateLimitResults = new Map([
      [
        'guard_rule:1:ip:1.2.3.4',
        {
          allowed: false,
          remaining: 0,
          resetAt: Date.now() + 60000,
          retryAfter: 10,
          limit: 100,
          window: 60,
        },
      ],
    ]);
    const guards = [
      makeGuard({
        rules: [
          makeRule({
            type: 'rate_limit_by_ip',
            config: { maxRequests: 100, perSeconds: 60 },
          }),
        ],
      }),
    ];
    const { evaluator, guardCacheBuilder, runtimeRegistry, guardAlertService } = makeFakeDeps({
      rateLimitResults,
      guards,
    });
    const mw = preAuthMetadataGuard(
      guardCacheBuilder,
      runtimeRegistry,
      evaluator,
      guardAlertService,
    );
    const req = makeReq('/api/orders');
    const res = makeRes();
    res.headersSent = true;
    const next = vi.fn();

    await mw(req, res, next);

    expect(res.setHeader).not.toHaveBeenCalled();
    expect(next.mock.calls[0][0]).toBeInstanceOf(GuardBlockedException);
  });

  it('throws GuardBlockedException with IP_NOT_ALLOWED for ip_whitelist reject', async () => {
    const guards = [
      makeGuard({
        rules: [
          makeRule({
            type: 'ip_whitelist',
            config: { ips: ['10.0.0.0/8'] },
          }),
        ],
      }),
    ];
    const { evaluator, guardCacheBuilder, runtimeRegistry, guardAlertService } = makeFakeDeps({
      guards,
    });
    const mw = preAuthMetadataGuard(
      guardCacheBuilder,
      runtimeRegistry,
      evaluator,
      guardAlertService,
    );
    const req = makeReq('/api/orders', '192.168.1.1');
    const res = makeRes();
    const next = vi.fn();

    await mw(req, res, next);

    const thrown = next.mock.calls[0][0];
    expect(thrown.statusCode).toBe(403);
    expect(thrown.errorCode).toBe('IP_NOT_ALLOWED');
    expect(thrown.details).toEqual({
      reason: 'ip_not_allowed',
    });
    expect(res.setHeader).toHaveBeenCalledWith('X-Enfyra-Guard-Reason', 'ip_not_allowed');
    expect(res.setHeader).toHaveBeenCalledWith('X-Enfyra-Guard-Error-Code', 'IP_NOT_ALLOWED');
  });

  it('post_auth middleware evaluates with userId (user-scoped rate limit)', async () => {
    const rateLimitResults = new Map([
      [
        'guard_rule:1:user:user-7',
        {
          allowed: false,
          remaining: 0,
          resetAt: Date.now() + 60000,
          retryAfter: 5,
          limit: 5,
          window: 60,
        },
      ],
    ]);
    const guards = [
      makeGuard({
        rules: [
          makeRule({
            type: 'rate_limit_by_user',
            config: { maxRequests: 5, perSeconds: 60 },
          }),
        ],
      }),
    ];
    const { evaluator, guardCacheBuilder, runtimeRegistry, guardAlertService } = makeFakeDeps({
      rateLimitResults,
      guards,
    });
    const mw = postAuthMetadataGuard(
      guardCacheBuilder,
      runtimeRegistry,
      evaluator,
      guardAlertService,
    );
    const req = makeReq('/api/orders');
    req.user = { id: 'user-7' };
    const res = makeRes();
    const next = vi.fn();

    await mw(req, res, next);

    const thrown = next.mock.calls[0][0];
    expect(thrown.statusCode).toBe(429);
    expect(thrown.details).toMatchObject({
      reason: 'rate_limit',
      scope: 'user',
      limit: 5,
      retryAfterSeconds: 5,
    });
    expect(res.setHeader).toHaveBeenCalledWith('X-Enfyra-Guard-Scope', 'user');
  });

  it('passes (no throw, no header) when guard passes', async () => {
    const guards = [
      makeGuard({
        rules: [
          makeRule({
            type: 'ip_whitelist',
            config: { ips: ['1.2.3.4'] },
          }),
        ],
      }),
    ];
    const { evaluator, guardCacheBuilder, runtimeRegistry, guardAlertService } = makeFakeDeps({
      guards,
    });
    const mw = preAuthMetadataGuard(
      guardCacheBuilder,
      runtimeRegistry,
      evaluator,
      guardAlertService,
    );
    const req = makeReq('/api/orders', '1.2.3.4');
    const res = makeRes();
    const next = vi.fn();

    await mw(req, res, next);

    expect(next).toHaveBeenCalledWith();
    expect(res.setHeader).not.toHaveBeenCalled();
  });

  it('sets X-RateLimit-* headers on successful pass when rate_limit rule was evaluated', async () => {
    const rateLimitResults = new Map([
      [
        'guard_rule:7:ip:1.2.3.4',
        {
          allowed: true,
          remaining: 80,
          resetAt: 1785690473123,
          retryAfter: 0,
          limit: 100,
          window: 60,
        },
      ],
    ]);
    const guards = [
      makeGuard({
        rules: [
          makeRule({
            id: 7,
            type: 'rate_limit_by_ip',
            config: { maxRequests: 100, perSeconds: 60 },
          }),
        ],
      }),
    ];
    const { evaluator, guardCacheBuilder, runtimeRegistry, guardAlertService } = makeFakeDeps({
      rateLimitResults,
      guards,
    });
    const mw = preAuthMetadataGuard(
      guardCacheBuilder,
      runtimeRegistry,
      evaluator,
      guardAlertService,
    );
    const req = makeReq('/api/orders', '1.2.3.4');
    const res = makeRes();
    const next = vi.fn();

    await mw(req, res, next);

    expect(next).toHaveBeenCalledWith();
    expect(res.setHeader).toHaveBeenCalledWith('X-RateLimit-Limit', '100');
    expect(res.setHeader).toHaveBeenCalledWith('X-RateLimit-Remaining', '80');
    expect(res.setHeader).toHaveBeenCalledWith('X-RateLimit-Reset', '1785690473123');
    expect(res.setHeader).toHaveBeenCalledWith('X-RateLimit-Window', '60');
    expect(res.setHeader).toHaveBeenCalledWith('X-RateLimit-Scope', 'ip');
    expect(res.setHeader).toHaveBeenCalledWith('X-RateLimit-Used', '20');
  });

  it('uses the strictest remaining ratio when multiple rate-limit guards pass', async () => {
    const rateLimitResults = new Map([
      [
        'guard_rule:7:ip:1.2.3.4',
        {
          allowed: true,
          remaining: 80,
          resetAt: 1785690473123,
          retryAfter: 0,
          limit: 100,
          window: 60,
        },
      ],
      [
        'guard_rule:8:route:/api/orders',
        {
          allowed: true,
          remaining: 2,
          resetAt: 1785690480000,
          retryAfter: 0,
          limit: 10,
          window: 60,
        },
      ],
    ]);
    const guards = [
      makeGuard({
        rules: [
          makeRule({
            id: 7,
            type: 'rate_limit_by_ip',
            config: { maxRequests: 100, perSeconds: 60 },
          }),
          makeRule({
            id: 8,
            type: 'rate_limit_by_route',
            config: { maxRequests: 10, perSeconds: 60 },
          }),
        ],
      }),
    ];
    const { evaluator, guardCacheBuilder, runtimeRegistry, guardAlertService } = makeFakeDeps({
      rateLimitResults,
      guards,
    });
    const mw = preAuthMetadataGuard(
      guardCacheBuilder,
      runtimeRegistry,
      evaluator,
      guardAlertService,
    );
    const req = makeReq('/api/orders', '1.2.3.4');
    const res = makeRes();
    const next = vi.fn();

    await mw(req, res, next);

    expect(next).toHaveBeenCalledWith();
    expect(res.setHeader).toHaveBeenCalledWith('X-RateLimit-Limit', '10');
    expect(res.setHeader).toHaveBeenCalledWith('X-RateLimit-Remaining', '2');
    expect(res.setHeader).toHaveBeenCalledWith('X-RateLimit-Scope', 'route');
    expect(res.setHeader).toHaveBeenCalledWith('X-RateLimit-Used', '8');
  });

  it('does NOT set rate-limit headers when rate-limit rule passes but res.headersSent', async () => {
    const rateLimitResults = new Map([
      [
        'guard_rule:7:ip:1.2.3.4',
        {
          allowed: true,
          remaining: 73,
          resetAt: 1,
          retryAfter: 0,
          limit: 100,
          window: 60,
        },
      ],
    ]);
    const guards = [
      makeGuard({
        rules: [
          makeRule({
            id: 7,
            type: 'rate_limit_by_ip',
            config: { maxRequests: 100, perSeconds: 60 },
          }),
        ],
      }),
    ];
    const { evaluator, guardCacheBuilder, runtimeRegistry, guardAlertService } = makeFakeDeps({
      rateLimitResults,
      guards,
    });
    const mw = preAuthMetadataGuard(
      guardCacheBuilder,
      runtimeRegistry,
      evaluator,
      guardAlertService,
    );
    const req = makeReq('/api/orders', '1.2.3.4');
    const res = makeRes();
    res.headersSent = true;
    const next = vi.fn();

    await mw(req, res, next);

    expect(res.setHeader).not.toHaveBeenCalled();
    expect(next).toHaveBeenCalledWith();
  });

  it('does NOT expose guardName or Redis key in thrown details', async () => {
    const rateLimitResults = new Map([
      [
        'guard_rule:99:ip:1.2.3.4',
        {
          allowed: false,
          remaining: 0,
          resetAt: 1,
          retryAfter: 1,
          limit: 1,
          window: 60,
        },
      ],
    ]);
    const guards = [
      makeGuard({
        name: 'super-secret-internal-name',
        rules: [
          makeRule({
            id: 99,
            type: 'rate_limit_by_ip',
            config: { maxRequests: 1, perSeconds: 60 },
          }),
        ],
      }),
    ];
    const { evaluator, guardCacheBuilder, runtimeRegistry, guardAlertService } = makeFakeDeps({
      rateLimitResults,
      guards,
    });
    const mw = preAuthMetadataGuard(
      guardCacheBuilder,
      runtimeRegistry,
      evaluator,
      guardAlertService,
    );
    const req = makeReq('/api/orders', '1.2.3.4');
    const res = makeRes();
    const next = vi.fn();

    await mw(req, res, next);

    const thrown = next.mock.calls[0][0];
    const detailsJson = JSON.stringify(thrown.details);
    expect(detailsJson).not.toContain('super-secret-internal-name');
    expect(detailsJson).not.toContain('guard_rule');
    expect(detailsJson).not.toContain('1.2.3.4');
    expect(detailsJson).not.toContain('99');
  });
});