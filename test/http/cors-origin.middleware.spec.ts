import { describe, expect, it, vi } from 'vitest';
import {
  corsOriginMiddleware,
  decideCorsOrigin,
} from '../../src/http/middlewares/cors-origin.middleware';

const ALLOW_LIST = { loaded: true, origins: ['https://a.test'] };
const EMPTY_LIST = { loaded: true, origins: [] };

function createRes() {
  const headers = new Map<string, string>();
  return {
    headers,
    statusCode: undefined as number | undefined,
    ended: false,
    setHeader: vi.fn((key: string, value: string) => headers.set(key, value)),
    status: vi.fn(function (this: any, code: number) {
      this.statusCode = code;
      return this;
    }),
    end: vi.fn(function (this: any) {
      this.ended = true;
      return this;
    }),
  };
}

function makeRegistry(policy: { loaded: boolean; origins: string[] } | null) {
  return { getCorsOriginPolicy: () => policy } as any;
}

describe('decideCorsOrigin', () => {
  it('allows a request without an Origin header and grants no CORS', () => {
    expect(decideCorsOrigin(undefined, null)).toEqual({ allowed: true });
  });

  it('denies a cross-origin request while the allow-list is not loaded', () => {
    expect(
      decideCorsOrigin('https://a.test', { loaded: false, origins: [] }),
    ).toEqual({ allowed: false });
  });

  it('grants public CORS when the allow-list is empty', () => {
    expect(decideCorsOrigin('https://a.test', EMPTY_LIST)).toEqual({
      allowed: true,
      corsOrigin: '*',
    });
  });

  it('grants the configured entry for a listed origin', () => {
    expect(decideCorsOrigin('https://a.test', ALLOW_LIST)).toEqual({
      allowed: true,
      corsOrigin: 'https://a.test',
    });
  });

  it('denies an origin that is not listed', () => {
    expect(decideCorsOrigin('https://evil.test', ALLOW_LIST)).toEqual({
      allowed: false,
    });
  });

  it('never grants a null origin', () => {
    expect(decideCorsOrigin('null', ALLOW_LIST)).toEqual({ allowed: false });
  });
});

describe('corsOriginMiddleware', () => {
  it('grants CORS and answers a preflight for a listed origin', async () => {
    const middleware = corsOriginMiddleware(makeRegistry(ALLOW_LIST));
    const res = createRes();
    const next = vi.fn();

    await middleware(
      {
        method: 'OPTIONS',
        headers: {
          origin: 'https://a.test',
          'access-control-request-method': 'POST',
          'access-control-request-headers': 'content-type',
        },
      } as any,
      res as any,
      next,
    );

    expect(res.headers.get('Access-Control-Allow-Origin')).toBe(
      'https://a.test',
    );
    expect(res.headers.get('Access-Control-Allow-Credentials')).toBe('true');
    expect(res.headers.get('Vary')).toBe('Origin');
    expect(res.headers.get('Access-Control-Allow-Methods')).toContain('PATCH');
    expect(res.headers.get('Access-Control-Allow-Headers')).toBe(
      'content-type',
    );
    expect(res.statusCode).toBe(204);
    expect(next).not.toHaveBeenCalled();
  });

  it('rejects a preflight from a denied origin with 403', async () => {
    const middleware = corsOriginMiddleware(makeRegistry(ALLOW_LIST));
    const res = createRes();
    const next = vi.fn();

    await middleware(
      {
        method: 'OPTIONS',
        headers: {
          origin: 'https://evil.test',
          'access-control-request-method': 'POST',
        },
      } as any,
      res as any,
      next,
    );

    expect(res.statusCode).toBe(403);
    expect(res.headers.has('Access-Control-Allow-Origin')).toBe(false);
    expect(next).not.toHaveBeenCalled();
  });

  it('blocks a state-changing request from a denied origin with 403', async () => {
    const middleware = corsOriginMiddleware(makeRegistry(ALLOW_LIST));
    const res = createRes();
    const next = vi.fn();

    await middleware(
      { method: 'POST', headers: { origin: 'https://evil.test' } } as any,
      res as any,
      next,
    );

    expect(next.mock.calls[0][0].statusCode).toBe(403);
    expect(res.headers.has('Access-Control-Allow-Origin')).toBe(false);
  });

  it('lets a safe request through without the CORS grant when the origin is denied', async () => {
    const middleware = corsOriginMiddleware(makeRegistry(ALLOW_LIST));
    const res = createRes();
    const next = vi.fn();

    await middleware(
      { method: 'GET', headers: { origin: 'https://evil.test' } } as any,
      res as any,
      next,
    );

    expect(next).toHaveBeenCalledWith();
    expect(res.headers.has('Access-Control-Allow-Origin')).toBe(false);
  });

  it.each(['GET', 'OPTIONS'])('grants wildcard without credentials for %s', async (method) => {
    const middleware = corsOriginMiddleware(makeRegistry(EMPTY_LIST));
    const res = createRes();
    const next = vi.fn();

    await middleware(
      { method, headers: { origin: 'https://a.test', ...(method === 'OPTIONS' ? { 'access-control-request-method': 'POST', 'access-control-request-headers': 'authorization, x-enfyra-pat' } : {}) } } as any,
      res as any,
      next,
    );

    expect(res.headers.get('Access-Control-Allow-Origin')).toBe('*');
    expect(res.headers.has('Access-Control-Allow-Credentials')).toBe(false);
    if (method === 'OPTIONS') {
      expect(res.statusCode).toBe(204);
      expect(res.headers.get('Access-Control-Allow-Headers')).toBe('authorization, x-enfyra-pat');
    } else {
      expect(next).toHaveBeenCalledWith();
    }
  });
});
