import { describe, expect, it, vi } from 'vitest';
import {
  corsOriginMiddleware,
  evaluateCorsOrigin,
} from '../../src/http/middlewares/cors-origin.middleware';

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

describe('evaluateCorsOrigin', () => {
  it('allows requests without an Origin header', () => {
    expect(evaluateCorsOrigin(undefined, null)).toBe(true);
  });

  it('denies a cross-origin request while the allow-list is not loaded', () => {
    expect(
      evaluateCorsOrigin('https://a.test', { loaded: false, origins: [] }),
    ).toBe(false);
  });

  it('allows every origin when the loaded allow-list is empty', () => {
    expect(
      evaluateCorsOrigin('https://a.test', { loaded: true, origins: [] }),
    ).toBe(true);
  });

  it('allows only listed origins once the allow-list has entries', () => {
    const policy = { loaded: true, origins: ['https://a.test'] };
    expect(evaluateCorsOrigin('https://a.test', policy)).toBe(true);
    expect(evaluateCorsOrigin('https://b.test', policy)).toBe(false);
  });
});

describe('corsOriginMiddleware', () => {
  it('grants CORS and answers a preflight for an allowed origin', async () => {
    const middleware = corsOriginMiddleware(
      makeRegistry({ loaded: true, origins: ['https://a.test'] }),
    );
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
    const middleware = corsOriginMiddleware(
      makeRegistry({ loaded: true, origins: ['https://a.test'] }),
    );
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
    const middleware = corsOriginMiddleware(
      makeRegistry({ loaded: true, origins: ['https://a.test'] }),
    );
    const res = createRes();
    const next = vi.fn();

    await middleware(
      { method: 'POST', headers: { origin: 'https://evil.test' } } as any,
      res as any,
      next,
    );

    const error = next.mock.calls[0][0];
    expect(error.statusCode).toBe(403);
    expect(res.headers.has('Access-Control-Allow-Origin')).toBe(false);
  });

  it('lets a safe request through without the CORS grant when the origin is denied', async () => {
    const middleware = corsOriginMiddleware(
      makeRegistry({ loaded: true, origins: ['https://a.test'] }),
    );
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

  it('does not treat a denied request as a preflight when the method is GET', async () => {
    const middleware = corsOriginMiddleware(makeRegistry(null));
    const res = createRes();
    const next = vi.fn();

    await middleware(
      { method: 'GET', headers: { origin: 'https://evil.test' } } as any,
      res as any,
      next,
    );

    expect(res.statusCode).toBeUndefined();
    expect(next).toHaveBeenCalledWith();
  });
});
