/**
 * Tests that $req in TDynamicContext only exposes a safe whitelist of properties
 * derived from the raw Express request object.
 *
 * The whitelist lives in `DynamicContextFactory.createHttp`, so this exercises the
 * real factory rather than a copy of its object literal. An inlined copy would keep
 * passing after the production whitelist drifted.
 */
import { DynamicContextFactory } from '../../src/shared/services/dynamic-context.factory';

const WHITELISTED_KEYS = [
  'method',
  'url',
  'headers',
  'query',
  'params',
  'ip',
  'hostname',
  'protocol',
  'path',
  'originalUrl',
  'rawBody',
];

function makeFactory(): DynamicContextFactory {
  const env = { SECRET_KEY: 'test-secret-key' };
  return new DynamicContextFactory({
    bcryptService: {
      hash: async () => 'hash',
      compare: async () => true,
    } as any,
    apiTokenService: {} as any,
    userCacheService: {
      get: async () => null,
      set: async () => undefined,
      deleteKey: async () => undefined,
    } as any,
    envService: { get: (key: string) => env[key] } as any,
    databaseConfigService: {} as any,
    knexService: {} as any,
    mongoService: {} as any,
    websocketContextFactory: {
      createGlobalProxy: () => ({}),
    } as any,
  });
}

function makeMockReq(overrides: Record<string, any> = {}): Record<string, any> {
  return {
    method: 'GET',
    url: '/api/test?foo=bar',
    headers: {
      'content-type': 'application/json',
      authorization: 'Bearer token123',
    },
    query: { foo: 'bar' },
    params: { id: '42' },
    hostname: 'example.com',
    protocol: 'https',
    path: '/api/test',
    originalUrl: '/api/test?foo=bar',
    socket: { remoteAddress: '10.0.0.1' },
    connection: { remoteAddress: '10.0.0.1' },
    res: { send: jest.fn() },
    app: { locals: { db: 'real-db-connection' } },
    _internalSecret: 'super-secret',
    rawBody: Buffer.from('raw payload'),
    ...overrides,
  };
}

function buildReqContext(
  req: Record<string, any>,
  realClientIP: string,
): Record<string, any> {
  return makeFactory().createHttp(req, { params: req.params ?? {}, realClientIP })
    .$req as Record<string, any>;
}

describe('$req context sanitization', () => {
  it('exposes only whitelisted keys', () => {
    const req = makeMockReq();
    const $req = buildReqContext(req, '10.0.0.1');
    const actualKeys = Object.keys($req);
    expect(actualKeys.sort()).toEqual([...WHITELISTED_KEYS].sort());
  });

  it('does not expose res object', () => {
    const req = makeMockReq();
    const $req = buildReqContext(req, '10.0.0.1');
    expect('res' in $req).toBe(false);
  });

  it('does not expose the socket object', () => {
    const req = makeMockReq();
    const $req = buildReqContext(req, '10.0.0.1');
    expect('socket' in $req).toBe(false);
  });

  it('does not expose app internals', () => {
    const req = makeMockReq();
    const $req = buildReqContext(req, '10.0.0.1');
    expect('app' in $req).toBe(false);
  });

  it('does not expose non-standard request properties', () => {
    const req = makeMockReq();
    const $req = buildReqContext(req, '10.0.0.1');
    expect('_internalSecret' in $req).toBe(false);
  });

  it('exposes the resolved client IP, not the raw socket address', () => {
    const req = makeMockReq();
    const $req = buildReqContext(req, '203.0.113.7');
    expect($req.ip).toBe('203.0.113.7');
  });

  it('passes through query string parameters', () => {
    const req = makeMockReq({ query: { page: '1', limit: '20' } });
    const $req = buildReqContext(req, '1.2.3.4');
    expect($req.query).toEqual({ page: '1', limit: '20' });
  });

  it('passes through path params', () => {
    const req = makeMockReq({ params: { id: '99', slug: 'my-post' } });
    const $req = buildReqContext(req, '1.2.3.4');
    expect($req.params).toEqual({ id: '99', slug: 'my-post' });
  });

  it('correctly reflects HTTP method', () => {
    const req = makeMockReq({ method: 'POST' });
    const $req = buildReqContext(req, '1.2.3.4');
    expect($req.method).toBe('POST');
  });

  it('has exactly 11 whitelisted keys — no more, no less', () => {
    const req = makeMockReq();
    const $req = buildReqContext(req, '1.2.3.4');
    expect(Object.keys($req).length).toBe(11);
  });
});
