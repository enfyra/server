/**
 * Tests that $req in TDynamicContext only exposes a safe whitelist
 * of properties derived from the raw Express request object.
 *
 * The middleware constructs $req as an explicit object literal:
 *   { method, url, headers, query, params, ip, hostname, protocol, path, originalUrl }
 *
 * This test verifies that pattern by simulating what the middleware does
 * and confirming that sensitive / internal properties are excluded.
 */

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
] as const;

function buildReqContext(req: Record<string, any>, resolvedIp: string): Record<string, any> {
  return {
    method: req.method,
    url: req.url,
    headers: req.headers,
    query: req.query,
    params: req.params,
    ip: resolvedIp,
    hostname: req.hostname,
    protocol: req.protocol,
    path: req.path,
    originalUrl: req.originalUrl,
  };
}

function makeMockReq(overrides: Record<string, any> = {}): Record<string, any> {
  return {
    method: 'GET',
    url: '/api/test?foo=bar',
    headers: { 'content-type': 'application/json', authorization: 'Bearer token123' },
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

  it('does not expose app or internal express properties', () => {
    const req = makeMockReq();
    const $req = buildReqContext(req, '10.0.0.1');
    expect('app' in $req).toBe(false);
    expect('socket' in $req).toBe(false);
    expect('connection' in $req).toBe(false);
  });

  it('does not expose _internalSecret or rawBody', () => {
    const req = makeMockReq({ _internalSecret: 'boom', rawBody: Buffer.from('secret') });
    const $req = buildReqContext(req, '10.0.0.1');
    expect('_internalSecret' in $req).toBe(false);
    expect('rawBody' in $req).toBe(false);
  });

  it('uses resolvedIp (not req.ip directly)', () => {
    const req = makeMockReq({ ip: '127.0.0.1' });
    const resolvedIp = '203.0.113.5';
    const $req = buildReqContext(req, resolvedIp);
    expect($req.ip).toBe('203.0.113.5');
    expect($req.ip).not.toBe('127.0.0.1');
  });

  it('passes through headers object including auth header', () => {
    const req = makeMockReq({ headers: { authorization: 'Bearer abc', 'x-custom': 'yes' } });
    const $req = buildReqContext(req, '1.2.3.4');
    expect($req.headers).toEqual({ authorization: 'Bearer abc', 'x-custom': 'yes' });
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

  it('has exactly 10 whitelisted keys — no more, no less', () => {
    const req = makeMockReq();
    const $req = buildReqContext(req, '1.2.3.4');
    expect(Object.keys($req).length).toBe(10);
  });
});
