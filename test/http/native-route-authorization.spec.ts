import { IncomingMessage, ServerResponse } from 'node:http';
import { Socket } from 'node:net';
import { describe, expect, it, vi } from 'vitest';
import { buildExpressApp } from '../../src/express-app';
import { PolicyService } from '../../src/domain/policy/policy.service';

function fixture(options: { missing?: boolean; unavailable?: boolean; allowed?: boolean; fallback?: boolean } = {}) {
  const routes = options.missing ? [] : [{
    id: 1, path: options.fallback ? '/:resource/:service/:action' : '/admin/redis/key',
    methodConfigs: [{ available: !options.unavailable, method: { name: 'GET' }, routePermissions: options.allowed ? [{ isEnabled: true, role: { id: 'reader' } }] : [] }],
  }];
  const getKey = vi.fn(async () => ({ synthetic: true }));
  const cradle: any = {
    runtimeSchemaActivationGateService: { isBlocked: () => false },
    runtimeRegistryService: { requireRoutes: () => routes, getCorsOriginPolicy: () => ({}), getMaxRequestBodySizeBytes: () => 1024, getGuardsForRoute: () => [] },
    runtimeMetricsCollectorService: { recordRequest: () => {}, runWithQueryContext: async (_kind: string, work: () => unknown) => work() },
    dynamicContextFactory: { createHttp: () => ({ $helpers: {}, $query: {}, $share: {} }) },
    repoRegistryService: { createReposProxy: () => ({}) }, uploadFileHelper: { createStorageHelper: () => ({}) },
    guardCacheBuilder: { ensureGuardsLoaded: async () => {} }, guardEvaluatorService: {}, guardAlertService: {},
    authenticationService: { authenticate: async ({ headers }: any) => {
      if (headers['x-test-user'] !== 'yes') throw Object.assign(new Error('Unauthorized'), { statusCode: 401 });
      return { user: { id: 7, roles: [{ id: 'reader' }] }, source: 'test' };
    } },
    policyService: new PolicyService({} as any), redisAdminService: { getKey },
    graphqlService: { getYogaApp: () => (_req: any, res: any) => res.json({ transport: 'graphql' }) },
    dynamicService: {},
  };
  const container: any = { cradle, createScope: () => ({ cradle, register: () => {}, dispose: async () => {} }) };
  const app = buildExpressApp(container);
  const dispatch = (url: string, authenticated = true) => new Promise<{ status: number; body: string }>((resolve, reject) => {
    const socket = new Socket();
    const req = new IncomingMessage(socket); req.method = 'GET'; req.url = url; req.headers = authenticated ? { 'x-test-user': 'yes' } : {};
    const res = new ServerResponse(req);
    const timeout = setTimeout(() => { socket.destroy(); reject(new Error('Synthetic request timed out')); }, 1000);
    res.end = ((body: unknown) => { clearTimeout(timeout); socket.destroy(); resolve({ status: res.statusCode, body: String(body ?? '') }); return res; }) as typeof res.end;
    app.handle(req, res);
  });
  return { dispatch, getKey };
}

describe('native route authorization binding', () => {
  it('denies the canonical and mixed-case native endpoint without a grant', async () => {
    const { dispatch, getKey } = fixture();
    expect((await dispatch('/admin/redis/key?key=fixture')).status).toBe(403);
    expect((await dispatch('/ADMIN/redis/key?key=fixture')).status).toBe(403);
    expect((await dispatch('/admin/redis/key/?key=fixture')).status).toBe(403);
    expect((await dispatch('/admin/redis/%6bey?key=fixture')).status).not.toBe(200);
    expect(getKey).not.toHaveBeenCalled();
  });

  it.each([{ missing: true }, { unavailable: true }])('does not execute a native route without an available metadata binding %j', async options => {
    const { dispatch, getKey } = fixture(options);
    expect((await dispatch('/admin/redis/key?key=fixture')).status).toBe(404);
    expect(getKey).not.toHaveBeenCalled();
  });

  it('does not authorize a native handler through a different generic operation', async () => {
    const { dispatch, getKey } = fixture({ allowed: true, fallback: true });
    expect((await dispatch('/admin/redis/key?key=fixture')).status).toBe(404);
    expect(getKey).not.toHaveBeenCalled();
  });

  it('keeps the authorized canonical endpoint available', async () => {
    const { dispatch, getKey } = fixture({ allowed: true });
    expect((await dispatch('/admin/redis/key?key=fixture')).status).toBe(200);
    expect(getKey).toHaveBeenCalledOnce();
  });

  it('does not bypass authentication with mixed case', async () => {
    const { dispatch, getKey } = fixture();
    expect((await dispatch('/ADMIN/redis/key', false)).status).toBe(401);
    expect(getKey).not.toHaveBeenCalled();
  });

  it('preserves the readiness probe and independently authorized GraphQL transport', async () => {
    const { dispatch } = fixture();
    expect((await dispatch('/health/ready', false)).status).toBe(200);
    const graphql = await dispatch('/graphql');
    expect(graphql.status).toBe(200);
    expect(JSON.parse(graphql.body)).toEqual({ transport: 'graphql' });
  });
});
