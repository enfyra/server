import { EventEmitter } from 'node:events';
import { createServer, type Server } from 'node:http';
import { Readable } from 'node:stream';
import { describe, expect, it, vi } from 'vitest';
import {
  buildExpressApp,
  disposeRequestScopeOnResponse,
} from '../../src/express-app';
import { attachStreamResponseHelper } from '../../src/modules/dynamic-api/services/dynamic.service';

function makeReqRes(dispose = vi.fn()) {
  const res = new EventEmitter();
  const req = {
    scope: {
      dispose,
    },
  };
  return { req, res, dispose };
}

describe('request scope disposal', () => {
  it('disposes the request scope when the response finishes', async () => {
    const { req, res, dispose } = makeReqRes();

    disposeRequestScopeOnResponse(req, res);
    res.emit('finish');
    await Promise.resolve();

    expect(dispose).toHaveBeenCalledTimes(1);
  });

  it('disposes the request scope when the connection closes before finish', async () => {
    const { req, res, dispose } = makeReqRes();

    disposeRequestScopeOnResponse(req, res);
    res.emit('close');
    await Promise.resolve();

    expect(dispose).toHaveBeenCalledTimes(1);
  });

  it('disposes the request scope only once when finish and close both fire', async () => {
    const { req, res, dispose } = makeReqRes();

    disposeRequestScopeOnResponse(req, res);
    res.emit('finish');
    res.emit('close');
    await Promise.resolve();

    expect(dispose).toHaveBeenCalledTimes(1);
  });
});

function listen(server: Server): Promise<string> {
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const address = server.address();
      if (!address || typeof address === 'string') {
        reject(new Error('Failed to bind test server'));
        return;
      }
      resolve(`http://127.0.0.1:${address.port}`);
    });
  });
}

function close(server: Server): Promise<void> {
  return new Promise((resolve, reject) => {
    server.close((error) => (error ? reject(error) : resolve()));
  });
}

function makeAppContainer(dispose = vi.fn()) {
  const cradle: any = {
    settingCacheService: {
      getMaxRequestBodySizeBytes: () => 1024 * 1024,
      getMaxUploadFileSizeBytes: () => 1024 * 1024,
    },
    runtimeMetricsCollectorService: {
      recordRequest: vi.fn(),
      runWithQueryContext: async (_context: string, callback: () => void) => {
        callback();
      },
    },
    routeCacheService: {
      matchRoute: async (method: string, path: string) => {
        const isKnownGet =
          method === 'GET' && ['/ok', '/boom', '/stream'].includes(path);
        const isKnownPost = method === 'POST' && path === '/webhook';
        if (!isKnownGet && !isKnownPost) {
          return null;
        }
        return {
          params: {},
          route: {
            path,
            route: { path },
            availableMethods: [{ name: method }],
            publishedMethods: [{ name: method }],
            handlers: [],
            preHooks: [],
            postHooks: [],
          },
        };
      },
      getRouteEngine: () => ({
        find: (method: string, path: string) => {
        const isKnownGet =
          method === 'GET' && ['/ok', '/boom', '/stream'].includes(path);
        const isKnownPost = method === 'POST' && path === '/webhook';
        if (!isKnownGet && !isKnownPost) {
          return null;
        }
        return {
          params: {},
          route: {
            path,
            route: { path },
            availableMethods: [{ name: method }],
            publishedMethods: [{ name: method }],
            handlers: [],
            preHooks: [],
            postHooks: [],
            },
          };
        },
      }),
    },
    repoRegistryService: {
      createReposProxy: vi.fn(() => ({})),
    },
    uploadFileHelper: {
      createStorageHelper: vi.fn(() => ({})),
    },
    rateLimitService: {
      check: vi.fn(),
      reset: vi.fn(),
      status: vi.fn(),
    },
    flowService: {
      trigger: vi.fn(),
    },
    dynamicContextFactory: {
      createHttp: vi.fn((req: any) => ({
        $body: req.body || {},
        $helpers: {},
        $query: req.query || {},
        $share: { $logs: [] },
        $req: {
          headers: req.headers,
          rawBody: req.rawBody,
          ip: '127.0.0.1',
        },
      })),
    },
    guardCacheService: {
      ensureGuardsLoaded: vi.fn(),
      getGuardsForRoute: vi.fn(() => []),
    },
    guardEvaluatorService: {
      evaluateGuard: vi.fn(),
    },
    queryBuilderService: {},
    cacheService: {},
    envService: {
      get: vi.fn(() => 'test-secret'),
    },
    policyService: {
      checkRequestAccess: vi.fn(() => ({ allow: true })),
    },
    executorEngineService: {
      register: vi.fn(),
      runBatch: vi.fn(),
    },
    dynamicService: {
      runHandler: vi.fn(async (req: any) => {
        if (req.path === '/webhook') {
          return {
            body: req.routeData.context.$body,
            signature:
              req.routeData.context.$req.headers['paddle-signature'] ?? null,
            rawBody: req.routeData.context.$req.rawBody,
          };
        }
        if (req.path === '/boom') {
          throw new Error('pipeline failure');
        }
        if (req.path === '/stream') {
          attachStreamResponseHelper(req.routeData.res);
          req.routeData.context.$res = req.routeData.res;
          req.routeData.context.$res.stream(Readable.from(['backup-stream']), {
            mimetype: 'application/sql',
            filename: 'backup.sql',
            headers: {
              'X-Backup': 'yes',
            },
          });
          return undefined;
        }
        return { success: true };
      }),
    },
    graphqlService: {
      getYogaApp: vi.fn(() => (_req: any, _res: any, next: any) => next()),
    },
  };

  const root: any = {
    cradle,
    createScope: vi.fn(() => ({
      cradle,
      register: vi.fn(),
      dispose,
    })),
  };
  return { container: root, dispose };
}

describe('request scope disposal through the Express pipeline', () => {
  it('disposes the request scope after a successful dynamic response', async () => {
    const { container, dispose } = makeAppContainer();
    const server = createServer(buildExpressApp(container));
    const baseUrl = await listen(server);

    try {
      const response = await fetch(`${baseUrl}/ok`);
      expect(response.status).toBe(200);
      expect(await response.json()).toEqual({ success: true });
      await new Promise((resolve) => setImmediate(resolve));
      expect(dispose).toHaveBeenCalledTimes(1);
    } finally {
      await close(server);
    }
  });

  it('disposes the request scope when the pipeline reaches the error handler', async () => {
    const { container, dispose } = makeAppContainer();
    const server = createServer(buildExpressApp(container));
    const baseUrl = await listen(server);

    try {
      const response = await fetch(`${baseUrl}/boom`);
      expect(response.status).toBe(500);
      const body = await response.json();
      expect(body.error.message).toBe('pipeline failure');
      await new Promise((resolve) => setImmediate(resolve));
      expect(dispose).toHaveBeenCalledTimes(1);
    } finally {
      await close(server);
    }
  });

  it('fails app construction when GraphQL endpoint setup fails', () => {
    const { container } = makeAppContainer();
    container.cradle.graphqlService.getYogaApp = vi.fn(() => {
      throw new Error('schema missing');
    });

    expect(() => buildExpressApp(container)).toThrow('schema missing');
  });

  it('streams dynamic handler responses without JSON wrapping', async () => {
    const { container, dispose } = makeAppContainer();
    const server = createServer(buildExpressApp(container));
    const baseUrl = await listen(server);

    try {
      const response = await fetch(`${baseUrl}/stream`);
      expect(response.status).toBe(200);
      expect(response.headers.get('content-type')).toContain('application/sql');
      expect(response.headers.get('content-disposition')).toBe(
        'attachment; filename="backup.sql"',
      );
      expect(await response.text()).toBe('backup-stream');
      await new Promise((resolve) => setImmediate(resolve));
      expect(dispose).toHaveBeenCalledTimes(1);
    } finally {
      await close(server);
    }
  });

  it('exposes parsed body, headers, and rawBody to dynamic handlers', async () => {
    const { container, dispose } = makeAppContainer();
    const server = createServer(buildExpressApp(container));
    const baseUrl = await listen(server);
    const payload = '{"event_type":"transaction.completed","data":{"id":"txn_1"}}';

    try {
      const response = await fetch(`${baseUrl}/webhook`, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'Paddle-Signature': 'ts=1;h1=test',
        },
        body: payload,
      });
      expect(response.status).toBe(200);
      expect(await response.json()).toEqual({
        body: {
          event_type: 'transaction.completed',
          data: { id: 'txn_1' },
        },
        signature: 'ts=1;h1=test',
        rawBody: payload,
      });
      await new Promise((resolve) => setImmediate(resolve));
      expect(dispose).toHaveBeenCalledTimes(1);
    } finally {
      await close(server);
    }
  });
});
