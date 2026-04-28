import { EventEmitter } from 'node:events';
import { createServer, type Server } from 'node:http';
import { describe, expect, it, vi } from 'vitest';
import {
  buildExpressApp,
  disposeRequestScopeOnResponse,
} from '../../src/express-app';

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
      getRouteEngine: () => ({
        find: (method: string, path: string) => {
          if (method !== 'GET' || !['/ok', '/boom'].includes(path)) {
            return null;
          }
          return {
            params: {},
            route: {
              path,
              route: { path },
              availableMethods: [{ method: 'GET' }],
              publishedMethods: [{ method: 'GET' }],
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
      createUploadFileHelper: vi.fn(),
      createUpdateFileHelper: vi.fn(),
      createDeleteFileHelper: vi.fn(),
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
        $req: { ip: '127.0.0.1' },
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
        if (req.path === '/boom') {
          throw new Error('pipeline failure');
        }
        return { success: true };
      }),
    },
    graphqlService: {
      getYogaApp: vi.fn(
        () => (_req: any, _res: any, next: any) => next(),
      ),
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
});
