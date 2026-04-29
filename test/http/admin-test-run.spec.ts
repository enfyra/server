import { describe, expect, it } from 'vitest';
import { registerAdminRoutes } from '../../src/http/routes';
import { DynamicContextFactory } from '../../src/shared/services';
import { WebsocketContextFactory } from '../../src/modules/websocket';

class InlineExecutor {
  async run(code: string, ctx: any) {
    const consoleProxy = {
      log: (...args: any[]) => ctx.$logs(...args),
      warn: (...args: any[]) => ctx.$logs(...args),
      error: (...args: any[]) => ctx.$logs(...args),
      info: (...args: any[]) => ctx.$logs(...args),
    };
    const fn = new Function(
      '$ctx',
      'console',
      `return (async () => { ${code} })()`,
    );
    return fn(ctx, consoleProxy);
  }
}

function createDynamicContextFactory() {
  return new DynamicContextFactory({
    bcryptService: {} as any,
    userCacheService: {} as any,
    envService: { get: () => 'test-secret' } as any,
    websocketContextFactory: new WebsocketContextFactory({
      dynamicWebSocketGateway: {},
    }),
  });
}

function createAppHarness(cradle: any) {
  const handlers = new Map<string, any>();
  const register = (path: string, handler: any) => handlers.set(path, handler);
  const app = {
    get: register,
    post: register,
    patch: register,
    delete: register,
  };
  registerAdminRoutes(app as any, { cradle } as any);

  return async (body: any) => {
    let response: any;
    await handlers.get('/admin/test/run')(
      { body, user: { id: 7 }, scope: { cradle } },
      { json: (value: any) => (response = value) },
    );
    return response;
  };
}

describe('admin test run', () => {
  it('resolves saved route handler code and runs it with HTTP-like context', async () => {
    const cradle = {
      executorEngineService: new InlineExecutor(),
      routeCacheService: {
        getRoutes: async () => [
          {
            id: 1,
            path: '/orders',
            mainTable: { name: 'order_definition' },
            handlers: [
              {
                id: 10,
                method: { method: 'POST' },
                sourceCode:
                  'return { table: $ctx.$repos.main.table, value: $ctx.$body.value, method: $ctx.$req.method };',
                scriptLanguage: 'typescript',
              },
            ],
          },
        ],
      },
      repoRegistryService: {
        createReposProxy: (_ctx: any, mainTableName: string) => ({
          main: { table: mainTableName },
        }),
      },
      dynamicContextFactory: createDynamicContextFactory(),
      flowService: { trigger: async () => ({ triggered: true }) },
    };
    const postTest = createAppHarness(cradle);

    const result = await postTest({
      kind: 'script',
      tableName: 'route_handler_definition',
      routeId: 1,
      method: 'POST',
      body: { value: 42 },
    });

    expect(result.success).toBe(true);
    expect(result.result).toEqual({
      table: 'order_definition',
      value: 42,
      method: 'POST',
    });
  });

  it('resolves saved websocket event code and runs it with websocket context', async () => {
    const cradle = {
      executorEngineService: new InlineExecutor(),
      websocketCacheService: {
        getGateways: async () => [
          {
            id: 1,
            path: '/ws',
            events: [
              {
                id: 20,
                name: 'message',
                handlerScript:
                  'return { payload: $ctx.$data, url: $ctx.$api.request.url };',
                scriptLanguage: 'typescript',
              },
            ],
          },
        ],
      },
      repoRegistryService: {
        createReposProxy: () => ({}),
      },
      dynamicContextFactory: createDynamicContextFactory(),
    };
    const postTest = createAppHarness(cradle);

    const result = await postTest({
      kind: 'websocket_event',
      gatewayId: 1,
      eventName: 'message',
      payload: { text: 'hi' },
    });

    expect(result.success).toBe(true);
    expect(result.result).toEqual({
      payload: { text: 'hi' },
      url: '/ws/message',
    });
  });

  it('returns logs from generic script tests', async () => {
    const cradle = {
      executorEngineService: new InlineExecutor(),
      repoRegistryService: {
        createReposProxy: () => ({}),
      },
      dynamicContextFactory: createDynamicContextFactory(),
      flowService: { trigger: async () => ({ triggered: true }) },
    };
    const postTest = createAppHarness(cradle);

    const result = await postTest({
      kind: 'script',
      script: "console.log('admin-log', { ok: true }); return 'done';",
    });

    expect(result.success).toBe(true);
    expect(result.result).toBe('done');
    expect(result.logs).toEqual(['admin-log', { ok: true }]);
  });
});
