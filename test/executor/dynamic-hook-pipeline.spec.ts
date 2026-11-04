import { createServer, type Server } from 'node:http';
import { ExecutorEngineService, IsolatedExecutorService } from '@enfyra/kernel';
import express from 'express';
import { afterAll, beforeAll, expect, it } from 'vitest';
import { RuntimeScriptExecutorService } from '../../src/engines/cache/services/runtime-script-executor.service';
import { dynamicInterceptorBegin } from '../../src/http/middlewares/dynamic-interceptor.middleware';
import { createNativeRouteRegistrar } from '../../src/http/utils/native-route-binding.util';
import { registerDynamicRoutes } from '../../src/http/routes/dynamic.routes';
import { parseQueryMiddleware } from '../../src/http/middlewares/parse-query.middleware';
import { DynamicService } from '../../src/modules/dynamic-api/services/dynamic.service';
import { emptySharedHelperRegistry } from '../helpers/shared-helper-registry';

const isolated = new IsolatedExecutorService({
  packageCacheService: { getPackages: async () => [] },
  packageCdnLoaderService: { getPackageSources: () => [] },
  tuning: { maxConcurrentWorkers: 1, isolateMemoryLimitMb: 64, isolatesPerWorker: 1 },
});
const kernel = new ExecutorEngineService({ isolatedExecutorService: isolated });
const batches: string[][] = [];
const runBatch = kernel.runBatch.bind(kernel);
kernel.runBatch = (req, ...args) => {
  batches.push(req.routeData.__codeBlocks.map((block: { type: string }) => block.type));
  return runBatch(req, ...args);
};
const executor = new RuntimeScriptExecutorService({
  runtimeRegistryService: emptySharedHelperRegistry(),
  kernelExecutorEngineService: kernel,
});
const effects: string[] = [];
let requestState: any;
const dynamicService = new DynamicService({
  executorEngineService: executor,
  loggingService: { error() {} },
  dynamicCancellationService: { async run() {} },
} as any);
let server: Server;
let baseUrl: string;

beforeAll(async () => {
  const app = express();
  app.use(parseQueryMiddleware);
  app.use((req: any, res, next) => {
    const native = req.path.startsWith('/native');
    const fail = req.path.endsWith('/throw');
    const respond = req.path.endsWith('/response');
    const stream = req.path.endsWith('/stream');
    req.routeData = {
      route: { path: req.path }, path: req.path, params: {}, res,
      routeMethodConfig: { available: true, method: { name: 'GET' }, timeout: 5000 },
      handler: native ? '' : 'await $ctx.$helpers.mark("handler"); return { callback: $ctx.$share.dispatch(), body: $ctx.$body.pre };',
      context: {
        $body: {}, $query: {}, $params: {}, $share: { $logs: [] },
        $api: { request: {} },
        $helpers: { mark: (phase: string) => effects.push(phase) },
      },
      preHooks: [{ code: fail
        ? '$ctx.$logs("before-throw"); await $ctx.$helpers.mark("pre"); $ctx.$throw.http(403, "denied");'
        : respond
          ? 'return await $ctx.$res.json({ lane: true }, { statusCode: 202 });'
          : stream
            ? '$ctx.$res.stream(require("node:stream").Readable.from(["lane"])); return { ignored: true };'
            : '$ctx.$body.pre = 7; $ctx.$share.dispatch = () => 7; await $ctx.$helpers.mark("pre"); return { ignored: true };' },
        { code: 'await $ctx.$helpers.mark("next-pre");' }],
      postHooks: [
        { code: 'if ($ctx.$error) { $ctx.$logs("after-throw"); await $ctx.$helpers.mark("api-error:" + $ctx.$api.error.statusCode); } await $ctx.$helpers.mark($ctx.$error ? "post-error:" + $ctx.$error.statusCode : "post-success");' },
        ...(fail ? [{ code: 'throw new Error("isolated post failure");' }, { code: 'await $ctx.$helpers.mark("last-post");' }] : []),
      ],
    };
    requestState = req;
    next();
  });
  const container: any = { cradle: { dynamicService, executorEngineService: executor, eventEmitter: { emit() {} } } };
  const native = createNativeRouteRegistrar(app, dynamicInterceptorBegin(executor));
  for (const path of ['/native/ok', '/native/throw', '/native/response', '/native/stream']) {
    native.get(path, (req: any, res) => {
      effects.push('native');
      res.json({ body: req.body.pre });
    });
  }
  registerDynamicRoutes(app, container);
  app.use((error: any, _req: any, res: any, _next: any) => {
    res.status(error.statusCode || error.getStatus?.() || 500).json({ failed: true });
  });
  server = createServer(app);
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
  baseUrl = 'http://127.0.0.1:' + (server.address() as { port: number }).port;
});

afterAll(async () => {
  isolated.onDestroy();
  server?.closeAllConnections();
  if (server) await new Promise<void>(resolve => server.close(() => resolve()));
});

it.each([['/dynamic/ok', ['preHook', 'preHook', 'handler', 'postHook']], ['/native/ok', ['preHook', 'preHook']]] as const)('continues after return on %s', async (path, firstBatch) => {
  effects.length = 0;
  batches.length = 0;
  const response = await fetch(baseUrl + path, { signal: AbortSignal.timeout(7000) });
  expect(response.status).toBe(200);
  expect(await response.json()).toEqual(path.startsWith('/native') ? { body: 7 } : { callback: 7, body: 7 });
  expect(effects).toEqual(['pre', 'next-pre', path.startsWith('/native') ? 'native' : 'handler', 'post-success']);
  expect(batches[0]).toEqual(firstBatch);
  expect(batches).toHaveLength(path.startsWith('/native') ? 2 : 1);
});

it.each(['/dynamic/throw', '/native/throw'])('skips inference but runs error post-hooks exactly once on %s', async (path) => {
  effects.length = 0;
  batches.length = 0;
  const response = await fetch(baseUrl + path, { signal: AbortSignal.timeout(7000) });
  expect(response.status).toBe(403);
  expect(await response.json()).toMatchObject({ failed: true });
  expect(effects).toEqual(['pre', 'api-error:403', 'post-error:403', 'last-post']);
  expect(requestState.routeData.context.$share.$logs).toEqual(['before-throw', 'after-throw']);
  expect(batches).toHaveLength(path.startsWith('/native') ? 2 : 1);
  if (path.startsWith('/native')) expect(batches[1]).toEqual(['postHook', 'postHook', 'postHook']);
});

it.each(['/dynamic/response', '/dynamic/stream', '/native/response', '/native/stream'])('does not execute later pre-hooks or handler after a native response on %s', async (path) => {
  effects.length = 0;
  const response = await fetch(baseUrl + path, { signal: AbortSignal.timeout(7000) });
  expect(response.status).toBe(path.endsWith('/response') ? 202 : 200);
  expect(await response.text()).toBe(path.endsWith('/response') ? '{"lane":true}' : 'lane');
  await new Promise(resolve => setTimeout(resolve, 30));
  expect(effects).toEqual(['post-success']);
});
