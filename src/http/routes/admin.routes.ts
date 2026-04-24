import type { Express, Response } from 'express';
import type { AwilixContainer } from 'awilix';
import type { Cradle } from '../../container';
import { transformCode } from '../../domain/shared/code-transformer';

export function registerAdminRoutes(
  app: Express,
  container: AwilixContainer<Cradle>,
) {
  app.post('/admin/reload', async (req: any, res: Response) => {
    const orchestrator =
      req.scope?.cradle?.cacheOrchestratorService ??
      container.cradle.cacheOrchestratorService;
    const startTime = Date.now();
    try {
      await orchestrator.reloadAll();
      const duration = Date.now() - startTime;
      res.json({
        success: true,
        message: 'All caches and schemas reloaded successfully',
        duration: `${duration}ms`,
      });
    } catch (error) {
      console.error('Error during reload:', error);
      throw error;
    }
  });

  app.post('/admin/reload/metadata', async (req: any, res: Response) => {
    const orchestrator =
      req.scope?.cradle?.cacheOrchestratorService ??
      container.cradle.cacheOrchestratorService;
    const start = Date.now();
    await orchestrator.reloadMetadataAndDeps();
    res.json({ success: true, duration: `${Date.now() - start}ms` });
  });

  app.post('/admin/reload/routes', async (req: any, res: Response) => {
    const orchestrator =
      req.scope?.cradle?.cacheOrchestratorService ??
      container.cradle.cacheOrchestratorService;
    const start = Date.now();
    await orchestrator.reloadRoutesOnly();
    res.json({ success: true, duration: `${Date.now() - start}ms` });
  });

  app.post('/admin/reload/graphql', async (req: any, res: Response) => {
    const orchestrator =
      req.scope?.cradle?.cacheOrchestratorService ??
      container.cradle.cacheOrchestratorService;
    const start = Date.now();
    await orchestrator.reloadGraphqlOnly();
    res.json({ success: true, duration: `${Date.now() - start}ms` });
  });

  app.post('/admin/reload/guards', async (req: any, res: Response) => {
    const orchestrator =
      req.scope?.cradle?.cacheOrchestratorService ??
      container.cradle.cacheOrchestratorService;
    const start = Date.now();
    await orchestrator.reloadGuardsOnly();
    res.json({ success: true, duration: `${Date.now() - start}ms` });
  });

  app.post('/admin/flow/test-step', async (req: any, res: Response) => {
    const flowService =
      req.scope?.cradle?.flowService ?? container.cradle.flowService;
    const result = await flowService.testStep(
      {
        type: req.body.type,
        config: req.body.config,
        timeout: req.body.timeout,
        key: req.body.key,
      },
      req.body.mockFlow,
    );
    res.json(result);
  });

  app.post('/admin/flow/trigger/:id', async (req: any, res: Response) => {
    const flowService =
      req.scope?.cradle?.flowService ?? container.cradle.flowService;
    const result = await flowService.trigger(
      req.params.id,
      req.body?.payload || {},
      req.user || null,
    );
    res.json({
      success: true,
      message: 'Flow triggered',
      jobId: result.jobId,
      flowId: result.flowId,
    });
  });

  app.post('/admin/websocket/test-event', async (req: any, res: Response) => {
    const result = await runTest(
      req.body,
      req.scope?.cradle ?? container.cradle,
    );
    res.json(result);
  });

  app.post('/admin/test/run', async (req: any, res: Response) => {
    const result = await runTest(
      req.body,
      req.scope?.cradle ?? container.cradle,
    );
    res.json(result);
  });
}

async function runTest(body: any, cradle: any) {
  const kind = String(body?.kind || '').trim();

  if (kind === 'flow_step') {
    const flowService = cradle.flowService;
    return flowService.testStep(
      {
        type: body.type,
        config: body.config,
        timeout: body.timeout,
        key: body.key,
      },
      body.mockFlow,
    );
  }

  if (kind === 'websocket_event') {
    const script = String(body?.script || '').trim();
    const gatewayPath = String(
      body?.gatewayPath || body?.path || '/__ws_test__',
    ).trim();
    const eventName = String(body?.eventName || '').trim();
    const timeoutMs = Number(body?.timeoutMs ?? body?.timeout ?? 5000);
    const payload = body?.payload ?? body?.body ?? {};
    const user = body?.user ?? null;
    const headers = body?.headers ?? {};

    if (!eventName) {
      return {
        success: false,
        error: { code: 'MISSING_EVENT_NAME', message: 'eventName is required' },
      };
    }
    if (!script) {
      return {
        success: false,
        error: { code: 'MISSING_SCRIPT', message: 'script is required' },
      };
    }

    const handlerExecutorService = cradle.executorEngineService;
    const repoRegistryService = cradle.repoRegistryService;
    const dynamicContextFactory = cradle.dynamicContextFactory;

    const { ctx, emitted } = dynamicContextFactory.createTestWebsocketCapture({
      method: 'WS_EVENT_TEST',
      url: `${gatewayPath}/${eventName}`,
      body: payload || {},
      user,
      headers,
      correlationId: `ws_test_${Date.now()}`,
    });

    ctx.$repos = repoRegistryService.createReposProxy(ctx);

    try {
      const transformed = transformCode(script);
      const result = await handlerExecutorService.run(
        transformed,
        ctx,
        timeoutMs,
      );
      return {
        success: true,
        result,
        logs: ctx.$share?.$logs?.length ? ctx.$share.$logs : [],
        emitted,
      };
    } catch (error: any) {
      return {
        success: false,
        error: {
          code: error?.errorCode || error?.code || 'TEST_FAILED',
          message: error?.message || 'Test failed',
          details: error?.details,
        },
        logs: ctx.$share?.$logs?.length ? ctx.$share.$logs : [],
        emitted,
      };
    }
  }

  if (kind === 'websocket_connection') {
    const script = String(body?.script || '').trim();
    const gatewayPath = String(
      body?.gatewayPath || body?.path || '/__ws_test__',
    ).trim();
    const timeoutMs = Number(body?.timeoutMs ?? body?.timeout ?? 5000);
    const payload = body?.payload ?? body?.body ?? {};
    const user = body?.user ?? null;
    const headers = body?.headers ?? {};

    if (!script) {
      return {
        success: false,
        error: { code: 'MISSING_SCRIPT', message: 'script is required' },
      };
    }

    const handlerExecutorService = cradle.executorEngineService;
    const repoRegistryService = cradle.repoRegistryService;
    const dynamicContextFactory = cradle.dynamicContextFactory;

    const { ctx, emitted } = dynamicContextFactory.createTestWebsocketCapture({
      method: 'WS_CONNECT_TEST',
      url: gatewayPath,
      body: payload || {},
      user,
      headers,
      correlationId: `ws_connect_test_${Date.now()}`,
    });

    ctx.$repos = repoRegistryService.createReposProxy(ctx);

    try {
      const transformed = transformCode(script);
      const result = await handlerExecutorService.run(
        transformed,
        ctx,
        timeoutMs,
      );
      return {
        success: true,
        result,
        logs: ctx.$share?.$logs?.length ? ctx.$share.$logs : [],
        emitted,
      };
    } catch (error: any) {
      return {
        success: false,
        error: {
          code: error?.errorCode || error?.code || 'TEST_FAILED',
          message: error?.message || 'Test failed',
          details: error?.details,
        },
        logs: ctx.$share?.$logs?.length ? ctx.$share.$logs : [],
        emitted,
      };
    }
  }

  return {
    success: false,
    error: { code: 'INVALID_TEST_KIND', message: 'Invalid test kind' },
  };
}
