import type { Express, Response } from 'express';
import type { AwilixContainer } from 'awilix';
import type { Cradle } from '../../container';
import { BadRequestException } from '../../domain/exceptions';
import {
  CACHE_EVENTS,
  CACHE_IDENTIFIERS,
} from '../../shared/utils/cache-events.constants';
import { compileScriptSource } from '../../shared/utils/script-code.util';
import type { TCacheInvalidationPayload } from '../../shared/types/cache.types';

type MenuReorderUpdate = {
  id: string | number;
  order: number;
  parent?: string | number | null;
};

function resolveOrchestrator(req: any, container: AwilixContainer<Cradle>) {
  return (
    req.scope?.cradle?.cacheOrchestratorService ??
    container.cradle.cacheOrchestratorService
  );
}

function resolveRedisAdmin(req: any, container: AwilixContainer<Cradle>) {
  return (
    req.scope?.cradle?.redisAdminService ?? container.cradle.redisAdminService
  );
}

function resolveRuntimeMonitor(req: any, container: AwilixContainer<Cradle>) {
  return (
    req.scope?.cradle?.runtimeMonitorService ??
    container.cradle.runtimeMonitorService
  );
}

function getRecordId(record: any, pkField: string) {
  if (!record) return null;
  let id =
    pkField === '_id' ? (record._id ?? record.id) : (record.id ?? record._id);
  while (id && typeof id === 'object') {
    id = pkField === '_id' ? (id._id ?? id.id) : (id.id ?? id._id);
  }
  return id ?? null;
}

function normalizeMenuReorderUpdates(body: any): MenuReorderUpdate[] {
  const input = Array.isArray(body?.updates)
    ? body.updates
    : Array.isArray(body)
      ? body
      : null;

  if (!input) {
    throw new BadRequestException('updates must be an array');
  }

  const seen = new Set<string>();
  return input.map((item: any, index: number) => {
    const id = item?.id;
    if (id == null || String(id).trim() === '') {
      throw new BadRequestException(`updates[${index}].id is required`);
    }
    const idKey = String(id);
    if (seen.has(idKey)) {
      throw new BadRequestException(
        `Duplicate menu id in reorder payload: ${idKey}`,
      );
    }
    seen.add(idKey);

    const order = Number(item?.order);
    if (!Number.isInteger(order) || order < 0) {
      throw new BadRequestException(
        `updates[${index}].order must be a non-negative integer`,
      );
    }

    const parent = item?.parent ?? null;
    return {
      id,
      order,
      parent: parent == null || String(parent).trim() === '' ? null : parent,
    };
  });
}

function isSameId(a: unknown, b: unknown) {
  return String(a ?? '') === String(b ?? '');
}

function assertNoMenuCycle(
  menuId: string | number,
  nextParentId: string | number | null,
  parentById: Map<string, string | number | null>,
) {
  const visited = new Set<string>([String(menuId)]);
  let currentParentId = nextParentId;

  while (currentParentId != null) {
    const key = String(currentParentId);
    if (visited.has(key)) {
      throw new BadRequestException('Menu parent cycle is not allowed');
    }
    visited.add(key);
    currentParentId = parentById.get(key) ?? null;
  }
}

async function emitMenuReload(
  req: any,
  container: AwilixContainer<Cradle>,
  ids: (string | number)[],
) {
  const eventEmitter =
    req.scope?.cradle?.eventEmitter ?? container.cradle.eventEmitter;
  const payload: TCacheInvalidationPayload = {
    table: 'enfyra_menu',
    action: 'reload',
    timestamp: Date.now(),
    scope: ids.length ? 'partial' : 'full',
    ids,
  };

  if (typeof eventEmitter.emitAsync === 'function') {
    await eventEmitter.emitAsync(CACHE_EVENTS.INVALIDATE, payload);
    return;
  }
  eventEmitter.emit(CACHE_EVENTS.INVALIDATE, payload);
}

function startReload(
  res: Response,
  label: string,
  reload: () => Promise<void>,
) {
  void Promise.resolve()
    .then(reload)
    .catch((error) => {
      console.error(`Error during ${label} reload:`, error);
    });
  res.status?.(202);
  res.json({
    success: true,
    status: 'accepted',
    message: `${label} reload started`,
  });
}

export function registerAdminRoutes(
  app: Express,
  container: AwilixContainer<Cradle>,
) {
  app.post('/admin/script/validate', async (req: any, res: Response) => {
    const body = req.routeData?.context?.$body ?? req.body ?? {};
    const sourceCode = String(body.sourceCode ?? body.code ?? '');
    const scriptLanguage = body.scriptLanguage ?? 'typescript';
    try {
      const compiledCode = compileScriptSource(sourceCode, scriptLanguage);
      const AsyncFunction = Object.getPrototypeOf(
        async function () {},
      ).constructor;
      new AsyncFunction('$ctx', compiledCode || '');
      res.json({
        success: true,
        valid: true,
        data: {
          compiledCode,
          scriptLanguage,
        },
      });
    } catch (error: any) {
      res.status(400).json({
        success: false,
        valid: false,
        error: {
          code: error?.errorCode || error?.code || 'SCRIPT_VALIDATION_FAILED',
          message: error?.message || 'Script validation failed',
          details: error?.details,
        },
      });
    }
  });

  app.post('/admin/menu/reorder', async (req: any, res: Response) => {
    const updates = normalizeMenuReorderUpdates(
      req.routeData?.context?.$body ?? req.body ?? {},
    );
    if (updates.length === 0) {
      res.json({ success: true, data: { updated: 0 } });
      return;
    }

    const queryBuilderService =
      req.scope?.cradle?.queryBuilderService ??
      container.cradle.queryBuilderService;
    const pkField = queryBuilderService.getPkField();
    const ids = updates.map((update) => update.id);
    const existingResult = await queryBuilderService.find({
      table: 'enfyra_menu',
      fields: [pkField, 'type', 'path', 'isSystem', 'parent.*'],
      limit: 0,
    });

    const existingById = new Map<string, any>();
    const parentById = new Map<string, string | number | null>();
    for (const record of existingResult?.data ?? []) {
      const recordId = getRecordId(record, pkField);
      if (recordId == null) continue;
      existingById.set(String(recordId), record);
      parentById.set(String(recordId), getRecordId(record.parent, pkField));
    }

    for (const update of updates) {
      parentById.set(String(update.id), update.parent ?? null);
    }

    for (const update of updates) {
      const existing = existingById.get(String(update.id));
      if (!existing) {
        throw new BadRequestException(`Menu not found: ${String(update.id)}`);
      }

      if (update.parent != null) {
        if (isSameId(update.id, update.parent)) {
          throw new BadRequestException('Menu cannot be its own parent');
        }

        const parent = existingById.get(String(update.parent));
        if (!parent) {
          throw new BadRequestException(
            `Menu parent not found: ${String(update.parent)}`,
          );
        }
        if (parent.type !== 'Dropdown Menu') {
          throw new BadRequestException('Menu parent must be a dropdown menu');
        }
        if (parent.path === '/data') {
          throw new BadRequestException(
            'The Data menu cannot accept child menus',
          );
        }
      }

      assertNoMenuCycle(update.id, update.parent ?? null, parentById);

      if (existing.isSystem) {
        const existingParentId = getRecordId(existing.parent, pkField);
        if (String(existingParentId ?? '') !== String(update.parent ?? '')) {
          throw new BadRequestException('Cannot change menu parent reference');
        }
      }
    }

    const persistedIds: (string | number)[] = [];
    try {
      for (const update of updates) {
        await queryBuilderService.update('enfyra_menu', update.id, {
          order: update.order,
          parent: update.parent,
        });
        persistedIds.push(update.id);
      }
    } catch (error) {
      if (persistedIds.length > 0) {
        await emitMenuReload(req, container, persistedIds);
      }
      throw error;
    }

    await emitMenuReload(req, container, ids);

    res.json({
      success: true,
      data: {
        updated: updates.length,
        ids,
      },
    });
  });

  app.get('/admin/redis/overview', async (req: any, res: Response) => {
    const redisAdminService = resolveRedisAdmin(req, container);
    res.json({
      success: true,
      data: await redisAdminService.getOverview(),
    });
  });

  app.get('/admin/redis/keys', async (req: any, res: Response) => {
    const redisAdminService = resolveRedisAdmin(req, container);
    res.json({
      success: true,
      data: await redisAdminService.listKeys({
        cursor: req.query?.cursor,
        pattern: req.query?.pattern,
        count: req.query?.count,
      }),
    });
  });

  app.get('/admin/redis/key', async (req: any, res: Response) => {
    const redisAdminService = resolveRedisAdmin(req, container);
    res.json({
      success: true,
      data: await redisAdminService.getKey(String(req.query?.key || ''), {
        limit: req.query?.limit,
      }),
    });
  });

  app.post('/admin/redis/key', async (req: any, res: Response) => {
    const redisAdminService = resolveRedisAdmin(req, container);
    const runtimeMonitorService = resolveRuntimeMonitor(req, container);
    const data = await redisAdminService.setKey(req.body || {});
    runtimeMonitorService.emitRedisKeyChanged({
      operation: 'set',
      key: data.key,
      detail: data,
    });
    await runtimeMonitorService.emitRedisOverview();
    res.json({
      success: true,
      data,
    });
  });

  app.delete('/admin/redis/key', async (req: any, res: Response) => {
    const redisAdminService = resolveRedisAdmin(req, container);
    const runtimeMonitorService = resolveRuntimeMonitor(req, container);
    const key = String(req.query?.key || '');
    const data = await redisAdminService.deleteKey(key);
    runtimeMonitorService.emitRedisKeyChanged({
      operation: 'delete',
      key,
      deleted: data.deleted,
    });
    await runtimeMonitorService.emitRedisOverview();
    res.json({
      success: true,
      data,
    });
  });

  app.patch('/admin/redis/key/ttl', async (req: any, res: Response) => {
    const redisAdminService = resolveRedisAdmin(req, container);
    const runtimeMonitorService = resolveRuntimeMonitor(req, container);
    const data = await redisAdminService.expireKey(
      String(req.body?.key || ''),
      req.body?.ttlSeconds ?? null,
    );
    runtimeMonitorService.emitRedisKeyChanged({
      operation: 'ttl',
      key: data.key,
      summary: data,
    });
    await runtimeMonitorService.emitRedisOverview();
    res.json({
      success: true,
      data,
    });
  });

  app.post('/admin/reload', async (req: any, res: Response) => {
    const orchestrator = resolveOrchestrator(req, container);
    startReload(res, 'All cache', () => orchestrator.reloadAll());
  });

  app.post('/admin/reload/metadata', async (req: any, res: Response) => {
    const orchestrator = resolveOrchestrator(req, container);
    startReload(res, 'Metadata cache', () =>
      orchestrator.reloadMetadataAndDeps(),
    );
  });

  app.post('/admin/reload/routes', async (req: any, res: Response) => {
    const orchestrator = resolveOrchestrator(req, container);
    startReload(res, 'Route cache', () => orchestrator.reloadRoutesOnly());
  });

  app.post('/admin/reload/graphql', async (req: any, res: Response) => {
    const orchestrator = resolveOrchestrator(req, container);
    startReload(res, 'GraphQL cache', () => orchestrator.reloadGraphqlOnly());
  });

  app.post('/admin/reload/guards', async (req: any, res: Response) => {
    const orchestrator = resolveOrchestrator(req, container);
    startReload(res, 'Guard cache', () => orchestrator.reloadGuardsOnly());
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

  app.post('/admin/test/run', async (req: any, res: Response) => {
    const result = await runTest(
      req.body,
      req.scope?.cradle ?? container.cradle,
      req.user ?? null,
    );
    res.json(result);
  });
}

async function runTest(body: any, cradle: any, currentUser: any = null) {
  const kind = String(body?.kind || '').trim();

  if (kind === 'flow_step') {
    const flowService = cradle.flowService;
    return flowService.testStep(
      {
        id: body.id ?? body.stepId,
        stepId: body.stepId,
        flowId: body.flowId ?? body.flow?.id,
        flowName: body.flowName ?? body.flow?.name,
        type: body.type,
        config: body.config,
        sourceCode: body.sourceCode,
        scriptLanguage: body.scriptLanguage,
        compiledCode: body.compiledCode,
        timeout: body.timeout,
        key: body.key,
      },
      {
        ...(body.mockFlow || {}),
        $payload:
          body.mockFlow?.$payload ??
          body.payload ??
          body.testPayload ??
          body.body ??
          {},
      },
    );
  }

  if (kind === 'websocket_event') {
    const resolved = await resolveWebsocketEventTest(body, cradle);
    const script = String(resolved.script || '').trim();
    const gatewayPath = resolved.gatewayPath;
    const eventName = resolved.eventName;
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
      const transformed = compileScriptSource(
        script,
        resolved.scriptLanguage ?? body?.scriptLanguage,
      );
      const result = await handlerExecutorService.run(
        transformed || '',
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

  if (kind === 'script') {
    const resolved = await resolveScriptTest(body, cradle);
    const script = String(resolved.script || '').trim();
    const timeoutMs = Number(body?.timeoutMs ?? body?.timeout ?? 5000);
    const tableName = resolved.mainTableName;

    if (!script) {
      return {
        success: false,
        error: { code: 'MISSING_SCRIPT', message: 'script is required' },
      };
    }

    const handlerExecutorService = cradle.executorEngineService;
    const repoRegistryService = cradle.repoRegistryService;

    const ctx = createHttpLikeTestContext({
      body,
      cradle,
      currentUser,
      route: resolved.route,
      mainTableName: tableName,
    });
    ctx.$repos = repoRegistryService.createReposProxy(ctx, tableName);

    try {
      const transformed = compileScriptSource(
        script,
        resolved.scriptLanguage ?? body?.scriptLanguage,
      );
      const result = await handlerExecutorService.run(
        transformed || '',
        ctx,
        timeoutMs,
      );
      return {
        success: true,
        result,
        logs: ctx.$share?.$logs?.length ? ctx.$share.$logs : [],
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
      };
    }
  }

  if (kind === 'websocket_connection') {
    const resolved = await resolveWebsocketConnectionTest(body, cradle);
    const script = String(resolved.script || '').trim();
    const gatewayPath = resolved.gatewayPath;
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
      const transformed = compileScriptSource(
        script,
        resolved.scriptLanguage ?? body?.scriptLanguage,
      );
      const result = await handlerExecutorService.run(
        transformed || '',
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

async function resolveScriptTest(body: any, cradle: any) {
  const tableName = String(body?.tableName || body?.table || '').trim();
  const recordId =
    body?.id ?? body?.recordId ?? body?.handlerId ?? body?.hookId;
  const routeId = body?.routeId ?? body?.route?.id;
  const method = String(body?.method || '')
    .trim()
    .toUpperCase();

  let record: any = null;
  let route: any = null;
  let mainTableName = tableName || undefined;

  if (routeId || body?.path || method) {
    route = await findRouteForTest(body, cradle);
    if (route?.mainTable?.name) mainTableName = route.mainTable.name;
  }

  if (recordId && tableName && cradle.queryBuilderService) {
    const pkField =
      typeof cradle.queryBuilderService.getPkField === 'function'
        ? cradle.queryBuilderService.getPkField()
        : 'id';
    record = await cradle.queryBuilderService.findOne({
      table: tableName,
      where: { [pkField]: recordId },
    });
  }

  if (!record && route) {
    record = findRouteScriptRecord({ body, route, tableName, method });
  }

  return {
    script:
      body?.script ??
      body?.sourceCode ??
      record?.sourceCode ??
      record?.logic ??
      record?.code ??
      record?.compiledCode ??
      '',
    scriptLanguage:
      body?.scriptLanguage ?? record?.scriptLanguage ?? 'typescript',
    route,
    mainTableName,
  };
}

async function resolveWebsocketEventTest(body: any, cradle: any) {
  const gateway = await findWebsocketGatewayForTest(body, cradle);
  const eventId = body?.id ?? body?.eventId ?? body?.recordId;
  const eventName = String(body?.eventName || body?.name || '').trim();
  const event = gateway?.events?.find((candidate: any) => {
    if (eventId !== undefined && String(candidate.id) === String(eventId)) {
      return true;
    }
    return !!eventName && candidate.name === eventName;
  });

  return {
    gatewayPath: String(
      body?.gatewayPath || body?.path || gateway?.path || '/__ws_test__',
    ).trim(),
    eventName: String(eventName || event?.name || '').trim(),
    script:
      body?.script ??
      body?.sourceCode ??
      event?.handlerScript ??
      event?.sourceCode ??
      event?.compiledCode ??
      '',
    scriptLanguage: body?.scriptLanguage ?? event?.scriptLanguage,
  };
}

async function resolveWebsocketConnectionTest(body: any, cradle: any) {
  const gateway = await findWebsocketGatewayForTest(body, cradle);
  return {
    gatewayPath: String(
      body?.gatewayPath || body?.path || gateway?.path || '/__ws_test__',
    ).trim(),
    script:
      body?.script ??
      body?.sourceCode ??
      gateway?.connectionHandlerScript ??
      gateway?.sourceCode ??
      gateway?.compiledCode ??
      '',
    scriptLanguage: body?.scriptLanguage ?? gateway?.scriptLanguage,
  };
}

async function findRouteForTest(body: any, cradle: any) {
  const routes = cradle.runtimeRegistryService?.getRoutes?.() ?? [];
  const routeId = body?.routeId ?? body?.route?.id;
  const path = String(body?.path || body?.routePath || '').trim();
  return (
    routes.find((route: any) => {
      if (routeId !== undefined && String(route.id) === String(routeId)) {
        return true;
      }
      return !!path && route.path === path;
    }) || null
  );
}

function findRouteScriptRecord(options: {
  body: any;
  route: any;
  tableName?: string;
  method?: string;
}) {
  const { body, route, tableName, method } = options;
  const recordId =
    body?.id ?? body?.recordId ?? body?.handlerId ?? body?.hookId;
  const source = String(tableName || body?.source || body?.target || '').trim();

  const pools =
    source === 'enfyra_pre_hook'
      ? [route.preHooks || []]
      : source === 'enfyra_post_hook'
        ? [route.postHooks || []]
        : source === 'enfyra_route_handler'
          ? [route.handlers || []]
          : [route.handlers || [], route.preHooks || [], route.postHooks || []];

  for (const pool of pools) {
    const found = pool.find((record: any) => {
      if (recordId !== undefined && String(record.id) === String(recordId)) {
        return true;
      }
      if (method && record.method?.name === method) return true;
      return false;
    });
    if (found) return found;
  }

  return null;
}

async function findWebsocketGatewayForTest(body: any, cradle: any) {
  const gateways =
    cradle.runtimeRegistryService?.getActiveData?.(
      CACHE_IDENTIFIERS.WEBSOCKET,
    ) ?? [];
  const gatewayId = body?.gatewayId ?? body?.gateway?.id;
  const path = String(body?.gatewayPath || body?.path || '').trim();
  return (
    gateways.find((gateway: any) => {
      if (gatewayId !== undefined && String(gateway.id) === String(gatewayId)) {
        return true;
      }
      return !!path && gateway.path === path;
    }) || null
  );
}

function createHttpLikeTestContext(options: {
  body: any;
  cradle: any;
  currentUser: any;
  route?: any;
  mainTableName?: string;
}) {
  const { body, cradle, currentUser, route } = options;
  const dynamicContextFactory = cradle.dynamicContextFactory;
  const method = String(body?.method || 'POST').toUpperCase();
  const path = String(
    body?.path || body?.routePath || route?.path || '/__test__',
  );
  const requestBody = body?.body ?? body?.payload ?? {};
  const query = body?.query ?? {};
  const params = body?.params ?? {};
  const req = {
    method,
    url: path,
    body: requestBody,
    query,
    params,
    user: body?.user ?? currentUser,
    headers: body?.headers ?? {},
    hostname: 'localhost',
    protocol: 'http',
    path,
    originalUrl: path,
  };

  const ctx = dynamicContextFactory.createHttp(req, {
    params,
    realClientIP: body?.ip ?? '127.0.0.1',
  });
  ctx.$data = body?.data;
  ctx.$statusCode = body?.statusCode;
  ctx.$trigger = (flowIdOrName: string | number, payload?: any) =>
    cradle.flowService.trigger(flowIdOrName, payload, req.user);

  return ctx;
}
