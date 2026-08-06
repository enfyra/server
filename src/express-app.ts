/**
 * Express app assembly — thin facade.
 *
 * Middleware grouping lives in `src/http/pipelines/middleware-pipelines.ts`.
 * Route mounting order and pipeline order are still defined here explicitly.
 */
import express from 'express';
import cors from 'cors';
import qs from 'qs';
import type { AwilixContainer } from 'awilix';
import { buildRequestScope, type Cradle } from './container';
import { globalExceptionMiddleware } from './domain/exceptions';

import { captureRawBody } from './http/utils/raw-body-capture.util';
import {
  pipelinePreRouting,
  pipelineGuardsAuth,
  pipelineRequestHandling,
  pipelineTail,
} from './http/pipelines/middleware-pipelines';

import { registerAuthRoutes } from './http/routes/auth.routes';
import { registerOAuthRoutes } from './http/routes/oauth.routes';
import { registerAdminRoutes } from './http/routes/admin.routes';
import { registerLogRoutes } from './http/routes/log.routes';
import { registerMetadataRoutes } from './http/routes/metadata.routes';
import { registerExtensionRoutes } from './http/routes/extension.routes';
import { registerAssetsRoutes } from './http/routes/assets.routes';
import { registerFileRoutes } from './http/routes/file.routes';
import { registerFolderRoutes } from './http/routes/folder.routes';
import { registerGraphqlSchemaRoutes } from './http/routes/graphql-schema.routes';
import { registerPackageRoutes } from './http/routes/package.routes';
import { registerMeRoutes } from './http/routes/me.routes';
import { registerDynamicRoutes } from './http/routes/dynamic.routes';
import { DebugTrace } from './shared/utils/debug-trace.util';
import { resolveClientIpFromRequest } from './shared/utils/client-ip.util';

export function disposeRequestScopeOnResponse(req: any, res: any): void {
  let disposed = false;
  const dispose = () => {
    if (disposed) return;
    disposed = true;
    Promise.resolve(req.scope?.dispose?.()).catch((error) => {
      console.error('Request scope dispose failed:', error);
    });
  };
  res.once('finish', dispose);
  res.once('close', dispose);
}

export function buildExpressApp(container: AwilixContainer<Cradle>) {
  const app = express();
  app.set('query parser', (str: string) => {
    return qs.parse(str, {
      allowPrototypes: false,
      depth: 10,
      parameterLimit: 1000,
      strictNullHandling: false,
      arrayLimit: 200,
    });
  });

  // ── Foundation ────────────────────────────────────────────────────
  app.use(cors({ origin: true, credentials: true }));
  app.use(express.json({ limit: '10mb', verify: captureRawBody }));
  app.use(express.urlencoded({ extended: true }));
  app.use(express.text({ type: 'text/plain' }));

  // ── Request scope + debug trace ──────────────────────────────────
  app.use((req: any, res, next) => {
    const start = performance.now();
    req._perfStart = start;
    const debugMode = req.query?.debugMode === 'true' || req.query?.debugMode === true;
    if (debugMode) {
      req._debug = new DebugTrace();
      req._debug.dur('mw_scope_create', start);
    }
    req.scope = buildRequestScope(container, req, res);
    disposeRequestScopeOnResponse(req, res);
    next();
  });

  const c = container.cradle;

  // ── Activation gate (503 while schema reload pending) ─────────────
  app.use((_req, res, next) => {
    if (!c.runtimeSchemaActivationGateService?.isBlocked?.()) {
      next();
      return;
    }
    res.status(503).json({
      message: 'Runtime schema activation is pending; this instance is not ready',
    });
  });

  // ── Metrics (runWithQueryContext) ─────────────────────────────────
  app.use((req: any, res, next) => {
    const startedAt = performance.now();
    res.on('finish', () => {
      const route =
        req.routeData?.path ||
        req.route?.path ||
        req.path ||
        req.originalUrl?.split('?')?.[0] ||
        'unknown';
      c.runtimeMetricsCollectorService.recordRequest({
        method: req.method,
        route,
        statusCode: res.statusCode,
        durationMs: performance.now() - startedAt,
      });
    });
    c.runtimeMetricsCollectorService.runWithQueryContext('runtime', async () => next()).catch(next);
  });

  // ── Pipelines (order matters — see skill `enfyra-http-pipeline`) ──
  for (const mw of pipelinePreRouting(c, container)) app.use(mw);
  for (const mw of pipelineGuardsAuth(c)) app.use(mw);
  for (const mw of pipelineRequestHandling(c, container)) app.use(mw);

  // ── Built-in routes ───────────────────────────────────────────────
  registerAuthRoutes(app, container);
  registerOAuthRoutes(app, container);
  registerAdminRoutes(app, container);
  registerLogRoutes(app, container);
  registerMetadataRoutes(app, container);
  registerExtensionRoutes(app, container);
  registerAssetsRoutes(app, container);
  registerFileRoutes(app, container);
  registerFolderRoutes(app, container);
  registerGraphqlSchemaRoutes(app, container);
  registerPackageRoutes(app, container);
  registerMeRoutes(app, container);

  // ── GraphQL ───────────────────────────────────────────────────────
  c.graphqlService.getYogaApp();
  app.use('/graphql', (req: any, res: any) => {
    const yogaApp = c.graphqlService.getYogaApp();
    return yogaApp(req, res, {
      clientIp: resolveClientIpFromRequest(req),
      auth: req.auth ?? null,
    });
  });

  // ── Dynamic catch-all ─────────────────────────────────────────────
  registerDynamicRoutes(app, container);

  // ── Tail ──────────────────────────────────────────────────────────
  for (const mw of pipelineTail()) app.use(mw);
  app.use(globalExceptionMiddleware);

  return app;
}
