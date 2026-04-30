import express from 'express';
import cors from 'cors';
import qs from 'qs';
import type { AwilixContainer } from 'awilix';
import { buildRequestScope, type Cradle } from './container';
import { globalExceptionMiddleware } from './domain/exceptions';

import { routeDetectMiddleware } from './http/middlewares/route-detect.middleware';
import { notFoundDetectMiddleware } from './http/middlewares/not-found-detect.middleware';
import {
  preAuthMetadataGuard,
  postAuthMetadataGuard,
} from './http/middlewares/metadata-guard.middleware';
import { jwtAuthMiddleware } from './http/middlewares/jwt-auth.middleware';
import { roleGuardMiddleware } from './http/middlewares/role-guard.middleware';
import {
  requestLoggingBegin,
  requestLoggingEnd,
} from './http/middlewares/request-logging.middleware';
import { bodyValidationMiddleware } from './http/middlewares/body-validation.middleware';
import {
  dynamicInterceptorBegin,
  dynamicInterceptorEnd,
} from './http/middlewares/dynamic-interceptor.middleware';
import { parseQueryMiddleware } from './http/middlewares/parse-query.middleware';
import { bodyParserMiddleware } from './http/middlewares/body-parser.middleware';
import { fileUploadMiddleware } from './http/middlewares/file-upload.middleware';

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

  app.use(cors({ origin: true, credentials: true }));
  app.use(express.json({ limit: '10mb' }));
  app.use(express.urlencoded({ extended: true }));
  app.use(express.text({ type: 'text/plain' }));

  app.use((req: any, res, next) => {
    const start = performance.now();
    req._perfStart = start;
    const debugMode =
      req.query?.debugMode === 'true' || req.query?.debugMode === true;
    if (debugMode) {
      req._debug = new DebugTrace();
      req._debug.dur('mw_scope_create', start);
    }
    req.scope = buildRequestScope(container, req, res);
    disposeRequestScopeOnResponse(req, res);
    next();
  });

  const c = container.cradle;

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
    c.runtimeMetricsCollectorService
      .runWithQueryContext('runtime', async () => next())
      .catch(next);
  });

  app.use(bodyParserMiddleware(c.settingCacheService));
  app.use(parseQueryMiddleware);
  app.use(fileUploadMiddleware(c.settingCacheService));
  app.use((req: any, _res: any, next: any) => {
    req._perfRouteDetect = performance.now();
    next();
  });
  app.use(
    routeDetectMiddleware(
      c.routeCacheService,
      c.repoRegistryService,
      c.uploadFileHelper,
      c.rateLimitService,
      c.flowService,
      c.dynamicContextFactory,
    ),
  );
  app.use((req: any, _res: any, next: any) => {
    if (req._debug) req._debug.dur('mw_route_detect', req._perfRouteDetect);
    req._perfJwt = performance.now();
    next();
  });
  app.use(notFoundDetectMiddleware);
  app.use(preAuthMetadataGuard(c.guardCacheService, c.guardEvaluatorService));
  app.use(
    jwtAuthMiddleware(
      c.queryBuilderService,
      c.cacheService,
      c.envService.get('SECRET_KEY'),
    ),
  );
  app.use((req: any, _res: any, next: any) => {
    if (req._debug) req._debug.dur('mw_jwt_auth', req._perfJwt);
    next();
  });
  app.use(roleGuardMiddleware(c.policyService));
  app.use(postAuthMetadataGuard(c.guardCacheService, c.guardEvaluatorService));
  app.use(requestLoggingBegin);
  app.use(bodyValidationMiddleware(container));
  app.use(dynamicInterceptorBegin(c.executorEngineService));

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

  app.use('/graphql', (req: any, res: any, next: any) => {
    const yogaApp = c.graphqlService.getYogaApp();
    return yogaApp(req, res, next);
  });

  registerDynamicRoutes(app, container);

  app.use(dynamicInterceptorEnd);
  app.use(requestLoggingEnd);

  app.use(globalExceptionMiddleware);

  return app;
}
