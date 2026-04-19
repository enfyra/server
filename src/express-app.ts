import express from 'express';
import cors from 'cors';
import type { AwilixContainer } from 'awilix';
import type { Cradle } from './container';
import { buildRequestScope } from './container';
import { globalExceptionMiddleware } from './core/exceptions/filters/global-exception.filter';

import { routeDetectMiddleware } from './http/middleware/route-detect.middleware';
import { notFoundDetectMiddleware } from './http/middleware/not-found-detect.middleware';
import { preAuthMetadataGuard, postAuthMetadataGuard } from './http/middleware/metadata-guard.middleware';
import { jwtAuthMiddleware } from './http/middleware/jwt-auth.middleware';
import { roleGuardMiddleware } from './http/middleware/role-guard.middleware';
import { requestLoggingBegin, requestLoggingEnd } from './http/middleware/request-logging.middleware';
import { dynamicInterceptorBegin, dynamicInterceptorEnd } from './http/middleware/dynamic-interceptor.middleware';
import { parseQueryMiddleware } from './http/middleware/parse-query.middleware';
import { bodyParserMiddleware } from './http/middleware/body-parser.middleware';
import { fileUploadMiddleware } from './http/middleware/file-upload.middleware';

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

export function buildExpressApp(container: AwilixContainer<Cradle>) {
  const app = express();
  const qs = require('qs');
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
    req.scope = buildRequestScope(container, req, res);
    next();
  });

  const c = container.cradle;

  app.use(bodyParserMiddleware(c.settingCacheService));
  app.use(parseQueryMiddleware);
  app.use(fileUploadMiddleware(c.settingCacheService));
  app.use(
    routeDetectMiddleware(
      c.envService.get('SECRET_KEY'),
      c.routeCacheService,
      c.repoRegistryService,
      c.cacheService,
      c.bcryptService,
      c.uploadFileHelper,
      c.dynamicWebSocketGateway,
      c.rateLimitService,
      c.flowService,
    ),
  );
  app.use(notFoundDetectMiddleware);
  app.use(preAuthMetadataGuard(c.guardCacheService, c.guardEvaluatorService));
  app.use(jwtAuthMiddleware(c.queryBuilderService, c.cacheService, c.envService.get('SECRET_KEY')));
  app.use(roleGuardMiddleware(c.policyService));
  app.use(postAuthMetadataGuard(c.guardCacheService, c.guardEvaluatorService));
  app.use(requestLoggingBegin);
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

  try {
    const graphqlService = c.graphqlService;
    app.use('/graphql', (req: any, res: any, next: any) => {
      return graphqlService.getYogaApp()(req, res, next);
    });
  } catch (error: any) {
    console.warn('GraphQL endpoint not available:', error.message);
  }

  registerDynamicRoutes(app, container);

  app.use(dynamicInterceptorEnd);
  app.use(requestLoggingEnd);

  app.use((req: any, res, next) => {
    req.scope?.dispose?.();
    next();
  });

  app.use(globalExceptionMiddleware);

  return app;
}
