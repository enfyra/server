/**
 * Named middleware pipelines — groups the raw app.use(...) calls in express-app.ts
 * into readable blocks. Order is still defined in express-app.ts; this file only
 * names the groups and wires their dependencies from the container.
 */
import { routeDetectMiddleware } from '../middlewares/route-detect.middleware';
import { notFoundDetectMiddleware } from '../middlewares/not-found-detect.middleware';
import { preAuthMetadataGuard, postAuthMetadataGuard } from '../middlewares/metadata-guard.middleware';
import { authMiddleware } from '../middlewares/auth.middleware';
import { roleGuardMiddleware } from '../middlewares/role-guard.middleware';
import { requestLoggingBegin, requestLoggingEnd } from '../middlewares/request-logging.middleware';
import { bodyValidationMiddleware } from '../middlewares/body-validation.middleware';
import { dynamicInterceptorBegin, dynamicInterceptorEnd } from '../middlewares/dynamic-interceptor.middleware';
import { parseQueryMiddleware } from '../middlewares/parse-query.middleware';
import { fileUploadMiddleware } from '../middlewares/file-upload.middleware';
import type { AwilixContainer } from 'awilix';
import type { Cradle } from '../../container';

export function pipelinePreRouting(cradle: Cradle, container: AwilixContainer<Cradle>) {
  return [
    parseQueryMiddleware,
    // perf marker before routeDetect
    ((req: any, _res: any, next: any) => {
      req._perfRouteDetect = performance.now();
      next();
    }) as any,
    routeDetectMiddleware(
      cradle.runtimeRegistryService,
      cradle.repoRegistryService,
      cradle.uploadFileHelper,
      cradle.rateLimitService,
      cradle.flowService,
      cradle.dynamicContextFactory,
    ),
    ((req: any, _res: any, next: any) => {
      if (req._debug) req._debug.dur('mw_route_detect', req._perfRouteDetect);
      req._perfAuth = performance.now();
      next();
    }) as any,
    notFoundDetectMiddleware,
  ];
}

export function pipelineGuardsAuth(cradle: Cradle) {
  return [
    preAuthMetadataGuard(
      cradle.guardCacheBuilder,
      cradle.runtimeRegistryService,
      cradle.guardEvaluatorService,
      cradle.guardAlertService,
    ),
    authMiddleware(cradle.authenticationService),
    ((req: any, _res: any, next: any) => {
      if (req._debug) req._debug.dur('mw_auth', req._perfAuth);
      next();
    }) as any,
    roleGuardMiddleware(cradle.policyService),
    postAuthMetadataGuard(
      cradle.guardCacheBuilder,
      cradle.runtimeRegistryService,
      cradle.guardEvaluatorService,
      cradle.guardAlertService,
    ),
  ];
}

export function pipelineRequestHandling(cradle: Cradle, container: AwilixContainer<Cradle>) {
  return [
    fileUploadMiddleware(cradle.runtimeRegistryService, cradle.dynamicWebSocketGateway),
    requestLoggingBegin,
    bodyValidationMiddleware(container),
    dynamicInterceptorBegin(cradle.executorEngineService, cradle.runtimeScriptRepairService),
  ];
}

export function pipelineTail() {
  return [dynamicInterceptorEnd, requestLoggingEnd];
}
