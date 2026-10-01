import { Response, NextFunction } from 'express';
import { RepoRegistryService, RateLimitService } from '../../engines/cache';
import { UploadFileHelper } from '../../shared/helpers';
import { FlowService } from '../../modules/flow';
import { resolveClientIpFromRequest } from '../../shared/utils/client-ip.util';
import { DynamicContextFactory } from '../../shared/services';
import { matchRouteInRoutes } from '../../shared/utils/route-match.util';
import type { RuntimeRegistryService } from '../../engines/cache/services/runtime-registry.service';

export function routeDetectMiddleware(
  runtimeRegistryService: RuntimeRegistryService,
  repoRegistryService: RepoRegistryService,
  uploadFileHelper: UploadFileHelper,
  rateLimitService: RateLimitService,
  flowService: FlowService,
  dynamicContextFactory: DynamicContextFactory,
) {
  return async (req: any, res: Response, next: NextFunction) => {
    const method = req.method;
    const path = req.path || req.url?.split('?')[0] || '/';
    const matchedRoute = matchRouteInRoutes(
      runtimeRegistryService.requireRoutes(),
      method,
      path,
    );

    const findRouteMethodConfig = (route: any) => {
      if (!Array.isArray(route?.methodConfigs)) return null;
      return (
        route.methodConfigs.find(
          (config: any) =>
            config?.available === true &&
            (config.method?.name ?? config.method) === method,
        ) ?? null
      );
    };

    if (matchedRoute) {
      const routeMethodConfig = findRouteMethodConfig(matchedRoute.route);
      if (!routeMethodConfig) return next();
      const realClientIP = resolveClientIpFromRequest(req);
      const context = dynamicContextFactory.createHttp(req, {
        params: matchedRoute.params ?? {},
        realClientIP,
      });

      const routePath = matchedRoute.route.path || req.baseUrl;
      const createRateLimitHelper = () => {
        const check = async (
          key: string,
          options: { maxRequests: number; perSeconds: number },
        ) => {
          return rateLimitService.check(key, options);
        };

        const byIp = async (options: {
          maxRequests: number;
          perSeconds: number;
        }) => {
          const key = `ip:${realClientIP}:${routePath}`;
          return check(key, options);
        };

        const byUser = async (options: {
          maxRequests: number;
          perSeconds: number;
        }) => {
          const userId = req.user?.id ?? req.user?._id ?? 'anonymous';
          const key = `user:${userId}:${routePath}`;
          return check(key, options);
        };

        const byRoute = async (options: {
          maxRequests: number;
          perSeconds: number;
        }) => {
          const key = `route:${routePath}`;
          return check(key, options);
        };

        const byIpGlobal = async (options: {
          maxRequests: number;
          perSeconds: number;
        }) => {
          const key = `ip:${realClientIP}`;
          return check(key, options);
        };

        const byUserGlobal = async (options: {
          maxRequests: number;
          perSeconds: number;
        }) => {
          const userId = req.user?.id ?? req.user?._id ?? 'anonymous';
          const key = `user:${userId}`;
          return check(key, options);
        };

        const reset = async (key: string) => {
          return rateLimitService.reset(key);
        };

        const status = async (
          key: string,
          options: { maxRequests: number; perSeconds: number },
        ) => {
          return rateLimitService.status(key, options);
        };

        return {
          check,
          byIp,
          byUser,
          byRoute,
          byIpGlobal,
          byUserGlobal,
          reset,
          status,
        };
      };

      context.$helpers.$rateLimit = createRateLimitHelper() as any;
      if (req.file) {
        context.$uploadedFile = {
          originalname: req.file.originalname,
          mimetype: req.file.mimetype,
          encoding: req.file.encoding || 'utf8',
          path: req.file.path,
          size: req.file.size,
          fieldname: req.file.fieldname,
        };
      }

      const mainTableName = matchedRoute.route.mainTable?.name;
      context.$repos = repoRegistryService.createReposProxy(
        context,
        mainTableName,
      );

      context.$trigger = (flowIdOrName: string | number, payload?: any) =>
        flowService.trigger(flowIdOrName, payload, req.user);

      try {
        context.$storage = uploadFileHelper.createStorageHelper(context);
      } catch (error) {
        console.warn('Failed to initialize storage helpers:', error);
      }

      const { route, params } = matchedRoute;
      const handlerRecord = routeMethodConfig.handler ?? null;
      const handler = handlerRecord?.logic ?? null;

      req.routeData = {
        ...route,
        routeMethodConfig,
        routePermissions: routeMethodConfig.routePermissions ?? [],
        handlerRecord,
        handler,
        params,
        preHooks: routeMethodConfig.preHooks ?? [],
        postHooks: routeMethodConfig.postHooks ?? [],
        isPublic: routeMethodConfig.isPublic === true,
        context,
        res,
      };
    }

    next();
  };
}
