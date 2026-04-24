import { Response, NextFunction } from 'express';
import { RouteCacheService } from '../../engine/cache/services/route-cache.service';
import { RepoRegistryService } from '../../engine/cache/services/repo-registry.service';
import { UploadFileHelper } from '../../shared/helpers/upload-file.helper';
import { RateLimitService } from '../../engine/cache/services/rate-limit.service';
import { FlowService } from '../../modules/flow/services/flow.service';
import { resolveClientIpFromRequest } from '../../shared/utils/client-ip.util';
import { DynamicContextFactory } from '../../shared/services/dynamic-context.factory';

export function routeDetectMiddleware(
  routeCacheService: RouteCacheService,
  repoRegistryService: RepoRegistryService,
  uploadFileHelper: UploadFileHelper,
  rateLimitService: RateLimitService,
  flowService: FlowService,
  dynamicContextFactory: DynamicContextFactory,
) {
  return async (req: any, res: Response, next: NextFunction) => {
    const method = req.method;
    const routeEngine = routeCacheService.getRouteEngine();
    const path = req.path || req.url?.split('?')[0] || '/';
    const matchedRoute = routeEngine.find(method, path);

    const isMethodAvailable = (route: any) => {
      const methods = route?.availableMethods;
      if (!methods || !Array.isArray(methods) || methods.length === 0)
        return false;
      const methodNames = methods
        .map((m: any) => m?.method ?? m)
        .filter(Boolean);
      return methodNames.includes(method);
    };

    if (matchedRoute && isMethodAvailable(matchedRoute.route)) {
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
          const userId = req.user?.id || 'anonymous';
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
          const userId = req.user?.id || 'anonymous';
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
          buffer: req.file.buffer,
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
        context.$helpers.$uploadFile =
          uploadFileHelper.createUploadFileHelper(context);
        context.$helpers.$updateFile =
          uploadFileHelper.createUpdateFileHelper(context);
        context.$helpers.$deleteFile =
          uploadFileHelper.createDeleteFileHelper(context);
      } catch (error) {
        console.warn('Failed to initialize file helpers:', error);
      }

      const { route, params } = matchedRoute;

      const filterHooks = (hooks: any[]) => {
        if (!hooks || !Array.isArray(hooks)) return [];
        return hooks.filter((hook: any) => {
          const methodList = hook.methods?.map((m: any) => m.method) ?? [];
          return methodList.includes(method);
        });
      };

      const filteredPreHooks = filterHooks(route.preHooks);
      const filteredPostHooks = filterHooks(route.postHooks);
      const routeHandlers = Array.isArray(route.handlers) ? route.handlers : [];
      const handler =
        routeHandlers.find((handler: any) => handler.method?.method === method)
          ?.logic ?? null;

      req.routeData = {
        ...route,
        handlers: routeHandlers,
        handler,
        params,
        preHooks: filteredPreHooks,
        postHooks: filteredPostHooks,
        isPublished:
          route.publishedMethods?.some(
            (pubMethod: any) => pubMethod.method === req.method,
          ) || false,
        context,
        res,
      };
    }

    next();
  };
}
