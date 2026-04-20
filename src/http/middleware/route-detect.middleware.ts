import { Request, Response, NextFunction } from 'express';
import * as jwt from 'jsonwebtoken';
import { TDynamicContext } from '../../shared/types';
import { RouteCacheService } from '../../infrastructure/cache/services/route-cache.service';
import { RepoRegistryService } from '../../infrastructure/cache/services/repo-registry.service';
import { BcryptService } from '../../core/auth/services/bcrypt.service';
import { ScriptErrorFactory } from '../../shared/utils/script-error-factory';
import { autoSlug } from '../../shared/utils/auto-slug.helper';
import { CacheService } from '../../infrastructure/cache/services/cache.service';
import { UploadFileHelper } from '../../shared/helpers/upload-file.helper';
import { createFetchHelper } from '../../shared/helpers/fetch.helper';
import { DynamicWebSocketGateway } from '../../modules/websocket/gateway/dynamic-websocket.gateway';
import { RateLimitService } from '../../infrastructure/cache/services/rate-limit.service';
import { FlowService } from '../../modules/flow/services/flow.service';
import { resolveClientIpFromRequest } from '../../shared/utils/client-ip.util';

export function routeDetectMiddleware(
  secretKey: string,
  routeCacheService: RouteCacheService,
  repoRegistryService: RepoRegistryService,
  cacheService: CacheService,
  bcryptService: BcryptService,
  uploadFileHelper: UploadFileHelper,
  dynamicWebSocketGateway: DynamicWebSocketGateway,
  rateLimitService: RateLimitService,
  flowService: FlowService,
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
      const context: TDynamicContext = {
        $body: req.routeData?.context?.$body || req.body || {},
        $throw: ScriptErrorFactory.createThrowHandlers(),
        $helpers: {
          $jwt: (payload: any, exp: string) =>
            jwt.sign(payload, secretKey, {
              expiresIn: exp as import('ms').StringValue,
            }),
          $bcrypt: {
            hash: async (plain: string) => await bcryptService.hash(plain),
            compare: async (p: string, h: string) =>
              await bcryptService.compare(p, h),
          },
          autoSlug: autoSlug,
        },
        $cache: cacheService,
        $params: matchedRoute.params ?? {},
        $query: req.query ?? {},
        $user: req.user ?? null,
        $repos: {},
        $req: {
          method: req.method,
          url: req.url,
          headers: req.headers,
          query: req.query,
          params: req.params,
          ip: realClientIP,
          hostname: req.hostname,
          protocol: req.protocol,
          path: req.path,
          originalUrl: req.originalUrl,
        } as any,
        $share: {
          $logs: [],
        },
        $socket: {
          emitToUser: (userId: any, event: string, data: any) => {
            dynamicWebSocketGateway.emitToUser(userId, event, data);
          },
          emitToRoom: (room: string, event: string, data: any) => {
            dynamicWebSocketGateway.emitToRoom(room, event, data);
          },
          emitToGateway: (path: string, event: string, data: any) => {
            dynamicWebSocketGateway.emitToNamespace(path, event, data);
          },
          broadcast: (event: string, data: any) => {
            dynamicWebSocketGateway.emitToAll(event, data);
          },
          roomSize: async (room: string): Promise<number> =>
            dynamicWebSocketGateway.roomSize(room),
        },
        $api: {
          request: {
            method: req.method,
            url: req.url,
            timestamp: new Date().toISOString(),
            correlationId:
              (req.headers['x-correlation-id'] as string) ||
              generateCorrelationId(),
            userAgent: req.headers['user-agent'],
            ip: realClientIP,
          },
        },
      };

      context.$logs = (...args: any[]) => {
        context.$share.$logs.push(...args);
      };

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
      context.$helpers.$fetch = createFetchHelper();

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

      req.routeData = {
        ...route,
        handler:
          route.handlers.find((handler) => handler.method?.method === method)
            ?.logic ?? null,
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

function generateCorrelationId(): string {
  return `req_${Date.now()}_${Math.random().toString(36).substring(2, 11)}`;
}
