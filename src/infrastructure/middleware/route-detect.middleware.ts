import { Injectable, NestMiddleware, Logger } from '@nestjs/common';
import { JwtService } from '@nestjs/jwt';
import { TDynamicContext } from '../../shared/types';
import { RouteCacheService } from '../cache/services/route-cache.service';
import { RepoRegistryService } from '../cache/services/repo-registry.service';
import { BcryptService } from '../../core/auth/services/bcrypt.service';
import { ScriptErrorFactory } from '../../shared/utils/script-error-factory';
import { autoSlug } from '../../shared/utils/auto-slug.helper';
import { CacheService } from '../cache/services/cache.service';
import { UploadFileHelper } from '../../shared/helpers/upload-file.helper';
import { DynamicWebSocketGateway } from '../../modules/websocket/gateway/dynamic-websocket.gateway';
import { RateLimitService } from '../cache/services/rate-limit.service';
import { FlowService } from '../../modules/flow/services/flow.service';

@Injectable()
export class RouteDetectMiddleware implements NestMiddleware {
  private readonly logger = new Logger(RouteDetectMiddleware.name);

  constructor(
    private jwtService: JwtService,
    private routeCacheService: RouteCacheService,
    private repoRegistryService: RepoRegistryService,
    private cacheService: CacheService,
    private bcryptService: BcryptService,
    private uploadFileHelper: UploadFileHelper,
    private websocketGateway: DynamicWebSocketGateway,
    private rateLimitService: RateLimitService,
    private flowService: FlowService,
  ) {}

  async use(req: any, res: any, next: (error?: any) => void) {
    const method = req.method;
    const routeEngine = this.routeCacheService.getRouteEngine();
    const matchedRoute = routeEngine.find(method, req.baseUrl);

    const isMethodAvailable = (route: any) => {
      const methods = route?.availableMethods;
      if (!methods || !Array.isArray(methods) || methods.length === 0) return false;
      const methodNames = methods.map((m: any) => m?.method ?? m).filter(Boolean);
      return methodNames.includes(method);
    };

    if (matchedRoute && isMethodAvailable(matchedRoute.route)) {
      const realClientIP = this.detectClientIP(req);
      const context: TDynamicContext = {
        $body: req.routeData?.context?.$body || req.body || {},
        $statusCode: undefined,
        $throw: ScriptErrorFactory.createThrowHandlers(),
        $helpers: {
          $jwt: (payload: any, exp: string) =>
            this.jwtService.sign(payload, { expiresIn: exp }),
          $bcrypt: {
            hash: async (plain: string) => await this.bcryptService.hash(plain),
            compare: async (p: string, h: string) =>
              await this.bcryptService.compare(p, h),
          },
          autoSlug: autoSlug,
        },
        $cache: this.cacheService,
        $params: matchedRoute.params ?? {},
        $query: req.query ?? {},
        $user: req.user ?? null,
        $repos: {},
        $req: {
          ...req,
          ip: realClientIP,
        },
        $share: {
          $logs: [],
        },
        $socket: {
          emitToUser: (userId: any, event: string, data: any) => {
            this.websocketGateway.emitToUser(userId, event, data);
          },
          emitToRoom: (room: string, event: string, data: any) => {
            this.websocketGateway.emitToRoom(room, event, data);
          },
          emitToNamespace: (path: string, event: string, data: any) => {
            this.websocketGateway.emitToNamespace(path, event, data);
          },
          emitToAll: (event: string, data: any) => {
            this.websocketGateway.emitToAll(event, data);
          },
        },
        $api: {
          request: {
            method: req.method,
            url: req.url,
            timestamp: new Date().toISOString(),
            correlationId: req.headers['x-correlation-id'] as string || this.generateCorrelationId(),
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
        const check = async (key: string, options: { maxRequests: number; perSeconds: number }) => {
          return this.rateLimitService.check(key, options);
        };

        const byIp = async (options: { maxRequests: number; perSeconds: number }) => {
          const key = `ip:${realClientIP}:${routePath}`;
          return check(key, options);
        };

        const byUser = async (options: { maxRequests: number; perSeconds: number }) => {
          const userId = req.user?.id || 'anonymous';
          const key = `user:${userId}:${routePath}`;
          return check(key, options);
        };

        const byRoute = async (options: { maxRequests: number; perSeconds: number }) => {
          const key = `route:${routePath}`;
          return check(key, options);
        };

        const byIpGlobal = async (options: { maxRequests: number; perSeconds: number }) => {
          const key = `ip:${realClientIP}`;
          return check(key, options);
        };

        const byUserGlobal = async (options: { maxRequests: number; perSeconds: number }) => {
          const userId = req.user?.id || 'anonymous';
          const key = `user:${userId}`;
          return check(key, options);
        };

        const reset = async (key: string) => {
          return this.rateLimitService.reset(key);
        };

        const status = async (key: string, options: { maxRequests: number; perSeconds: number }) => {
          return this.rateLimitService.status(key, options);
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

      context.$helpers.$rateLimit = createRateLimitHelper();

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
      context.$repos = this.repoRegistryService.createReposProxy(context, mainTableName);

      context.$dispatch = {
        trigger: (flowIdOrName: string | number, payload?: any) =>
          this.flowService.trigger(flowIdOrName, payload, req.user),
      };

      try {
        context.$helpers.$uploadFile = this.uploadFileHelper.createUploadFileHelper(context);
        context.$helpers.$updateFile = this.uploadFileHelper.createUpdateFileHelper(context);
        context.$helpers.$deleteFile = this.uploadFileHelper.createDeleteFileHelper(context);
      } catch (error) {
        this.logger.warn('Failed to initialize file helpers:', error);
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
  }

  private generateCorrelationId(): string {
    return `req_${Date.now()}_${Math.random().toString(36).substring(2, 11)}`;
  }

  private detectClientIP(req: any): string {
    const forwardedFor = req.headers['x-forwarded-for'];
    const realIP = req.headers['x-real-ip'];
    const cfConnectingIP = req.headers['cf-connecting-ip'];
    const remoteAddress = req.connection?.remoteAddress || req.socket?.remoteAddress;
    const reqIP = req.ip;
    let clientIP: string;

    if (forwardedFor) {
      clientIP = forwardedFor.split(',')[0].trim();
    } else if (realIP) {
      clientIP = realIP;
    } else if (cfConnectingIP) {
      clientIP = cfConnectingIP;
    } else if (reqIP && reqIP !== '::1' && reqIP !== '127.0.0.1') {
      clientIP = reqIP;
    } else if (remoteAddress && remoteAddress !== '::1' && remoteAddress !== '127.0.0.1') {
      clientIP = remoteAddress;
    } else {
      clientIP = reqIP || remoteAddress || 'unknown';
    }

    if (clientIP === '::1') {
      clientIP = '127.0.0.1';
    }
    if (clientIP?.startsWith('::ffff:')) {
      clientIP = clientIP.substring(7);
    }

    return clientIP;
  }
}
