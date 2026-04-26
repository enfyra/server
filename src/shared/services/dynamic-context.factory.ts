import * as jwt from 'jsonwebtoken';
import { TDynamicContext } from '../types';
import { BcryptService } from '../../domain/auth';
import { CacheService } from '../../engine/cache';
import { createFetchHelper } from '../helpers';
import { autoSlug } from '../utils/auto-slug.helper';
import { ScriptErrorFactory } from '../utils/script-error-factory';
import { EnvService } from './env.service';
import {
  SocketEmitCapture,
  WebsocketContextFactory,
} from '../../modules/websocket';

type DynamicContextOptions = {
  body?: any;
  data?: any;
  debug?: any;
  helpers?: TDynamicContext['$helpers'];
  cache?: TDynamicContext['$cache'];
  params?: any;
  query?: any;
  user?: any;
  repos?: TDynamicContext['$repos'];
  req?: TDynamicContext['$req'];
  share?: TDynamicContext['$share'];
  socket?: TDynamicContext['$socket'];
  apiRequest?: NonNullable<TDynamicContext['$api']>['request'];
  uploadedFile?: TDynamicContext['$uploadedFile'];
};

export class DynamicContextFactory {
  private readonly bcryptService: BcryptService;
  private readonly cacheService: CacheService;
  private readonly envService: EnvService;
  private readonly websocketContextFactory: WebsocketContextFactory;

  constructor(deps: {
    bcryptService: BcryptService;
    cacheService: CacheService;
    envService: EnvService;
    websocketContextFactory: WebsocketContextFactory;
  }) {
    this.bcryptService = deps.bcryptService;
    this.cacheService = deps.cacheService;
    this.envService = deps.envService;
    this.websocketContextFactory = deps.websocketContextFactory;
  }

  createBase(options: DynamicContextOptions = {}): TDynamicContext {
    const ctx: TDynamicContext = {
      $body: options.body ?? {},
      $data: options.data,
      $debug: options.debug,
      $throw: ScriptErrorFactory.createThrowHandlers(),
      $helpers: options.helpers ?? {},
      $cache: options.cache ?? {},
      $params: options.params ?? {},
      $query: options.query ?? {},
      $user: options.user ?? null,
      $repos: options.repos ?? {},
      $req: options.req,
      $share: options.share ?? { $logs: [] },
      $socket: options.socket,
      $api: options.apiRequest ? { request: options.apiRequest } : undefined,
      $uploadedFile: options.uploadedFile,
    };

    ctx.$logs = (...args: any[]) => {
      if (!ctx.$share) ctx.$share = { $logs: [] };
      if (!ctx.$share.$logs) ctx.$share.$logs = [];
      ctx.$share.$logs.push(...args);
    };

    return ctx;
  }

  createHttp(req: any, options: { params: any; realClientIP: string }) {
    return this.createBase({
      body: req.routeData?.context?.$body || req.body || {},
      debug: req._debug || undefined,
      helpers: {
        $jwt: (payload: any, exp: string) =>
          jwt.sign(payload, this.envService.get('SECRET_KEY'), {
            expiresIn: exp as import('ms').StringValue,
          }),
        $bcrypt: {
          hash: async (plain: string) => await this.bcryptService.hash(plain),
          compare: async (p: string, h: string) =>
            await this.bcryptService.compare(p, h),
        },
        autoSlug,
        $fetch: createFetchHelper(),
      },
      cache: this.cacheService,
      params: options.params ?? {},
      query: req.query ?? {},
      user: req.user ?? null,
      req: {
        method: req.method,
        url: req.url,
        headers: req.headers,
        query: req.query,
        params: req.params,
        ip: options.realClientIP,
        hostname: req.hostname,
        protocol: req.protocol,
        path: req.path,
        originalUrl: req.originalUrl,
      } as any,
      socket: this.websocketContextFactory.createGlobalProxy(),
      apiRequest: {
        method: req.method,
        url: req.url,
        timestamp: new Date().toISOString(),
        correlationId:
          (req.headers['x-correlation-id'] as string) ||
          this.generateCorrelationId(),
        userAgent: req.headers['user-agent'],
        ip: options.realClientIP,
      },
    });
  }

  createGraphql(options: {
    request?: any;
    user?: any;
    body?: any;
    params?: any;
    query?: any;
    args?: any;
  }) {
    const signJwt = (payload: any, exp: string) =>
      jwt.sign(payload, this.envService.get('SECRET_KEY'), {
        expiresIn: exp as import('ms').StringValue,
      });
    const ctx = this.createBase({
      body: options.body || {},
      params: options.params || {},
      query: options.query || {},
      user: options.user ?? null,
      req: options.request,
      helpers: {
        $jwt: signJwt,
        jwt: signJwt,
      } as any,
      socket: this.websocketContextFactory.createGlobalProxy(),
    });
    (ctx as any).$args = options.args || {};
    return ctx;
  }

  createFlow(options: {
    payload?: any;
    user?: any;
    socket?: TDynamicContext['$socket'];
    share?: TDynamicContext['$share'];
  }) {
    return this.createBase({
      body: options.payload || {},
      user: options.user ?? null,
      share: options.share,
      socket: options.socket ?? this.websocketContextFactory.createGlobalProxy(),
    });
  }

  createFlowTest(options: {
    payload?: any;
    user?: any;
    share?: TDynamicContext['$share'];
    emitted: SocketEmitCapture;
  }) {
    return this.createFlow({
      payload: options.payload,
      user: options.user,
      share: options.share,
      socket: this.websocketContextFactory.createCaptureProxy(options.emitted),
    });
  }

  createWebsocket(options: {
    method: string;
    url: string;
    body?: any;
    data?: any;
    user?: any;
    socket: TDynamicContext['$socket'];
    headers?: any;
    ip?: string | null;
    apiUrl?: string;
  }) {
    return this.createBase({
      body: options.body || {},
      data: options.data ?? options.body ?? {},
      helpers: { $fetch: createFetchHelper() },
      user: options.user ?? null,
      req: {
        method: options.method,
        url: options.url,
        ip: options.ip ?? null,
        headers: options.headers ?? {},
        user: options.user ?? null,
      } as any,
      socket: options.socket,
      apiRequest: {
        method: options.method,
        url: options.apiUrl ?? options.url,
        timestamp: new Date().toISOString(),
        ip: options.ip ?? undefined,
      },
    });
  }

  createWebsocketConnection(options: {
    gatewayPath: string;
    socketId: string;
    clientInfo?: any;
    user?: any;
  }) {
    return this.createWebsocket({
      method: 'WS_CONNECT',
      url: options.gatewayPath,
      body: options.clientInfo || {},
      data: options.clientInfo || {},
      user: options.user ?? null,
      headers: options.clientInfo?.headers,
      ip: options.clientInfo?.ip,
      socket: this.websocketContextFactory.createBoundProxy(
        options.gatewayPath,
        options.socketId,
      ),
    });
  }

  createWebsocketEvent(options: {
    gatewayPath: string;
    socketId: string;
    eventName: string;
    payload?: any;
    user?: any;
  }) {
    return this.createWebsocket({
      method: 'WS_EVENT',
      url: options.gatewayPath,
      apiUrl: `${options.gatewayPath}/${options.eventName}`,
      body: options.payload || {},
      data: options.payload || {},
      user: options.user ?? null,
      socket: this.websocketContextFactory.createBoundProxy(
        options.gatewayPath,
        options.socketId,
      ),
    });
  }

  createTestWebsocket(options: {
    method: string;
    url: string;
    body?: any;
    user?: any;
    headers?: any;
    socket: TDynamicContext['$socket'];
    correlationId: string;
  }) {
    const ctx = this.createWebsocket({
      method: options.method,
      url: options.url,
      body: options.body,
      data: options.body,
      user: options.user,
      headers: options.headers,
      socket: options.socket,
    });
    ctx.$api = {
      request: {
        method: options.method,
        url: options.url,
        timestamp: new Date().toISOString(),
        correlationId: options.correlationId,
      },
    };
    return ctx;
  }

  createTestWebsocketCapture(options: {
    method: string;
    url: string;
    body?: any;
    user?: any;
    headers?: any;
    correlationId: string;
  }) {
    const emitted: SocketEmitCapture = [];
    const ctx = this.createTestWebsocket({
      ...options,
      socket: this.websocketContextFactory.createCaptureProxy(emitted),
    });
    return { ctx, emitted };
  }

  private generateCorrelationId(): string {
    return `req_${Date.now()}_${Math.random().toString(36).substring(2, 11)}`;
  }
}
