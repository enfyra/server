import { Logger } from '../../../shared/logger';
import { Server, Socket } from 'socket.io';
import * as jwt from 'jsonwebtoken';
import { randomUUID } from 'node:crypto';
import { appendFileSync } from 'node:fs';
import { createAdapter } from '@socket.io/redis-adapter';
import Redis from 'ioredis';
import { getErrorMessage } from '../../../shared/utils/error.util';
import { CacheService, WebsocketCacheService } from '../../../engines/cache';
import { BuiltInSocketRegistry } from '../services/built-in-socket.registry';
import { EnvService } from '../../../shared/services';
import { CACHE_IDENTIFIERS } from '../../../shared/utils/cache-events.constants';
import {
  ENFYRA_ADMIN_ROOT_WEBSOCKET_ROOM,
  ENFYRA_ADMIN_WEBSOCKET_NAMESPACE,
} from '../../../shared/utils/constant';
import { QueryBuilderService } from '@enfyra/kernel';
import { RedisAdminService } from '../../admin/services/redis-admin.service';
import type {
  WebsocketDataShapeField,
  WebsocketNativeAction,
  WebsocketNativeFlowTrigger,
} from '../types/native-event.types';
import {
  loadUserWithRole,
  userCacheKey,
  USER_CACHE_TTL_MS,
} from '../../../shared/utils/load-user-with-role.util';
import type { Cradle } from '../../../container';

interface SocketData extends Socket {
  data: {
    user?: { id: number | string };
    userId?: number | string;
    gateway?: any;
  };
}

type RoomFanoutJob = {
  path: string;
  room: string;
  event: string;
  data: any;
  done?: () => void;
};

export class DynamicWebSocketGateway {
  server!: Server;
  private readonly logger = new Logger(DynamicWebSocketGateway.name);
  private readonly roomFanoutCommand = 'enfyra:chunk-room-emit';
  private readonly roomFanoutChunkThreshold: number;
  private readonly roomFanoutChunkSize: number;
  private readonly roomFanoutParallelChunks: number;
  private readonly roomFanoutBackpressureThreshold: number;
  private registeredGateways = new Set<string>();
  private gatewayConfigsByPath = new Map<string, any>();
  private roomFanoutQueues = new Map<string, RoomFanoutJob[]>();
  private activeRoomFanoutQueues = new Set<string>();
  private redisPubClient: Redis | null = null;
  private redisSubClient: Redis | null = null;
  private readonly websocketCacheService: WebsocketCacheService;
  private readonly builtInRegistry: BuiltInSocketRegistry;
  private readonly envService: EnvService;
  private readonly queryBuilderService: QueryBuilderService;
  private readonly cacheService: CacheService;
  private readonly redisAdminService: RedisAdminService;
  private readonly lazyRef: Cradle;
  private eventEmitter: any;

  constructor(deps: {
    websocketCacheService: WebsocketCacheService;
    builtInSocketRegistry: BuiltInSocketRegistry;
    eventEmitter: any;
    envService: EnvService;
    queryBuilderService: QueryBuilderService;
    cacheService: CacheService;
    redisAdminService: RedisAdminService;
    lazyRef: Cradle;
  }) {
    this.websocketCacheService = deps.websocketCacheService;
    this.builtInRegistry = deps.builtInSocketRegistry;
    this.envService = deps.envService;
    this.queryBuilderService = deps.queryBuilderService;
    this.cacheService = deps.cacheService;
    this.redisAdminService = deps.redisAdminService;
    this.lazyRef = deps.lazyRef;
    this.eventEmitter = deps.eventEmitter;
    this.roomFanoutChunkThreshold = this.readPositiveEnvNumber(
      'WS_ROOM_FANOUT_CHUNK_THRESHOLD',
      200,
    );
    this.roomFanoutChunkSize = this.readPositiveEnvNumber(
      'WS_ROOM_FANOUT_CHUNK_SIZE',
      100,
    );
    this.roomFanoutParallelChunks = this.readPositiveEnvNumber(
      'WS_ROOM_FANOUT_PARALLEL_CHUNKS',
      4,
    );
    this.roomFanoutBackpressureThreshold = this.readPositiveEnvNumber(
      'WS_ROOM_FANOUT_BACKPRESSURE_THRESHOLD',
      1000,
    );
    this.setupEventListeners();
  }

  private setupEventListeners() {
    this.eventEmitter.on(`${CACHE_IDENTIFIERS.WEBSOCKET}_LOADED`, () => {
      const reload = this.server ? this.reloadGateways() : this.registerGateways();
      reload.catch((error) =>
        this.logger.error('Failed to reload websocket gateways:', error),
      );
    });
  }

  private async setupRedisAdapter(server: Server) {
    const redisUri = this.envService.get('REDIS_URI');
    const redisHost = this.envService.get('REDIS_HOST') || 'localhost';
    const redisPort = 6379;
    const redisDb = 0;
    const redisPassword = this.envService.get('REDIS_PASSWORD');
    const nodeName = this.envService.get('NODE_NAME') || '';
    const keyPrefix = nodeName ? `${nodeName}:socket.io:` : 'socket.io:';

    const redisOptions = redisUri
      ? { lazyConnect: true }
      : {
          host: redisHost,
          port: redisPort,
          db: redisDb,
          password: redisPassword,
          lazyConnect: true,
        };
    const redisSubOptions = { ...redisOptions, enableReadyCheck: false };

    const pubClient = redisUri
      ? new Redis(redisUri, redisOptions)
      : new Redis(redisOptions);
    const subClient = redisUri
      ? new Redis(redisUri, redisSubOptions)
      : new Redis(redisSubOptions);

    pubClient.setMaxListeners(50);
    subClient.setMaxListeners(50);

    this.redisPubClient = pubClient;
    this.redisSubClient = subClient;

    await Promise.all([pubClient.connect(), subClient.connect()]);

    server.adapter(createAdapter(pubClient, subClient, { key: keyPrefix }));
    this.logger.log(`Redis adapter configured (prefix: ${keyPrefix})`);
  }

  private setupRoomFanoutCommandListener(server: Server) {
    server.removeAllListeners(this.roomFanoutCommand);
    server.on(
      this.roomFanoutCommand,
      (path: string, room: string, event: string, data: any) => {
        this.enqueueChunkedLocalRoomEmit(path, room, event, data);
      },
    );
  }

  async afterInit(server: Server) {
    this.server = server;
    await this.setupRedisAdapter(server);
    this.setupRoomFanoutCommandListener(server);
    await this.registerGateways();
    this.logger.log('WebSocket Gateway initialized');
  }

  private updateGatewayConfigs(gateways: any[]) {
    this.gatewayConfigsByPath.clear();
    for (const g of gateways) {
      this.gatewayConfigsByPath.set(g.path, g);
    }
  }

  async registerGateways() {
    if (!this.server) return;
    try {
      const gateways = await this.websocketCacheService.getGateways();
      const newPaths = new Set(gateways.map((g: any) => g.path));
      for (const path of this.registeredGateways) {
        if (path === ENFYRA_ADMIN_WEBSOCKET_NAMESPACE) continue;
        if (!newPaths.has(path)) {
          const namespace = this.server.of(path);
          namespace.disconnectSockets();
          namespace.removeAllListeners();
          this.registeredGateways.delete(path);
        }
      }
      this.updateGatewayConfigs(gateways);
      for (const gateway of gateways) {
        if (this.registeredGateways.has(gateway.path)) {
          continue;
        }
        this.setupNamespace(gateway.path);
        this.registeredGateways.add(gateway.path);
      }
      this.logger.log(`Registered ${gateways.length} websocket gateways`);
    } catch (error) {
      this.logger.error('Failed to register websocket gateways:', error);
    }
  }

  private setupNamespace(path: string) {
    const namespace = this.server.of(path);
    namespace.use(async (socket: SocketData, next) => {
      const gateway = this.gatewayConfigsByPath.get(path);
      if (!gateway) {
        return next(new Error('Gateway not configured'));
      }
      if (gateway.requireAuth) {
        const authHeader = socket.handshake.headers?.authorization;
        const cookieHeader = socket.handshake.headers?.cookie;
        const cookieToken =
          cookieHeader
            ?.split(';')
            .map((c) => c.trim())
            .find((c) => c.startsWith('accessToken='))
            ?.slice('accessToken='.length) || null;
        const token =
          socket.handshake.auth?.token ||
          (authHeader?.startsWith('Bearer ') ? authHeader.slice(7) : null) ||
          cookieToken;
        if (!token) {
          const err = new Error('Authentication token required');
          (err as any).data = { code: 'AUTH_REQUIRED', path };
          this.logger.warn(
            `Connection rejected: no token provided for ${path}`,
          );
          if (socket.conn) socket.conn.close();
          return next(err);
        }
        try {
          const user = jwt.verify(
            token,
            this.envService.get('SECRET_KEY'),
          ) as jwt.JwtPayload;
          socket.data.user = user as any;
          socket.data.userId = user.id || user.userId;
          socket.data.gateway = gateway;
          next();
        } catch (error) {
          const err = new Error('Invalid authentication token');
          (err as any).data = { code: 'AUTH_INVALID', path };
          this.logger.warn(`Connection rejected: invalid token for ${path}`);
          if (socket.conn) socket.conn.close();
          return next(err);
        }
      } else {
        socket.data.gateway = gateway;
        next();
      }
    });
    namespace.on('connection', async (socket: SocketData) => {
      const gatewayData =
        this.gatewayConfigsByPath.get(path) ?? socket.data.gateway;
      if (!gatewayData) {
        this.logger.warn(
          `Connection rejected: missing gateway data for ${path}`,
        );
        socket.emit('auth_error', {
          code: 'AUTH_REJECTED',
          message: 'Authentication failed',
        });
        socket.disconnect(true);
        return;
      }
      if (gatewayData.requireAuth && !socket.data.userId) {
        this.logger.warn(
          `Connection rejected: auth required but no user for ${gatewayData.path}`,
        );
        socket.emit('auth_error', {
          code: 'AUTH_REQUIRED',
          message: 'Authentication token required',
        });
        socket.disconnect(true);
        return;
      }
      if (path === ENFYRA_ADMIN_WEBSOCKET_NAMESPACE) {
        try {
          await this.setupAdminSocket(socket);
        } catch (error) {
          this.logger.warn(
            `Admin socket setup failed: ${getErrorMessage(error)}`,
          );
        }
      }
      const connectionScript =
        this.builtInRegistry.getConnectionScript(path) ??
        gatewayData.connectionHandlerScript;
      if (connectionScript) {
        try {
          await this.runConnectionScript(socket, gatewayData, connectionScript);
        } catch (error) {
          this.logger.error(
            `Connection handler failed for ${socket.id}:`,
            error,
          );
          socket.disconnect();
          return;
        }
      }
      const roomName = socket.data.userId
        ? `user_${socket.data.userId}`
        : `user_${socket.id}`;
      socket.join(roomName);
      for (const event of gatewayData.events) {
        const eventName = event.eventName;
        socket.on(eventName, async (payload: any, ack: any) => {
          const requestId = randomUUID();
          const socketReceivedAt = Date.now();
          let ackSentAt: number | null = null;
          try {
            const builtInScript = this.builtInRegistry.getEventScript(
              path,
              eventName,
            );
            const freshEvent: any = event;
            const script = builtInScript ?? event.handlerScript;

            if (this.hasNativeEventConfig(freshEvent)) {
              await this.runNativeEvent({
                requestId,
                socket,
                eventName,
                payload,
                gatewayPath: gatewayData.path,
                event: freshEvent,
                socketReceivedAt,
                ackSentAt,
              });
              if (typeof ack === 'function') {
                ack({ accepted: true, queued: false, requestId, eventName });
                ackSentAt = Date.now();
              }
              return;
            }

            if (!script) {
              this.logger.warn(
                `No handler for event ${eventName} on ${gatewayData.path}`,
              );
              if (typeof ack === 'function') {
                ack({
                  accepted: false,
                  queued: false,
                  requestId,
                  eventName,
                  error: {
                    code: 'NO_HANDLER',
                    message: 'No handler configured',
                  },
                });
              }
              return;
            }
            await this.runEventScript({
              requestId,
              socket,
              eventName,
              payload,
              gatewayPath: gatewayData.path,
              script,
              timeout: freshEvent?.timeout ?? event.timeout,
              socketReceivedAt,
              ackSentAt,
            });
            if (typeof ack === 'function') {
              ack({ accepted: true, queued: false, requestId, eventName });
              ackSentAt = Date.now();
            }
          } catch (error) {
            this.logger.error(`Event handler failed for ${eventName}:`, error);
            if (typeof ack === 'function') {
              ack({
                accepted: false,
                queued: false,
                requestId,
                eventName,
                error: {
                  code: 'WS_HANDLER_ERROR',
                  message: getErrorMessage(error) || 'Websocket handler failed',
                },
              });
            }
            socket.emit('ws:error', {
              requestId,
              eventName,
              code: 'WS_HANDLER_ERROR',
              message: getErrorMessage(error) || 'Websocket handler failed',
            });
          }
        });
      }
    });
  }

  private async runConnectionScript(
    socket: SocketData,
    gatewayData: any,
    script: string,
  ) {
    const userId = socket.data.userId || null;
    const ctx = this.lazyRef.dynamicContextFactory.createWebsocketConnection({
      gatewayPath: gatewayData.path,
      socketId: socket.id,
      clientInfo: {
        id: socket.id,
        ip: socket.handshake.address,
        headers: socket.handshake.headers,
        auth: socket.handshake.auth,
      },
      user: userId ? { id: userId } : null,
    });
    ctx.$repos = this.lazyRef.repoRegistryService.createReposProxy(ctx);
    ctx.$trigger = (flowIdOrName: string | number, payload?: any) =>
      this.lazyRef.flowService.trigger(
        flowIdOrName,
        payload,
        userId ? { id: userId } : null,
      );

    await this.lazyRef.executorEngineService.run(
      script,
      ctx,
      gatewayData.connectionHandlerTimeout,
    );
  }

  private async runEventScript(options: {
    requestId: string;
    socket: SocketData;
    eventName: string;
    payload: any;
    gatewayPath: string;
    script: string;
    timeout: number;
    socketReceivedAt: number;
    ackSentAt: number | null;
  }) {
    const startedAt = Date.now();
    const {
      requestId,
      socket,
      eventName,
      payload,
      gatewayPath,
      script,
      timeout,
      socketReceivedAt,
      ackSentAt,
    } = options;
    const userId = socket.data.userId || null;
    const ctx = this.lazyRef.dynamicContextFactory.createWebsocketEvent({
      gatewayPath,
      socketId: socket.id,
      eventName,
      payload,
      user: userId ? { id: userId } : null,
    });
    ctx.$repos = this.lazyRef.repoRegistryService.createReposProxy(ctx);
    ctx.$trigger = (flowIdOrName: string | number, payload?: any) =>
      this.lazyRef.flowService.trigger(
        flowIdOrName,
        payload,
        userId ? { id: userId } : null,
      );

    try {
      const result = await this.lazyRef.executorEngineService.run(
        script,
        ctx,
        timeout,
      );
      const endedAt = Date.now();
      if (payload?.__suppressResult !== true) {
        ctx.$socket?.reply?.('ws:result', {
          requestId,
          eventName,
          success: true,
          result,
          logs: ctx.$share?.$logs || [],
        });
      }
      this.writeEventTrace({
        type: 'ws_event',
        mode: 'inline',
        status: 'success',
        requestId,
        eventName,
        gatewayPath,
        socketReceivedAt,
        ackSentAt,
        workerStartedAt: startedAt,
        executorEndedAt: endedAt,
        queueWaitMs: 0,
        executorMs: endedAt - startedAt,
        inlineExecutorMs: endedAt - startedAt,
        totalHandlerMs: endedAt - socketReceivedAt,
        messageId: payload?.id,
        kind: payload?.kind,
        suppressResult: payload?.__suppressResult === true,
      });
    } catch (error: any) {
      const failedAt = Date.now();
      ctx.$socket?.reply?.('ws:error', {
        requestId,
        eventName,
        success: false,
        code: error?.errorCode || error?.code || 'WS_HANDLER_ERROR',
        message: error?.message || 'Websocket handler failed',
        logs: ctx.$share?.$logs || [],
        details: error?.details,
      });
      this.writeEventTrace({
        type: 'ws_event',
        mode: 'inline',
        status: 'error',
        requestId,
        eventName,
        gatewayPath,
        socketReceivedAt,
        ackSentAt,
        workerStartedAt: startedAt,
        failedAt,
        queueWaitMs: 0,
        totalHandlerMs: failedAt - socketReceivedAt,
        messageId: payload?.id,
        kind: payload?.kind,
        error: error?.message || String(error),
      });
      throw error;
    }
  }

  private async runNativeEvent(options: {
    requestId: string;
    socket: SocketData;
    eventName: string;
    payload: any;
    gatewayPath: string;
    event: any;
    socketReceivedAt: number;
    ackSentAt: number | null;
  }) {
    const startedAt = Date.now();
    const {
      requestId,
      socket,
      eventName,
      payload,
      gatewayPath,
      event,
      socketReceivedAt,
      ackSentAt,
    } = options;

    try {
      this.validateNativePayload(event.dataShape, payload);
      for (const action of this.getNativeActions(event)) {
        if (!this.matchesCondition(action.when, payload)) continue;
        await this.executeNativeAction(socket, gatewayPath, action, payload);
      }
      const flowTriggerStartedAt = Date.now();
      let triggeredFlowCount = 0;
      for (const flow of this.getNativeFlowTriggers(event)) {
        if (!this.matchesCondition(flow.when, payload)) continue;
        await this.triggerNativeFlow(flow, payload, socket.data.userId || null);
        triggeredFlowCount++;
      }
      const endedAt = Date.now();
      this.writeEventTrace({
        type: 'ws_event',
        mode: 'native',
        status: 'success',
        requestId,
        eventName,
        gatewayPath,
        socketReceivedAt,
        ackSentAt,
        workerStartedAt: startedAt,
        nativeEndedAt: endedAt,
        queueWaitMs: 0,
        nativeMs: endedAt - startedAt,
        triggerFlowMs:
          triggeredFlowCount > 0 ? endedAt - flowTriggerStartedAt : undefined,
        triggeredFlowCount,
        totalHandlerMs: endedAt - socketReceivedAt,
        clientSentAt: payload?.sentAt,
        serverReceiveLagMs:
          typeof payload?.sentAt === 'number'
            ? socketReceivedAt - payload.sentAt
            : undefined,
        messageId: payload?.id,
        kind: payload?.kind,
        suppressResult: true,
      });
    } catch (error: any) {
      const failedAt = Date.now();
      socket.emit('ws:error', {
        requestId,
        eventName,
        success: false,
        code: error?.code || 'WS_NATIVE_HANDLER_ERROR',
        message: error?.message || 'Websocket native handler failed',
      });
      this.writeEventTrace({
        type: 'ws_event',
        mode: 'native',
        status: 'error',
        requestId,
        eventName,
        gatewayPath,
        socketReceivedAt,
        ackSentAt,
        workerStartedAt: startedAt,
        failedAt,
        queueWaitMs: 0,
        totalHandlerMs: failedAt - socketReceivedAt,
        clientSentAt: payload?.sentAt,
        serverReceiveLagMs:
          typeof payload?.sentAt === 'number'
            ? socketReceivedAt - payload.sentAt
            : undefined,
        messageId: payload?.id,
        kind: payload?.kind,
        error: error?.message || String(error),
      });
      throw error;
    }
  }

  private hasNativeEventConfig(event: any) {
    return (
      event?.socketAction != null ||
      event?.socketActions != null ||
      event?.triggerFlow != null ||
      event?.triggerFlows != null
    );
  }

  private getNativeActions(event: any): WebsocketNativeAction[] {
    const actions = event?.socketActions ?? event?.socketAction;
    if (!actions) return [];
    return Array.isArray(actions) ? actions : [actions];
  }

  private getNativeFlowTriggers(event: any): WebsocketNativeFlowTrigger[] {
    const flows = event?.triggerFlows ?? event?.triggerFlow;
    if (!flows) return [];
    return Array.isArray(flows) ? flows : [flows];
  }

  private async executeNativeAction(
    socket: SocketData,
    gatewayPath: string,
    action: WebsocketNativeAction,
    payload: any,
  ): Promise<void> {
    const type = action.action ?? action.type;
    const event = String(action.event ?? action.eventName ?? '');
    const data = this.resolvePayloadExpression(
      action.payloadExpression ?? action.payload ?? '{{ data }}',
      payload,
    );

    switch (type) {
      case 'joinRoom':
        socket.join(this.resolveTemplate(action.room ?? action.roomTemplate, payload));
        return;
      case 'leaveRoom':
        socket.leave(this.resolveTemplate(action.room ?? action.roomTemplate, payload));
        return;
      case 'emitToRoom':
        await this.emitToNamespaceRoom(
          gatewayPath,
          this.resolveTemplate(action.room ?? action.roomTemplate, payload),
          event,
          data,
        );
        return;
      case 'emitToUser':
        this.emitToUser(
          this.resolveTemplate(action.userId ?? action.userTemplate, payload),
          event,
          data,
        );
        return;
      case 'reply':
        socket.emit(event, data);
        return;
      case 'broadcast':
        this.emitToNamespace(gatewayPath, event, data);
        return;
      case 'disconnect':
        socket.disconnect(true);
        return;
      default:
        throw new Error(`Unsupported websocket native action "${String(type)}"`);
    }
  }

  private async triggerNativeFlow(
    flow: WebsocketNativeFlowTrigger,
    payload: any,
    userId: number | string | null,
  ) {
    const flowIdOrName = flow.flowId ?? flow.flowName ?? flow.flow;
    if (flowIdOrName === undefined || flowIdOrName === null || flowIdOrName === '') {
      throw new Error('Native websocket flow trigger requires flowId or flowName');
    }
    const flowPayload = this.resolvePayloadExpression(
      flow.payloadExpression ?? flow.payload ?? '{{ data }}',
      payload,
    );
    await this.lazyRef.flowService.trigger(
      flowIdOrName,
      flowPayload,
      userId ? { id: userId } : null,
    );
  }

  private validateNativePayload(
    shape: WebsocketDataShapeField[] | null | undefined,
    payload: any,
    prefix = '',
  ) {
    if (!Array.isArray(shape) || shape.length === 0) return;
    for (const field of shape) {
      if (!field?.name) continue;
      const path = prefix ? `${prefix}.${field.name}` : field.name;
      const value = this.getPathValue(payload, path);
      if (field.required && (value === undefined || value === null || value === '')) {
        const error = new Error(`Missing required websocket payload field "${path}"`);
        (error as any).code = 'WS_PAYLOAD_VALIDATION_ERROR';
        throw error;
      }
      if (
        value != null &&
        field.type === 'object' &&
        Array.isArray(field.children) &&
        !Array.isArray(value)
      ) {
        this.validateNativePayload(field.children, value, '');
      }
    }
  }

  private matchesCondition(condition: any, payload: any) {
    if (!condition) return true;
    const path = condition.field ?? condition.path;
    if (!path) return true;
    const value = this.getPathValue(payload, String(path));
    if ('exists' in condition) {
      return condition.exists ? value !== undefined && value !== null : value === undefined || value === null;
    }
    if ('eq' in condition) return value === condition.eq;
    if ('ne' in condition) return value !== condition.ne;
    return Boolean(value);
  }

  private resolvePayloadExpression(expression: any, payload: any): any {
    if (
      expression === undefined ||
      expression === null ||
      expression === '$data' ||
      expression === '{{ data }}'
    ) {
      return payload;
    }
    if (typeof expression === 'string') {
      const trimmed = expression.trim();
      if (trimmed === '$data' || trimmed === '{{ data }}') return payload;
      if (trimmed.startsWith('$data.')) {
        return this.getPathValue(payload, trimmed.slice('$data.'.length));
      }
      const tokenOnly = trimmed.match(/^\{\{\s*data\.([^}]+)\s*\}\}$/);
      if (tokenOnly) return this.getPathValue(payload, tokenOnly[1].trim());
      return this.resolveTemplate(trimmed, payload);
    }
    if (Array.isArray(expression)) {
      return expression.map((item) => this.resolvePayloadExpression(item, payload));
    }
    if (typeof expression === 'object') {
      return Object.fromEntries(
        Object.entries(expression).map(([key, value]) => [
          key,
          this.resolvePayloadExpression(value, payload),
        ]),
      );
    }
    return expression;
  }

  private resolveTemplate(template: any, payload: any) {
    if (template === undefined || template === null) return '';
    return String(template).replace(/\{\{\s*data\.([^}]+)\s*\}\}/g, (_, path) => {
      const value = this.getPathValue(payload, String(path).trim());
      return value == null ? '' : String(value);
    });
  }

  private getPathValue(source: any, path: string) {
    return path.split('.').reduce((current, segment) => {
      if (current == null) return undefined;
      const normalized = segment.endsWith('[]') ? segment.slice(0, -2) : segment;
      const value = current[normalized];
      return Array.isArray(value) ? value[0] : value;
    }, source);
  }

  private writeEventTrace(entry: Record<string, any>) {
    const file = process.env.WS_EVENT_TRACE_FILE;
    if (!file) return;
    try {
      appendFileSync(file, `${JSON.stringify(entry)}\n`);
    } catch {}
  }

  private async setupAdminSocket(socket: SocketData) {
    const user = await this.loadSocketUser(socket);
    if (user?.isRootAdmin) {
      socket.join(ENFYRA_ADMIN_ROOT_WEBSOCKET_ROOM);
      this.redisAdminService
        .getOverview()
        .then((overview) => {
          socket.emit('$system:redis:overview', overview);
        })
        .catch(() => {});
    }

    socket.on('$system:redis:overview:get', async (_payload: any, ack: any) => {
      await this.ackRedisAdmin(socket, ack, () =>
        this.redisAdminService.getOverview(),
      );
    });
    socket.on('$system:redis:keys:list', async (payload: any, ack: any) => {
      await this.ackRedisAdmin(socket, ack, () =>
        this.redisAdminService.listKeys({
          cursor: payload?.cursor,
          pattern: payload?.pattern,
          count: payload?.count,
        }),
      );
    });
    socket.on('$system:redis:key:get', async (payload: any, ack: any) => {
      await this.ackRedisAdmin(socket, ack, () =>
        this.redisAdminService.getKey(String(payload?.key || ''), {
          limit: payload?.limit,
        }),
      );
    });
  }

  private async ackRedisAdmin<T>(
    socket: SocketData,
    ack: any,
    callback: () => Promise<T>,
  ) {
    try {
      const user = await this.loadSocketUser(socket);
      if (!user?.isRootAdmin) {
        throw new Error('Root admin access required');
      }
      const data = await callback();
      if (typeof ack === 'function') ack({ success: true, data });
    } catch (error) {
      const message = getErrorMessage(error) || 'Redis admin request failed';
      if (typeof ack === 'function') {
        ack({ success: false, error: { message } });
      } else {
        socket.emit('$system:redis:error', { message });
      }
    }
  }

  private async loadSocketUser(socket: SocketData): Promise<any | null> {
    const currentUser = socket.data.user as any;
    if (currentUser?.isRootAdmin !== undefined) return currentUser;
    const id = currentUser?.id ?? currentUser?.userId;
    if (id === undefined || id === null) return null;
    const cacheKey = userCacheKey(id);
    let user = await this.cacheService.get<any>(cacheKey);
    if (!user) {
      user = await loadUserWithRole(this.queryBuilderService, id);
      if (user) {
        await this.cacheService.set(cacheKey, user, USER_CACHE_TTL_MS);
      }
    }
    if (user) {
      Object.assign(user, {
        loginProvider: currentUser?.loginProvider ?? null,
      });
      socket.data.user = user;
      socket.data.userId = user.id || user._id || id;
    }
    return user;
  }

  async handleConnection(_client: Socket) {}
  async handleDisconnect(_client: Socket) {}

  async onDestroy() {
    try {
      this.server?.close();
    } catch {}
    try {
      this.redisPubClient?.disconnect();
    } catch {}
    try {
      this.redisSubClient?.disconnect();
    } catch {}
  }

  async reloadGateways() {
    this.logger.log('Reloading websocket gateways...');
    const keepAdminNamespace = this.registeredGateways.has(
      ENFYRA_ADMIN_WEBSOCKET_NAMESPACE,
    );
    for (const path of this.registeredGateways) {
      if (path === ENFYRA_ADMIN_WEBSOCKET_NAMESPACE) continue;
      const namespace = this.server.of(path);
      namespace.disconnectSockets();
      namespace.removeAllListeners();
    }
    this.registeredGateways.clear();
    if (keepAdminNamespace) {
      this.registeredGateways.add(ENFYRA_ADMIN_WEBSOCKET_NAMESPACE);
    }
    await this.registerGateways();
    this.logger.log(
      `Gateways reloaded. Total registered: ${this.registeredGateways.size}`,
    );
  }

  joinRoom(path: string, socketId: string, room: string) {
    const socket = this.server.of(path).sockets.get(socketId);
    if (socket) socket.join(room);
  }

  leaveRoom(path: string, socketId: string, room: string) {
    const socket = this.server.of(path).sockets.get(socketId);
    if (socket) socket.leave(room);
  }

  disconnectSocket(path: string, socketId: string) {
    const socket = this.server.of(path).sockets.get(socketId);
    if (socket) socket.disconnect(true);
  }

  emitToUser(userId: number | string, event: string, data: any) {
    const room = `user_${userId}`;
    for (const path of this.registeredGateways) {
      this.server.of(path).to(room).emit(event, data);
    }
  }

  emitToRoom(room: string, event: string, data: any) {
    for (const path of this.registeredGateways) {
      void this.emitToNamespaceRoom(path, room, event, data);
    }
  }

  emitToNamespace(path: string, event: string, data: any) {
    this.server.of(path).emit(event, data);
  }

  async emitToNamespaceRoom(path: string, room: string, event: string, data: any) {
    const localSize = this.localNamespaceRoomSize(path, room);
    if (localSize < this.roomFanoutChunkThreshold) {
      this.server.of(path).to(room).emit(event, data);
      return;
    }
    const backpressure = this.enqueueChunkedLocalRoomEmit(path, room, event, data);
    this.emitRoomFanoutCommand(path, room, event, data);
    await backpressure;
  }

  private emitRoomFanoutCommand(
    path: string,
    room: string,
    event: string,
    data: any,
  ) {
    try {
      this.server.serverSideEmit(this.roomFanoutCommand, path, room, event, data);
    } catch (error) {
      this.logger.error(
        `Failed to publish chunked room fanout for ${path}:${room}:`,
        error,
      );
    }
  }

  private enqueueChunkedLocalRoomEmit(
    path: string,
    room: string,
    event: string,
    data: any,
  ): Promise<void> {
    const key = `${path}:${room}`;
    const queue = this.roomFanoutQueues.get(key);
    const queueDepth = queue?.length || 0;
    let done: (() => void) | undefined;
    const shouldBackpressure =
      queueDepth >= this.roomFanoutBackpressureThreshold;
    const backpressure = shouldBackpressure
      ? new Promise<void>((resolve) => {
          done = resolve;
        })
      : Promise.resolve();
    const job = { path, room, event, data, done };
    if (queue) {
      queue.push(job);
    } else {
      this.roomFanoutQueues.set(key, [job]);
    }
    if (this.activeRoomFanoutQueues.has(key)) return backpressure;
    this.activeRoomFanoutQueues.add(key);
    void this.drainRoomFanoutQueue(key);
    return backpressure;
  }

  private async drainRoomFanoutQueue(key: string) {
    try {
      while (true) {
        const queue = this.roomFanoutQueues.get(key);
        const job = queue?.shift();
        if (!job) {
          this.roomFanoutQueues.delete(key);
          return;
        }
        if (!queue?.length) this.roomFanoutQueues.delete(key);
        await this.emitLocalRoomInChunks(job);
        job.done?.();
      }
    } catch (error) {
      this.logger.error(`Chunked room fanout failed for ${key}:`, error);
    } finally {
      this.activeRoomFanoutQueues.delete(key);
      const queue = this.roomFanoutQueues.get(key);
      if (queue?.length) {
        this.activeRoomFanoutQueues.add(key);
        void this.drainRoomFanoutQueue(key);
      }
    }
  }

  private async emitLocalRoomInChunks(job: RoomFanoutJob) {
    const namespace = this.server.of(job.path);
    const socketIds = [...(namespace.adapter.rooms.get(job.room) ?? [])];
    const windowSize = Math.max(
      this.roomFanoutChunkSize,
      this.roomFanoutChunkSize * this.roomFanoutParallelChunks,
    );
    for (let i = 0; i < socketIds.length; i += windowSize) {
      const window = socketIds.slice(i, i + windowSize);
      for (let j = 0; j < window.length; j += this.roomFanoutChunkSize) {
        const chunk = window.slice(j, j + this.roomFanoutChunkSize);
        queueMicrotask(() => {
          for (const socketId of chunk) {
            namespace.sockets.get(socketId)?.emit(job.event, job.data);
          }
        });
      }
      if (i + windowSize < socketIds.length) {
        await this.yieldToEventLoop();
      }
    }
    await this.yieldToEventLoop();
  }

  private localNamespaceRoomSize(path: string, room: string) {
    return this.server.of(path).adapter.rooms.get(room)?.size || 0;
  }

  private yieldToEventLoop() {
    return new Promise<void>((resolve) => setImmediate(resolve));
  }

  private readPositiveEnvNumber(name: string, fallback: number) {
    const value = Number(process.env[name]);
    return Number.isFinite(value) && value > 0 ? value : fallback;
  }

  emitToSocket(path: string, socketId: string, event: string, data: any) {
    this.server.of(path).to(socketId).emit(event, data);
  }

  emitToAll(event: string, data: any) {
    for (const path of this.registeredGateways) {
      this.server.of(path).emit(event, data);
    }
  }

  getConnectionStats() {
    if (!this.server) {
      return { total: 0, namespaces: [] };
    }

    const paths = new Set([
      ENFYRA_ADMIN_WEBSOCKET_NAMESPACE,
      ...this.registeredGateways,
    ]);
    const namespaces = Array.from(paths).map((path) => {
      const namespace = this.server.of(path);
      const userIds = new Set<string>();
      for (const socket of namespace.sockets.values()) {
        const userId = (socket as SocketData).data?.userId;
        if (userId !== undefined && userId !== null) {
          userIds.add(String(userId));
        }
      }
      return {
        path,
        connected: namespace.sockets.size,
        users: userIds.size,
      };
    });

    return {
      total: namespaces.reduce((sum, item) => sum + item.connected, 0),
      namespaces,
    };
  }

  async roomSize(room: string): Promise<number> {
    let total = 0;
    for (const path of this.registeredGateways) {
      total += await this.countNamespaceRoom(path, room);
    }
    return total;
  }

  async namespaceRoomSize(path: string, room: string): Promise<number> {
    return await this.countNamespaceRoom(path, room);
  }

  private async countNamespaceRoom(
    path: string,
    room: string,
  ): Promise<number> {
    const operator = this.server.of(path).in(room);
    if (typeof operator.fetchSockets === 'function') {
      const sockets = await operator.fetchSockets();
      return sockets.length;
    }
    const sockets = await operator.allSockets();
    return sockets.size;
  }
}
