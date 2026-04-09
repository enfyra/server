import {
  WebSocketGateway,
  WebSocketServer,
  OnGatewayConnection,
  OnGatewayDisconnect,
  OnGatewayInit,
} from '@nestjs/websockets';
import { Injectable, Logger } from '@nestjs/common';
import { OnEvent } from '@nestjs/event-emitter';
import { CACHE_EVENTS } from '../../../shared/utils/cache-events.constants';
import { Server, Socket } from 'socket.io';
import { InjectQueue } from '@nestjs/bullmq';
import { Queue } from 'bullmq';
import { ConfigService } from '@nestjs/config';
import { JwtService } from '@nestjs/jwt';
import { randomUUID } from 'node:crypto';
import { createAdapter } from '@socket.io/redis-adapter';
import Redis from 'ioredis';
import { WebsocketCacheService } from '../../../infrastructure/cache/services/websocket-cache.service';
import { BuiltInSocketRegistry } from '../services/built-in-socket.registry';
import { SYSTEM_QUEUES } from '../../../shared/utils/constant';
interface SocketData extends Socket {
  data: {
    user?: { id: number | string };
    userId?: number | string;
    gateway?: any;
  };
}
@Injectable()
@WebSocketGateway({
  cors: { origin: '*' },
})
export class DynamicWebSocketGateway
  implements OnGatewayInit, OnGatewayConnection, OnGatewayDisconnect
{
  @WebSocketServer()
  server: Server;
  private readonly logger = new Logger(DynamicWebSocketGateway.name);
  private registeredGateways = new Set<string>();
  private gatewayConfigsByPath = new Map<string, any>();
  constructor(
    private readonly configService: ConfigService,
    @InjectQueue(SYSTEM_QUEUES.WS_CONNECTION)
    private readonly connectionQueue: Queue,
    @InjectQueue(SYSTEM_QUEUES.WS_EVENT)
    private readonly eventQueue: Queue,
    private readonly websocketCache: WebsocketCacheService,
    private readonly builtInRegistry: BuiltInSocketRegistry,
  ) {
    this.jwtService = new JwtService({
      secret: this.configService.get('SECRET_KEY'),
    });
  }
  private readonly jwtService: JwtService;

  private setupRedisAdapter(server: Server) {
    const redisUri = this.configService.get('REDIS_URI');
    const redisHost = this.configService.get('REDIS_HOST') || 'localhost';
    const redisPort = this.configService.get<number>('REDIS_PORT') || 6379;
    const redisDb = this.configService.get<number>('REDIS_DB') || 0;
    const redisPassword = this.configService.get('REDIS_PASSWORD');
    const nodeName = this.configService.get('NODE_NAME') || '';
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

    const pubClient = redisUri
      ? new Redis(redisUri, redisOptions)
      : new Redis(redisOptions);
    const subClient = pubClient.duplicate();

    pubClient
      .connect()
      .catch((err) =>
        this.logger.error('Redis adapter pub client error:', err),
      );
    subClient
      .connect()
      .catch((err) =>
        this.logger.error('Redis adapter sub client error:', err),
      );

    server.adapter(createAdapter(pubClient, subClient, { key: keyPrefix }));
    this.logger.log(`Redis adapter configured (prefix: ${keyPrefix})`);
  }

  afterInit(server: Server) {
    this.setupRedisAdapter(server);
    this.logger.log('WebSocket Gateway initialized');
  }
  @OnEvent(CACHE_EVENTS.WEBSOCKET_LOADED)
  async onWebsocketCacheLoaded() {
    await this.registerGateways();
  }
  private updateGatewayConfigs(gateways: any[]) {
    this.gatewayConfigsByPath.clear();
    for (const g of gateways) {
      this.gatewayConfigsByPath.set(g.path, g);
    }
  }
  async registerGateways() {
    try {
      const gateways = await this.websocketCache.getGateways();
      const newPaths = new Set(gateways.map((g: any) => g.path));
      for (const path of this.registeredGateways) {
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
          const user = this.jwtService.verify(token);
          socket.data.user = user;
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
      const connectionScript =
        this.builtInRegistry.getConnectionScript(path) ??
        gatewayData.connectionHandlerScript;
      if (connectionScript) {
        try {
          await this.connectionQueue.add(
            `${gatewayData.path}:${socket.id}`,
            {
              socketId: socket.id,
              userId: socket.data.userId || null,
              clientInfo: {
                id: socket.id,
                ip: socket.handshake.address,
                headers: socket.handshake.headers,
              },
              gatewayId: gatewayData.id,
              gatewayPath: gatewayData.path,
              script: connectionScript,
              timeout: gatewayData.connectionHandlerTimeout,
            },
            {
              attempts: 0,
              removeOnComplete: {
                count: 100,
                age: 3600,
              },
              removeOnFail: {
                count: 500,
                age: 24 * 3600,
              },
            },
          );
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
        socket.on(eventName, async (payload, ack) => {
          const requestId = randomUUID();
          if (typeof ack === 'function') {
            ack({ queued: true, requestId, eventName });
          }
          try {
            const builtInScript = this.builtInRegistry.getEventScript(
              path,
              eventName,
            );
            let script = builtInScript;
            let freshEvent: any = event;
            if (!script) {
              const freshGateway = await this.websocketCache.getGatewayByPath(
                gatewayData.path,
              );
              freshEvent =
                freshGateway?.events?.find(
                  (e: any) => e.eventName === eventName,
                ) ?? event;
              script = freshEvent?.handlerScript ?? event.handlerScript;
            }

            if (!script) {
              this.logger.warn(
                `No handler for event ${eventName} on ${gatewayData.path}`,
              );
              if (typeof ack === 'function') {
                ack({
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
            await this.eventQueue.add(
              `ws-event-${gatewayData.id}-${eventName}`,
              {
                requestId,
                socketId: socket.id,
                userId: socket.data.userId || null,
                eventName,
                payload,
                gatewayId: gatewayData.id,
                gatewayPath: gatewayData.path,
                eventId: freshEvent?.id ?? event.id,
                script,
                timeout: freshEvent?.timeout ?? event.timeout,
              },
              {
                attempts: 0,
                removeOnComplete: {
                  count: 100,
                  age: 3600,
                },
                removeOnFail: {
                  count: 500,
                  age: 24 * 3600,
                },
              },
            );
          } catch (error) {
            this.logger.error(`Event handler failed for ${eventName}:`, error);
            if (typeof ack === 'function') {
              ack({
                queued: false,
                requestId,
                eventName,
                error: {
                  code: 'QUEUE_ERROR',
                  message: error?.message || 'Failed to queue event',
                },
              });
            }
            socket.emit('ws:error', {
              requestId,
              eventName,
              code: 'QUEUE_ERROR',
              message: error?.message || 'Failed to queue event',
            });
          }
        });
      }
    });
  }
  async handleConnection(_client: Socket) {}
  async handleDisconnect(_client: Socket) {}
  async reloadGateways() {
    this.logger.log('Reloading websocket gateways...');
    for (const path of this.registeredGateways) {
      const namespace = this.server.of(path);
      namespace.disconnectSockets();
      namespace.removeAllListeners();
    }
    this.registeredGateways.clear();
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
      this.server.of(path).to(room).emit(event, data);
    }
  }

  emitToNamespace(path: string, event: string, data: any) {
    this.server.of(path).emit(event, data);
  }

  emitToSocket(path: string, socketId: string, event: string, data: any) {
    this.server.of(path).to(socketId).emit(event, data);
  }

  emitToAll(event: string, data: any) {
    for (const path of this.registeredGateways) {
      this.server.of(path).emit(event, data);
    }
  }
}
