import {
  WebSocketGateway,
  WebSocketServer,
  SubscribeMessage,
  OnGatewayConnection,
  OnGatewayDisconnect,
  OnGatewayInit,
  ConnectedSocket,
  MessageBody,
} from '@nestjs/websockets';
import { Injectable, Logger } from '@nestjs/common';
import { OnEvent } from '@nestjs/event-emitter';
import { CACHE_EVENTS } from '../../../shared/utils/cache-events.constants';
import { Server, Socket } from 'socket.io';
import { InjectQueue } from '@nestjs/bullmq';
import { Queue } from 'bullmq';
import { ConfigService } from '@nestjs/config';
import { JwtService } from '@nestjs/jwt';
import { WebsocketCacheService } from '../../../infrastructure/cache/services/websocket-cache.service';
import { RedisPubSubService } from '../../../infrastructure/cache/services/redis-pubsub.service';
import { WEBSOCKET_CACHE_SYNC_EVENT_KEY } from '../../../shared/utils/constant';
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
export class DynamicWebSocketGateway implements OnGatewayInit, OnGatewayConnection, OnGatewayDisconnect {
  @WebSocketServer()
  server: Server;
  private readonly logger = new Logger(DynamicWebSocketGateway.name);
  private registeredGateways = new Set<string>();
  private gatewayConfigsByPath = new Map<string, any>();
  constructor(
    private readonly configService: ConfigService,
    @InjectQueue('ws-connection')
    private readonly connectionQueue: Queue,
    @InjectQueue('ws-event')
    private readonly eventQueue: Queue,
    private readonly websocketCache: WebsocketCacheService,
    private readonly redisPubSubService: RedisPubSubService,
  ) {
    this.jwtService = new JwtService({
      secret: this.configService.get('SECRET_KEY'),
    });
  }
  private readonly jwtService: JwtService;
  afterInit(server: Server) {
    this.logger.log('WebSocket Gateway initialized');
    this.subscribeToCacheSync();
  }
  @OnEvent(CACHE_EVENTS.WEBSOCKET_LOADED)
  async onWebsocketCacheLoaded() {
    await this.registerGateways();
  }
  private subscribeToCacheSync() {
    this.redisPubSubService.subscribeWithHandler(
      WEBSOCKET_CACHE_SYNC_EVENT_KEY,
      async (channel: string, message: string) => {
        const isWebsocketChannel = channel === WEBSOCKET_CACHE_SYNC_EVENT_KEY || channel.startsWith(WEBSOCKET_CACHE_SYNC_EVENT_KEY + ':');
        if (isWebsocketChannel) {
          try {
            const payload = JSON.parse(message);
            const gateways: any[] = payload.gateways || [];
            const newPaths = new Set(gateways.map((g: any) => g.path));
            const oldPaths = new Set(this.registeredGateways);
            this.updateGatewayConfigs(gateways);
            for (const path of oldPaths) {
              if (!newPaths.has(path)) {
                const namespace = this.server.of(path);
                namespace.disconnectSockets();
                namespace.removeAllListeners();
                this.registeredGateways.delete(path);
                this.gatewayConfigsByPath.delete(path);
              }
            }
            for (const gateway of gateways) {
              if (!this.registeredGateways.has(gateway.path)) {
                this.setupNamespace(gateway.path);
                this.registeredGateways.add(gateway.path);
              }
            }
          } catch (error) {
            this.logger.error('Failed to process websocket cache sync:', error);
          }
        }
      }
    );
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
        const { token } = socket.handshake.auth;
        if (!token) {
          const err = new Error('Authentication token required');
          (err as any).data = { code: 'AUTH_REQUIRED', path };
          this.logger.warn(`Connection rejected: no token provided for ${path}`);
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
      const gatewayData = this.gatewayConfigsByPath.get(path) ?? socket.data.gateway;
      if (!gatewayData) {
        this.logger.warn(`Connection rejected: missing gateway data for ${path}`);
        socket.emit('auth_error', { code: 'AUTH_REJECTED', message: 'Authentication failed' });
        socket.disconnect(true);
        return;
      }
      if (gatewayData.requireAuth && !socket.data.userId) {
        this.logger.warn(`Connection rejected: auth required but no user for ${gatewayData.path}`);
        socket.emit('auth_error', { code: 'AUTH_REQUIRED', message: 'Authentication token required' });
        socket.disconnect(true);
        return;
      }
      const userId = socket.data.userId || socket.id;
      this.logger.debug(`Client connected to ${gatewayData.path}: ${socket.id} (user: ${userId})`);
      if (gatewayData.connectionHandlerScript) {
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
              script: gatewayData.connectionHandlerScript,
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
          this.logger.error(`Connection handler failed for ${socket.id}:`, error);
          socket.disconnect();
          return;
        }
      }
      const roomName = socket.data.userId ? `user_${socket.data.userId}` : `user_${socket.id}`;
      socket.join(roomName);
      this.logger.debug(`Socket ${socket.id} joined room ${roomName}`);
      for (const event of gatewayData.events) {
        const eventName = event.eventName;
        socket.on(eventName, async (payload) => {
          this.logger.debug(`Event ${eventName} received from ${socket.id} on ${gatewayData.path}`);
          try {
            const freshGateway = await this.websocketCache.getGatewayByPath(gatewayData.path);
            const freshEvent = freshGateway?.events?.find((e: any) => e.eventName === eventName);
            const script = freshEvent?.handlerScript ?? event.handlerScript;

            if (!script) {
              this.logger.warn(`No handler for event ${eventName} on ${gatewayData.path}`);
              return;
            }
            await this.eventQueue.add(
              `ws-event-${gatewayData.id}-${eventName}`,
              {
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
            socket.emit('error', { event: eventName, message: error.message });
          }
        });
      }
      socket.on('disconnect', () => {
        this.logger.debug(`Client disconnected from ${gatewayData.path}: ${socket.id}`);
      });
    });
  }
  async handleConnection(client: Socket) {
  }
  async handleDisconnect(client: Socket) {
  }
  async reloadGateways() {
    this.logger.log('Reloading websocket gateways...');
    for (const path of this.registeredGateways) {
      const namespace = this.server.of(path);
      namespace.disconnectSockets();
      namespace.removeAllListeners();
    }
    this.registeredGateways.clear();
    await this.registerGateways();
    this.logger.log(`Gateways reloaded. Total registered: ${this.registeredGateways.size}`);
  }
  emitToUser(userId: number | string, event: string, data: any) {
    this.logger.debug(`Emitting to user_${userId}: ${event} ${JSON.stringify(data)}`);
    this.server.to(`user_${userId}`).emit(event, data);
  }
  emitToRoom(room: string, event: string, data: any) {
    this.logger.debug(`Emitting to room ${room}: ${event} ${JSON.stringify(data)}`);
    this.server.to(room).emit(event, data);
  }
  emitToNamespace(path: string, event: string, data: any) {
    this.logger.debug(`Emitting to namespace ${path}: ${event} ${JSON.stringify(data)}`);
    this.server.of(path).emit(event, data);
  }
  emitToSocket(path: string, socketId: string, event: string, data: any) {
    this.logger.debug(`Emitting to socket ${socketId} on ${path}: ${event}`);
    this.server.of(path).to(socketId).emit(event, data);
  }
  emitToAll(event: string, data: any) {
    this.logger.debug(`Emitting to all: ${event} ${JSON.stringify(data)}`);
    this.server.emit(event, data);
  }
}