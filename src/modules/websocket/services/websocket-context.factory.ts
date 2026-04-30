import { TDynamicContext } from '../../../shared/types';
import { DynamicWebSocketGateway } from '../gateway/dynamic-websocket.gateway';

export type SocketEmitCapture = Array<{ method: string; args: any[] }>;

export class WebsocketContextFactory {
  private readonly dynamicWebSocketGateway: DynamicWebSocketGateway;

  constructor(deps: { dynamicWebSocketGateway: DynamicWebSocketGateway }) {
    this.dynamicWebSocketGateway = deps.dynamicWebSocketGateway;
  }

  createGlobalProxy(): TDynamicContext['$socket'] {
    return {
      emitToUser: (userId: any, event: string, data: any) => {
        this.dynamicWebSocketGateway.emitToUser(userId, event, data);
      },
      emitToRoom: (room: string, event: string, data: any) => {
        this.dynamicWebSocketGateway.emitToRoom(room, event, data);
      },
      emitToGateway: (path: string, event: string, data: any) => {
        this.dynamicWebSocketGateway.emitToNamespace(path, event, data);
      },
      broadcast: (event: string, data: any) => {
        this.dynamicWebSocketGateway.emitToAll(event, data);
      },
      roomSize: (room: string) => this.dynamicWebSocketGateway.roomSize(room),
    };
  }

  createBoundProxy(
    gatewayPath: string,
    socketId: string,
  ): TDynamicContext['$socket'] {
    return {
      ...this.createGlobalProxy(),
      join: (room: string) => {
        this.dynamicWebSocketGateway.joinRoom(gatewayPath, socketId, room);
      },
      leave: (room: string) => {
        this.dynamicWebSocketGateway.leaveRoom(gatewayPath, socketId, room);
      },
      reply: (event: string, data: any) => {
        this.dynamicWebSocketGateway.emitToSocket(
          gatewayPath,
          socketId,
          event,
          data,
        );
      },
      disconnect: () => {
        this.dynamicWebSocketGateway.disconnectSocket(gatewayPath, socketId);
      },
    };
  }

  createCaptureProxy(emitted: SocketEmitCapture): TDynamicContext['$socket'] {
    const capture =
      (method: string) =>
      (...args: any[]) =>
        emitted.push({ method, args });

    return {
      join: capture('join'),
      leave: capture('leave'),
      reply: capture('reply'),
      emitToUser: capture('emitToUser'),
      emitToRoom: capture('emitToRoom'),
      emitToGateway: capture('emitToGateway'),
      broadcast: capture('broadcast'),
      roomSize: async () => 0,
      disconnect: capture('disconnect'),
    };
  }
}
