import { Server } from 'socket.io';
import { DynamicWebSocketGateway } from './dynamic-websocket.gateway';

export class WebsocketGatewayFactory {
  private readonly dynamicWebSocketGateway: DynamicWebSocketGateway;

  constructor(deps: { dynamicWebSocketGateway: DynamicWebSocketGateway }) {
    this.dynamicWebSocketGateway = deps.dynamicWebSocketGateway;
  }

  async emitToUser(
    userId: number | string,
    event: string,
    data: any,
  ): Promise<void> {
    this.dynamicWebSocketGateway.emitToUser(userId, event, data);
  }

  async emitToRoom(room: string, event: string, data: any): Promise<void> {
    this.dynamicWebSocketGateway.emitToRoom(room, event, data);
  }

  async emitToGateway(path: string, event: string, data: any): Promise<void> {
    this.dynamicWebSocketGateway.emitToNamespace(path, event, data);
  }

  async broadcast(event: string, data: any): Promise<void> {
    this.dynamicWebSocketGateway.emitToAll(event, data);
  }

  async getServer(): Promise<Server> {
    return (this.dynamicWebSocketGateway as any).server;
  }

  async registerGateways(): Promise<void> {
    await this.dynamicWebSocketGateway.registerGateways();
  }
}
