import { Injectable } from '@nestjs/common';
import { Server } from 'socket.io';
import { DynamicWebSocketGateway } from './dynamic-websocket.gateway';

@Injectable()
export class WebsocketGatewayFactory {
  constructor(private readonly dynamicGateway: DynamicWebSocketGateway) {}

  async emitToUser(
    userId: number | string,
    event: string,
    data: any,
  ): Promise<void> {
    this.dynamicGateway.emitToUser(userId, event, data);
  }

  async emitToRoom(room: string, event: string, data: any): Promise<void> {
    this.dynamicGateway.emitToRoom(room, event, data);
  }

  async emitToGateway(path: string, event: string, data: any): Promise<void> {
    this.dynamicGateway.emitToNamespace(path, event, data);
  }

  async broadcast(event: string, data: any): Promise<void> {
    this.dynamicGateway.emitToAll(event, data);
  }

  async getServer(): Promise<Server> {
    return (this.dynamicGateway as any).server;
  }

  async registerGateways(): Promise<void> {
    await this.dynamicGateway.registerGateways();
  }
}
