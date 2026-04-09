import { Injectable } from '@nestjs/common';
import { DynamicWebSocketGateway } from '../gateway/dynamic-websocket.gateway';

@Injectable()
export class WebsocketEmitService {
  constructor(private readonly websocketGateway: DynamicWebSocketGateway) {}

  emitToUser(userId: number | string, event: string, data: any) {
    this.websocketGateway.emitToUser(userId, event, data);
  }

  emitToRoom(room: string, event: string, data: any) {
    this.websocketGateway.emitToRoom(room, event, data);
  }

  emitToGateway(path: string, event: string, data: any) {
    this.websocketGateway.emitToNamespace(path, event, data);
  }

  broadcast(event: string, data: any) {
    this.websocketGateway.emitToAll(event, data);
  }
}
