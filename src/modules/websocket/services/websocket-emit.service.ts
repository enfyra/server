import { DynamicWebSocketGateway } from '../gateway/dynamic-websocket.gateway';

export class WebsocketEmitService {
  private readonly dynamicWebSocketGateway: DynamicWebSocketGateway;

  constructor(deps: { dynamicWebSocketGateway: DynamicWebSocketGateway }) {
    this.dynamicWebSocketGateway = deps.dynamicWebSocketGateway;
  }

  emitToUser(userId: number | string, event: string, data: any) {
    this.dynamicWebSocketGateway.emitToUser(userId, event, data);
  }

  emitToRoom(room: string, event: string, data: any) {
    this.dynamicWebSocketGateway.emitToRoom(room, event, data);
  }

  emitToGateway(path: string, event: string, data: any) {
    this.dynamicWebSocketGateway.emitToNamespace(path, event, data);
  }

  broadcast(event: string, data: any) {
    this.dynamicWebSocketGateway.emitToAll(event, data);
  }

  roomSize(room: string): Promise<number> {
    return this.dynamicWebSocketGateway.roomSize(room);
  }
}
