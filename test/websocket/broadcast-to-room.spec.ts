import { describe, expect, it, vi } from 'vitest';
import { DynamicWebSocketGateway, WebsocketContextFactory } from '../../src/modules/websocket';

function createGateway(socket: any) {
  const namespace = {
    sockets: new Map(socket ? [[socket.id, socket]] : []),
  };
  const server = {
    of: vi.fn(() => namespace),
  };
  const gateway = Object.create(DynamicWebSocketGateway.prototype);
  gateway.server = server;
  return { gateway: gateway as DynamicWebSocketGateway, server };
}

describe('DynamicWebSocketGateway.broadcastToRoom', () => {
  it('emits to a room within the requested gateway namespace', () => {
    const gateway = Object.create(DynamicWebSocketGateway.prototype);
    gateway.emitToNamespaceRoom = vi.fn();

    gateway.emitToRoom('/chat', 'conversation:1', 'chat:message', {
      text: 'hello',
    });

    expect(gateway.emitToNamespaceRoom).toHaveBeenCalledWith(
      '/chat',
      'conversation:1',
      'chat:message',
      { text: 'hello' },
    );
  });

  it('broadcasts to a room through the sender socket so the sender is excluded', () => {
    const emit = vi.fn();
    const to = vi.fn(() => ({ emit }));
    const socket = { id: 'socket-1', to };
    const { gateway, server } = createGateway(socket);

    gateway.broadcastToRoom('/chat', 'socket-1', 'conversation:1', 'chat:message', {
      text: 'hello',
    });

    expect(server.of).toHaveBeenCalledWith('/chat');
    expect(to).toHaveBeenCalledWith('conversation:1');
    expect(emit).toHaveBeenCalledWith('chat:message', { text: 'hello' });
  });

  it('does nothing when the sender socket is no longer connected', () => {
    const { gateway } = createGateway(null);

    expect(() =>
      gateway.broadcastToRoom('/chat', 'missing', 'conversation:1', 'chat:message', {
        text: 'hello',
      }),
    ).not.toThrow();
  });

  it('exposes broadcastToRoom on bound script socket context', () => {
    const broadcastToRoom = vi.fn();
    const factory = new WebsocketContextFactory({
      dynamicWebSocketGateway: { broadcastToRoom } as any,
    });

    const socket = factory.createBoundProxy('/chat', 'socket-1');
    socket.broadcastToRoom?.('conversation:1', 'chat:message', { text: 'hello' });

    expect(broadcastToRoom).toHaveBeenCalledWith(
      '/chat',
      'socket-1',
      'conversation:1',
      'chat:message',
      { text: 'hello' },
    );
  });

  it('exposes emitToCurrentRoom on bound script socket context', () => {
    const emitToNamespaceRoom = vi.fn();
    const factory = new WebsocketContextFactory({
      dynamicWebSocketGateway: { emitToNamespaceRoom } as any,
    });

    const socket = factory.createBoundProxy('/chat', 'socket-1');
    socket.emitToCurrentRoom?.('conversation:1', 'chat:message', { text: 'hello' });

    expect(emitToNamespaceRoom).toHaveBeenCalledWith(
      '/chat',
      'conversation:1',
      'chat:message',
      { text: 'hello' },
    );
  });
});
