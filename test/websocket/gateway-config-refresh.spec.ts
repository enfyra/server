import { describe, expect, it, vi } from 'vitest';
import { DynamicWebSocketGateway } from '../../src/modules/websocket';

function createGateway(
  previousGateway: Record<string, unknown>,
  nextGateway: Record<string, unknown>,
) {
  const namespace = { disconnectSockets: vi.fn() };
  const gateway = Object.create(
    DynamicWebSocketGateway.prototype,
  ) as DynamicWebSocketGateway & Record<string, any>;
  gateway.server = { of: vi.fn(() => namespace) } as any;
  gateway.registeredGateways = new Set(['/usage']);
  gateway.gatewayConfigsByPath = new Map([['/usage', previousGateway]]);
  gateway.runtimeRegistryService = { requireActiveData: () => [nextGateway] };
  gateway.logger = { log: vi.fn(), error: vi.fn() };
  return { gateway, namespace };
}

describe('DynamicWebSocketGateway gateway refresh', () => {
  it('authenticates native PAT handshake headers through the shared authentication service', async () => {
    let middleware: ((socket: any, next: (error?: Error) => void) => Promise<void>) | undefined;
    const namespace = {
      use: vi.fn((handler) => {
        middleware = handler;
      }),
      on: vi.fn(),
    };
    const gateway = Object.create(
      DynamicWebSocketGateway.prototype,
    ) as DynamicWebSocketGateway & Record<string, any>;
    const user = { id: 'user-1', isRootAdmin: true, roles: [] };
    const authenticate = vi.fn().mockResolvedValue({ user });
    gateway.server = { of: vi.fn(() => namespace) } as any;
    gateway.gatewayConfigsByPath = new Map([['/enfyra-admin', { path: '/enfyra-admin', requireAuth: true }]]);
    gateway.lazyRef = { authenticationService: { authenticate } };
    gateway.logger = { warn: vi.fn() };

    gateway.setupNamespace('/enfyra-admin');
    const socket = {
      handshake: {
        headers: { 'x-enfyra-pat': 'efy_pat_test' },
        auth: {},
      },
      data: {},
      conn: { close: vi.fn() },
    };
    const next = vi.fn();

    await middleware?.(socket, next);

    expect(authenticate).toHaveBeenCalledWith({
      headers: socket.handshake.headers,
    });
    expect(socket.data.user).toBe(user);
    expect(socket.data.userId).toBe('user-1');
    expect(next).toHaveBeenCalledWith();
  });

  it('reconnects current sockets when a connection script changes so new room membership applies', async () => {
    const previousGateway = {
      path: '/usage',
      requireAuth: true,
      sourceCode: "@SOCKET.join('notifications:all')",
      scriptLanguage: 'javascript',
      connectionHandlerTimeout: 5000,
      events: [],
    };
    const nextGateway = {
      ...previousGateway,
      sourceCode: "@SOCKET.join('notifications:all-v2')",
    };
    const { gateway, namespace } = createGateway(previousGateway, nextGateway);

    await gateway.registerGateways();

    expect(namespace.disconnectSockets).toHaveBeenCalledWith(true);
  });

  it('keeps current sockets connected when the effective gateway contract is unchanged', async () => {
    const config = {
      path: '/usage',
      requireAuth: true,
      sourceCode: "@SOCKET.join('notifications:all')",
      scriptLanguage: 'javascript',
      connectionHandlerTimeout: 5000,
      events: [],
    };
    const { gateway, namespace } = createGateway(config, { ...config });

    await gateway.registerGateways();

    expect(namespace.disconnectSockets).not.toHaveBeenCalled();
  });

  it('reconnects current sockets when an event handler changes so the replacement binds on reconnect', async () => {
    const previousGateway = {
      path: '/usage',
      requireAuth: true,
      sourceCode: null,
      scriptLanguage: 'javascript',
      connectionHandlerTimeout: 5000,
      events: [
        {
          id: 1,
          eventName: 'notification.created',
          sourceCode: 'return 1',
          isEnabled: true,
        },
      ],
    };
    const nextGateway = {
      ...previousGateway,
      events: [
        {
          id: 1,
          eventName: 'notification.created',
          sourceCode: 'return 2',
          isEnabled: true,
        },
      ],
    };
    const { gateway, namespace } = createGateway(previousGateway, nextGateway);

    await gateway.registerGateways();

    expect(namespace.disconnectSockets).toHaveBeenCalledWith(true);
  });
});
