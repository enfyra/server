import { createServer, type Server as HttpServer } from 'http';
import { Server } from 'socket.io';
import { io as ioClient, type Socket as ClientSocket } from 'socket.io-client';
import { vi } from 'vitest';
import {
  DynamicWebSocketGateway,
  WebsocketContextFactory,
} from '../../src/modules/websocket';
import { DynamicContextFactory } from '../../src/shared/services';

function waitConnected(client: ClientSocket, timeoutMs = 3000): Promise<void> {
  return new Promise((resolve, reject) => {
    if (client.connected) return resolve();
    const timer = setTimeout(
      () => reject(new Error('Client connect timeout')),
      timeoutMs,
    );
    client.once('connect', () => {
      clearTimeout(timer);
      resolve();
    });
    client.once('connect_error', (err) => {
      clearTimeout(timer);
      reject(err);
    });
  });
}

function emitWithAck<T = any>(
  client: ClientSocket,
  event: string,
  payload: any,
): Promise<T> {
  return new Promise((resolve) => {
    client.emit(event, payload, (ack: T) => resolve(ack));
  });
}

describe('DynamicWebSocketGateway event script execution — real socket E2E', () => {
  const namespace = `/script-event-${Date.now()}`;
  let httpServer: HttpServer;
  let ioServer: Server;
  let port: number;
  let client: ClientSocket;
  let gateway: DynamicWebSocketGateway;
  let gateways: any[];
  const authenticate = vi.fn(async () => ({
    user: { id: 'socket-user', isRootAdmin: false },
    payload: { id: 'socket-user' },
  }));
  const executorRun = vi.fn(async (script: string, ctx: any) => {
    if (script.includes('profile:accepted')) {
      ctx.$socket.reply('profile:accepted', {
        id: ctx.$data.userId,
        name: ctx.$data.displayName,
      });
      return { handled: true };
    }
    return { handled: true };
  });

  beforeAll(async () => {
    httpServer = createServer();
    ioServer = new Server(httpServer, { cors: { origin: '*' } });

    gateways = [
      {
        path: namespace,
        requireAuth: false,
        events: [
          {
            eventName: 'profile:update',
            handlerScript:
              'return await $ctx.$socket.reply("profile:accepted", { id: $ctx.$data.userId, name: $ctx.$data.displayName });',
          },
          { eventName: 'legacy-config:ignored' },
        ],
      },
    ];
    gateway = new DynamicWebSocketGateway({
      runtimeRegistryService: {
        requireActiveData: () => gateways,
      } as any,
      builtInSocketRegistry: {
        getConnectionScript: () => null,
        getEventScript: () => null,
      } as any,
      eventEmitter: { on: () => undefined } as any,
      envService: { get: () => undefined } as any,
      queryBuilderService: {} as any,
      redisAdminService: {} as any,
      lazyRef: {} as any,
    });
    const dynamicContextFactory = new DynamicContextFactory({
      bcryptService: {} as any,
      userCacheService: {} as any,
      envService: { get: () => undefined } as any,
      websocketContextFactory: new WebsocketContextFactory({
        dynamicWebSocketGateway: gateway,
      }),
    });
    (gateway as any).lazyRef = {
      dynamicContextFactory,
      executorEngineService: { run: executorRun },
      repoRegistryService: { createReposProxy: () => ({}) },
      flowService: { trigger: vi.fn() },
      authenticationService: { authenticate },
    };

    (gateway as any).server = ioServer;
    await gateway.registerGateways();

    await new Promise<void>((resolve) => {
      httpServer.listen(0, () => {
        port = (httpServer.address() as any).port;
        resolve();
      });
    });
  });

  afterAll(async () => {
    if (client?.connected) client.disconnect();
    ioServer.disconnectSockets(true);
    await new Promise<void>((resolve) => ioServer.close(() => resolve()));
    await new Promise<void>((resolve) => httpServer.close(() => resolve()));
  });

  beforeEach(async () => {
    client = ioClient(`http://localhost:${port}${namespace}`, {
      transports: ['websocket'],
      forceNew: true,
    });
    await waitConnected(client);
  });

  afterEach(() => {
    if (client.connected) client.disconnect();
  });

  it('runs event handler scripts inline through the executor', async () => {
    const received = new Promise<any>((resolve) => {
      client.once('profile:accepted', resolve);
    });

    const ack = await emitWithAck(client, 'profile:update', {
      userId: 'user_1',
      displayName: 'Alice',
    });

    expect(ack).toMatchObject({
      accepted: true,
      queued: true,
      eventName: 'profile:update',
    });
    await expect(received).resolves.toEqual({
      id: 'user_1',
      name: 'Alice',
    });
    expect(executorRun).toHaveBeenCalledWith(
      'return await $ctx.$socket.reply("profile:accepted", { id: $ctx.$data.userId, name: $ctx.$data.displayName });',
      expect.objectContaining({
        $data: {
          userId: 'user_1',
          displayName: 'Alice',
        },
      }),
      undefined,
      expect.objectContaining({
        sourceCode:
          'return await $ctx.$socket.reply("profile:accepted", { id: $ctx.$data.userId, name: $ctx.$data.displayName });',
        scriptLanguage: 'typescript',
      }),
    );
  });

  it('keeps an unchanged client connected when another gateway contract changes', async () => {
    const otherPath = `${namespace}-other`;
    const otherGateway = {
      path: otherPath,
      requireAuth: false,
      events: [],
      sourceCode: 'return 1;',
    };
    gateways = [...gateways, otherGateway];
    await gateway.refreshGateways();
    const other = ioClient(`http://localhost:${port}${otherPath}`, {
      transports: ['websocket'],
      forceNew: true,
      reconnectionDelay: 10,
      reconnectionDelayMax: 20,
    });
    const disconnected = vi.fn();
    client.on('disconnect', disconnected);
    try {
      await waitConnected(other);
      const otherDisconnected = new Promise<void>((resolve) =>
        other.once('disconnect', () => resolve()),
      );
      gateways = gateways.map((entry) =>
        entry.path === otherPath
          ? { ...entry, sourceCode: 'return 2;' }
          : entry,
      );
      await gateway.refreshGateways();
      await otherDisconnected;
      await waitConnected(other);
      expect(other.connected).toBe(true);
      for (let index = 0; index < 3; index++) await gateway.refreshGateways();
      expect(client.connected).toBe(true);
      expect(disconnected).not.toHaveBeenCalled();
      expect(
        await emitWithAck(client, 'legacy-config:ignored', {}),
      ).toMatchObject({ error: { code: 'NO_HANDLER' } });
    } finally {
      other.disconnect();
      client.off('disconnect', disconnected);
      gateways = gateways.filter((entry) => entry.path !== otherPath);
      await gateway.refreshGateways();
    }
  });

  it('authenticates once per handshake after refresh and disable/re-enable cycles', async () => {
    const authPath = `${namespace}-auth`;
    const authGateway = { path: authPath, requireAuth: true, events: [] };
    let authenticatedClient: ClientSocket | undefined;
    try {
      for (let index = 0; index < 3; index++) {
        gateways = [...gateways, authGateway];
        await gateway.refreshGateways();
        gateways = gateways.filter((entry) => entry.path !== authPath);
        await gateway.refreshGateways();
      }
      gateways = [...gateways, authGateway];
      await gateway.refreshGateways();
      authenticate.mockClear();
      authenticatedClient = ioClient(`http://localhost:${port}${authPath}`, {
        transports: ['websocket'],
        forceNew: true,
        reconnection: false,
      });
      await waitConnected(authenticatedClient);
      expect(authenticate).toHaveBeenCalledOnce();
      authenticatedClient.disconnect();
      authenticate.mockClear();
      authenticate.mockRejectedValueOnce(new Error('revoked credential'));
      authenticatedClient = ioClient(`http://localhost:${port}${authPath}`, {
        transports: ['websocket'],
        forceNew: true,
        reconnection: false,
      });
      await expect(waitConnected(authenticatedClient)).rejects.toThrow(
        'Invalid authentication token',
      );
      expect(authenticate).toHaveBeenCalledOnce();
      expect(authenticatedClient.connected).toBe(false);
    } finally {
      authenticatedClient?.disconnect();
      gateways = gateways.filter((entry) => entry.path !== authPath);
      await gateway.refreshGateways();
    }
  });

  it('rejects events without handler scripts', async () => {
    const ack = await emitWithAck(client, 'legacy-config:ignored', {});

    expect(ack).toMatchObject({
      accepted: false,
      queued: false,
      eventName: 'legacy-config:ignored',
      error: {
        code: 'NO_HANDLER',
        message: 'No handler configured',
      },
    });
  });
});
