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

    const gateway = new DynamicWebSocketGateway({
      websocketCacheService: {
        getGateways: async () => [
          {
            path: namespace,
            requireAuth: false,
            events: [
              {
                eventName: 'profile:update',
                handlerScript:
                  'return await $ctx.$socket.reply("profile:accepted", { id: $ctx.$data.userId, name: $ctx.$data.displayName });',
              },
              {
                eventName: 'legacy-config:ignored',
              },
            ],
          },
        ],
      } as any,
      builtInSocketRegistry: {
        getConnectionScript: () => null,
        getEventScript: () => null,
      } as any,
      eventEmitter: { on: () => undefined } as any,
      envService: { get: () => undefined } as any,
      queryBuilderService: {} as any,
      cacheService: {} as any,
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
      queued: false,
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
    );
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
