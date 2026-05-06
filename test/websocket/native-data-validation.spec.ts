import { createServer, type Server as HttpServer } from 'http';
import { Server } from 'socket.io';
import { io as ioClient, type Socket as ClientSocket } from 'socket.io-client';
import { DynamicWebSocketGateway } from '../../src/modules/websocket';

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

describe('DynamicWebSocketGateway native dataShape validation — real socket E2E', () => {
  const namespace = `/native-validation-${Date.now()}`;
  let httpServer: HttpServer;
  let ioServer: Server;
  let port: number;
  let client: ClientSocket;

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
                dataShape: [
                  { name: 'userId', type: 'string', required: true },
                  { name: 'displayName', type: 'string', required: true },
                ],
                socketAction: {
                  action: 'reply',
                  event: 'profile:accepted',
                  payloadExpression: {
                    id: '{{ data.userId }}',
                    name: '{{ data.displayName }}',
                  },
                },
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

  it('accepts valid payload and resolves native payload templates', async () => {
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
  });

  it('rejects missing required fields before native socket actions run', async () => {
    const errorEvent = new Promise<any>((resolve) => {
      client.once('ws:error', resolve);
    });
    let emitted = false;
    client.once('profile:accepted', () => {
      emitted = true;
    });

    const ack = await emitWithAck(client, 'profile:update', {
      userId: 'user_1',
    });

    expect(ack).toMatchObject({
      accepted: false,
      queued: false,
      eventName: 'profile:update',
      error: {
        code: 'WS_HANDLER_ERROR',
        message: 'Missing required websocket payload field "displayName"',
      },
    });
    await expect(errorEvent).resolves.toMatchObject({
      eventName: 'profile:update',
      code: 'WS_PAYLOAD_VALIDATION_ERROR',
      message: 'Missing required websocket payload field "displayName"',
    });
    expect(emitted).toBe(false);
  });
});
