import { Server } from 'socket.io';
import { io, Socket as ClientSocket } from 'socket.io-client';
import { createServer, Server as HttpServer } from 'http';
import { DynamicWebSocketGateway } from '../../src/modules/websocket/gateway/dynamic-websocket.gateway';
import { EventQueueService } from '../../src/modules/websocket/queues/event-queue.service';

function waitForEvent<T = any>(
  socket: ClientSocket,
  event: string,
  timeoutMs = 3000,
): Promise<T> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`Timeout waiting for "${event}"`)),
      timeoutMs,
    );
    socket.once(event, (data: T) => {
      clearTimeout(timer);
      resolve(data);
    });
  });
}

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

describe('WebSocket DX (ACK + ws:result/ws:error)', () => {
  let httpServer: HttpServer;
  let ioServer: Server;
  let port: number;
  let gateway: DynamicWebSocketGateway;
  const clients: ClientSocket[] = [];

  const NAMESPACE = '/dx';
  const EVENT_NAME = 'ping';

  beforeAll(async () => {
    httpServer = createServer();
    ioServer = new Server(httpServer, { cors: { origin: '*' } });
    port = await new Promise<number>((resolve) => {
      httpServer.listen(0, () => resolve((httpServer.address() as any).port));
    });

    const configService = {
      get: () => undefined,
    } as any;

    const connectionQueue = { add: jest.fn() } as any;
    const eventQueue = { add: jest.fn() } as any;
    const websocketCache = { getGatewayByPath: jest.fn() } as any;
    const builtInRegistry = {
      getConnectionScript: jest.fn().mockReturnValue(null),
      getEventScript: jest.fn().mockReturnValue(null),
    } as any;

    gateway = new DynamicWebSocketGateway(
      configService,
      connectionQueue,
      eventQueue,
      websocketCache,
      builtInRegistry,
    );

    (gateway as any).server = ioServer;

    const gatewayConfig = {
      id: 1,
      path: NAMESPACE,
      requireAuth: false,
      connectionHandlerScript: null,
      events: [
        {
          id: 10,
          eventName: EVENT_NAME,
          handlerScript: 'return { ok: true }',
          timeout: 500,
        },
      ],
    };

    (gateway as any).gatewayConfigsByPath.set(NAMESPACE, gatewayConfig);
    websocketCache.getGatewayByPath.mockResolvedValue(gatewayConfig);
    (gateway as any).setupNamespace(NAMESPACE);
  }, 15000);

  afterEach(() => {
    while (clients.length) clients.pop()?.disconnect();
  });

  afterAll(async () => {
    ioServer.disconnectSockets(true);
    await new Promise<void>((resolve) => ioServer.close(() => resolve()));
    await new Promise<void>((resolve) => httpServer.close(() => resolve()));
  });

  function connectClient(): ClientSocket {
    const c = io(`http://localhost:${port}${NAMESPACE}`, {
      transports: ['websocket'],
      forceNew: true,
    });
    clients.push(c);
    return c;
  }

  it('acks immediately with requestId and eventName', async () => {
    const client = connectClient();
    await waitConnected(client);

    const ack = await new Promise<any>((resolve) => {
      client.emit(EVENT_NAME, { hello: 1 }, (a: any) => resolve(a));
    });

    expect(ack.queued).toBe(true);
    expect(typeof ack.requestId).toBe('string');
    expect(ack.requestId.length).toBeGreaterThan(10);
    expect(ack.eventName).toBe(EVENT_NAME);
  });

  it('ws:result includes same requestId and script logs', async () => {
    const captured: any[] = [];
    (gateway as any).eventQueue.add.mockImplementation(
      async (_name: string, data: any) => {
        captured.push(data);
      },
    );

    const handlerExecutor = {
      run: async (_code: string, ctx: any) => {
        ctx.$logs('hello');
        return { ok: true };
      },
    } as any;
    const repoRegistryService = { createReposProxy: () => ({}) } as any;
    const flowService = { trigger: jest.fn() } as any;
    const eventQueueSvc = new EventQueueService(
      handlerExecutor,
      gateway as any,
      repoRegistryService,
      flowService,
    );

    const client = connectClient();
    await waitConnected(client);

    const ack = await new Promise<any>((resolve) => {
      client.emit(EVENT_NAME, { hello: 1 }, (a: any) => resolve(a));
    });
    expect(ack.queued).toBe(true);
    expect(captured.length).toBe(1);
    expect(captured[0].requestId).toBe(ack.requestId);

    const resultPromise = waitForEvent<any>(client, 'ws:result');
    await eventQueueSvc.process({ data: captured[0] } as any);
    const wsResult = await resultPromise;

    expect(wsResult.requestId).toBe(ack.requestId);
    expect(wsResult.eventName).toBe(EVENT_NAME);
    expect(wsResult.success).toBe(true);
    expect(wsResult.result).toEqual({ ok: true });
    expect(wsResult.logs).toEqual(['hello']);
  });

  it('ws:error includes same requestId when handler throws', async () => {
    const captured: any[] = [];
    (gateway as any).eventQueue.add.mockImplementation(
      async (_name: string, data: any) => {
        captured.push(data);
      },
    );

    const handlerExecutor = {
      run: async () => {
        throw new Error('boom');
      },
    } as any;
    const repoRegistryService = { createReposProxy: () => ({}) } as any;
    const flowService = { trigger: jest.fn() } as any;
    const eventQueueSvc = new EventQueueService(
      handlerExecutor,
      gateway as any,
      repoRegistryService,
      flowService,
    );

    const client = connectClient();
    await waitConnected(client);

    const ack = await new Promise<any>((resolve) => {
      client.emit(EVENT_NAME, { hello: 1 }, (a: any) => resolve(a));
    });
    expect(ack.queued).toBe(true);
    expect(captured.length).toBe(1);

    const errPromise = waitForEvent<any>(client, 'ws:error');
    await eventQueueSvc.process({ data: captured[0] } as any);
    const wsErr = await errPromise;

    expect(wsErr.requestId).toBe(ack.requestId);
    expect(wsErr.eventName).toBe(EVENT_NAME);
    expect(wsErr.success).toBe(false);
    expect(typeof wsErr.message).toBe('string');
  });
});
