import { Server } from 'socket.io';
import { io as ioClient, Socket as ClientSocket } from 'socket.io-client';
import { createAdapter } from '@socket.io/redis-adapter';
import Redis from 'ioredis';
import { createServer, Server as HttpServer } from 'http';
import { DynamicWebSocketGateway } from '../../src/modules/websocket/gateway/dynamic-websocket.gateway';

const REDIS_URI = process.env.REDIS_URI || 'redis://localhost:6379';

function wait(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
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

describe('DynamicWebSocketGateway.roomSize — pure mock', () => {
  function makeGateway(perPathSizes: Record<string, Record<string, number>>) {
    const gw = Object.create(DynamicWebSocketGateway.prototype);
    (gw as any).registeredGateways = new Set(Object.keys(perPathSizes));
    (gw as any).server = {
      of: (path: string) => ({
        in: (room: string) => ({
          allSockets: async () => {
            const size = perPathSizes[path]?.[room] ?? 0;
            return new Set(
              Array.from({ length: size }, (_, i) => `${path}:${room}:${i}`),
            );
          },
        }),
      }),
    };
    return gw as DynamicWebSocketGateway;
  }

  it('returns 0 when no gateways registered', async () => {
    const gw = makeGateway({});
    expect(await gw.roomSize('any_room')).toBe(0);
  });

  it('returns 0 when room is empty across all gateways', async () => {
    const gw = makeGateway({
      '/chat': { other_room: 5 },
      '/notify': {},
    });
    expect(await gw.roomSize('absent')).toBe(0);
  });

  it('returns count from single gateway', async () => {
    const gw = makeGateway({
      '/chat': { room_x: 3 },
    });
    expect(await gw.roomSize('room_x')).toBe(3);
  });

  it('aggregates count across multiple gateways with same room name', async () => {
    const gw = makeGateway({
      '/chat': { lobby: 4 },
      '/notify': { lobby: 2 },
      '/admin': { lobby: 1 },
    });
    expect(await gw.roomSize('lobby')).toBe(7);
  });

  it('only counts the requested room (ignores others)', async () => {
    const gw = makeGateway({
      '/chat': { room_a: 100, room_b: 50 },
    });
    expect(await gw.roomSize('room_a')).toBe(100);
    expect(await gw.roomSize('room_b')).toBe(50);
  });
});

describe('DynamicWebSocketGateway.roomSize — real Socket.IO + Redis adapter', () => {
  let httpServer: HttpServer;
  let ioServer: Server;
  let port: number;
  let pub: Redis;
  let sub: Redis;
  const NS = '/room-size-test';
  const KEY_PREFIX = `test-roomsize-${Date.now()}:socket.io:`;
  const clients: ClientSocket[] = [];

  beforeAll(async () => {
    httpServer = createServer();
    ioServer = new Server(httpServer, { cors: { origin: '*' } });
    pub = new Redis(REDIS_URI, { lazyConnect: true });
    sub = pub.duplicate();
    await Promise.all([pub.connect(), sub.connect()]);
    ioServer.adapter(createAdapter(pub, sub, { key: KEY_PREFIX }));
    ioServer.of(NS).on('connection', (socket) => {
      socket.on('join', (room: string, ack: any) => {
        socket.join(room);
        if (typeof ack === 'function') ack({ ok: true });
      });
      socket.on('leave', (room: string, ack: any) => {
        socket.leave(room);
        if (typeof ack === 'function') ack({ ok: true });
      });
    });
    await new Promise<void>((resolve) => {
      httpServer.listen(0, () => {
        port = (httpServer.address() as any).port;
        resolve();
      });
    });
  });

  afterAll(async () => {
    for (const c of clients) {
      if (c.connected) c.disconnect();
    }
    ioServer.disconnectSockets(true);
    await new Promise<void>((resolve) => ioServer.close(() => resolve()));
    await new Promise<void>((resolve) => httpServer.close(() => resolve()));
    pub.disconnect();
    sub.disconnect();
    await wait(50);
  });

  function buildGatewayBoundTo(server: Server, paths: string[]) {
    const gw = Object.create(DynamicWebSocketGateway.prototype);
    (gw as any).registeredGateways = new Set(paths);
    (gw as any).server = server;
    return gw as DynamicWebSocketGateway;
  }

  it('counts real connected sockets in a room', async () => {
    const gw = buildGatewayBoundTo(ioServer, [NS]);

    const c1 = ioClient(`http://localhost:${port}${NS}`, {
      transports: ['websocket'],
      forceNew: true,
    });
    const c2 = ioClient(`http://localhost:${port}${NS}`, {
      transports: ['websocket'],
      forceNew: true,
    });
    const c3 = ioClient(`http://localhost:${port}${NS}`, {
      transports: ['websocket'],
      forceNew: true,
    });
    clients.push(c1, c2, c3);
    await Promise.all([
      waitConnected(c1),
      waitConnected(c2),
      waitConnected(c3),
    ]);

    await new Promise<void>((resolve) =>
      c1.emit('join', 'meeting_42', () => resolve()),
    );
    await new Promise<void>((resolve) =>
      c2.emit('join', 'meeting_42', () => resolve()),
    );
    await new Promise<void>((resolve) =>
      c3.emit('join', 'other_room', () => resolve()),
    );
    await wait(50);

    expect(await gw.roomSize('meeting_42')).toBe(2);
    expect(await gw.roomSize('other_room')).toBe(1);
    expect(await gw.roomSize('empty_room')).toBe(0);
  });

  it('decreases count after a socket leaves the room', async () => {
    const gw = buildGatewayBoundTo(ioServer, [NS]);

    const a = ioClient(`http://localhost:${port}${NS}`, {
      transports: ['websocket'],
      forceNew: true,
    });
    const b = ioClient(`http://localhost:${port}${NS}`, {
      transports: ['websocket'],
      forceNew: true,
    });
    clients.push(a, b);
    await Promise.all([waitConnected(a), waitConnected(b)]);

    await new Promise<void>((resolve) =>
      a.emit('join', 'leavable', () => resolve()),
    );
    await new Promise<void>((resolve) =>
      b.emit('join', 'leavable', () => resolve()),
    );
    await wait(50);
    expect(await gw.roomSize('leavable')).toBe(2);

    await new Promise<void>((resolve) =>
      a.emit('leave', 'leavable', () => resolve()),
    );
    await wait(50);
    expect(await gw.roomSize('leavable')).toBe(1);
  });

  it('decreases count after socket disconnects', async () => {
    const gw = buildGatewayBoundTo(ioServer, [NS]);

    const x = ioClient(`http://localhost:${port}${NS}`, {
      transports: ['websocket'],
      forceNew: true,
    });
    const y = ioClient(`http://localhost:${port}${NS}`, {
      transports: ['websocket'],
      forceNew: true,
    });
    clients.push(x, y);
    await Promise.all([waitConnected(x), waitConnected(y)]);

    await new Promise<void>((resolve) =>
      x.emit('join', 'disconnect_room', () => resolve()),
    );
    await new Promise<void>((resolve) =>
      y.emit('join', 'disconnect_room', () => resolve()),
    );
    await wait(50);
    expect(await gw.roomSize('disconnect_room')).toBe(2);

    x.disconnect();
    await wait(100);
    expect(await gw.roomSize('disconnect_room')).toBe(1);

    y.disconnect();
    await wait(100);
    expect(await gw.roomSize('disconnect_room')).toBe(0);
  });
});
