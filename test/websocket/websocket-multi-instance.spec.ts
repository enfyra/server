import { Server } from 'socket.io';
import { io, Socket as ClientSocket } from 'socket.io-client';
import { createAdapter } from '@socket.io/redis-adapter';
import Redis from 'ioredis';
import { createServer, Server as HttpServer } from 'http';

/**
 * Multi-instance WebSocket tests.
 *
 * Spins up TWO independent Socket.IO servers (simulating two Enfyra instances)
 * backed by the same Redis adapter. Verifies that emits on one server reach
 * clients connected to the other.
 *
 * Requires a running Redis on localhost:6379 (or REDIS_URI env).
 */

const REDIS_URI = process.env.REDIS_URI || 'redis://localhost:6379';
const KEY_PREFIX = `test-${Date.now()}:socket.io:`;
const NAMESPACE = '/test-chat';
const WAIT_MS = 500;

function wait(ms = WAIT_MS) {
  return new Promise((r) => setTimeout(r, ms));
}

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

interface Instance {
  httpServer: HttpServer;
  io: Server;
  port: number;
  pub: Redis;
  sub: Redis;
}

async function createInstance(): Promise<Instance> {
  const httpServer = createServer();
  const ioServer = new Server(httpServer, { cors: { origin: '*' } });

  const pub = new Redis(REDIS_URI, { lazyConnect: true });
  const sub = pub.duplicate();
  await Promise.all([pub.connect(), sub.connect()]);

  ioServer.adapter(createAdapter(pub, sub, { key: KEY_PREFIX }));

  const port = await new Promise<number>((resolve) => {
    httpServer.listen(0, () => {
      resolve((httpServer.address() as any).port);
    });
  });

  return { httpServer, io: ioServer, port, pub, sub };
}

async function destroyInstance(inst: Instance) {
  inst.io.disconnectSockets(true);
  await new Promise<void>((resolve) => {
    inst.io.close(() => resolve());
  });
  await new Promise<void>((resolve) => {
    inst.httpServer.close(() => resolve());
  });
  inst.pub.disconnect();
  inst.sub.disconnect();
}

function connectClient(
  port: number,
  namespace = NAMESPACE,
  auth?: Record<string, any>,
): ClientSocket {
  return io(`http://localhost:${port}${namespace}`, {
    transports: ['websocket'],
    forceNew: true,
    auth,
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

describe('WebSocket Multi-Instance (Redis Adapter)', () => {
  let inst1: Instance;
  let inst2: Instance;
  const clients: ClientSocket[] = [];

  beforeAll(async () => {
    inst1 = await createInstance();
    inst2 = await createInstance();

    inst1.io.of(NAMESPACE);
    inst2.io.of(NAMESPACE);

    await wait(300);
  }, 15000);

  afterEach(() => {
    while (clients.length) {
      const c = clients.pop();
      c?.disconnect();
    }
  });

  afterAll(async () => {
    await destroyInstance(inst1);
    await destroyInstance(inst2);
  }, 10000);

  function trackClient(c: ClientSocket) {
    clients.push(c);
    return c;
  }

  // ─── Cross-instance broadcast ───────────────────────────────────────

  describe('cross-instance namespace broadcast', () => {
    it('emit on instance1 reaches client connected to instance2', async () => {
      const client2 = trackClient(connectClient(inst2.port));
      await waitConnected(client2);

      const msgPromise = waitForEvent(client2, 'announcement');
      inst1.io.of(NAMESPACE).emit('announcement', { text: 'hello from inst1' });

      const data = await msgPromise;
      expect(data).toEqual({ text: 'hello from inst1' });
    });

    it('emit on instance2 reaches client connected to instance1', async () => {
      const client1 = trackClient(connectClient(inst1.port));
      await waitConnected(client1);

      const msgPromise = waitForEvent(client1, 'announcement');
      inst2.io.of(NAMESPACE).emit('announcement', { text: 'hello from inst2' });

      const data = await msgPromise;
      expect(data).toEqual({ text: 'hello from inst2' });
    });

    it('broadcast reaches clients on BOTH instances', async () => {
      const client1 = trackClient(connectClient(inst1.port));
      const client2 = trackClient(connectClient(inst2.port));
      await Promise.all([waitConnected(client1), waitConnected(client2)]);

      const p1 = waitForEvent(client1, 'broadcast');
      const p2 = waitForEvent(client2, 'broadcast');
      inst1.io.of(NAMESPACE).emit('broadcast', { n: 42 });

      const [d1, d2] = await Promise.all([p1, p2]);
      expect(d1).toEqual({ n: 42 });
      expect(d2).toEqual({ n: 42 });
    });
  });

  // ─── Cross-instance room emit ──────────────────────────────────────

  describe('cross-instance room emit', () => {
    it('emit to room on inst1 reaches client in that room on inst2', async () => {
      const client2 = trackClient(connectClient(inst2.port));
      await waitConnected(client2);

      const serverSocket2 = await getServerSocket(
        inst2,
        NAMESPACE,
        client2.id!,
      );
      serverSocket2.join('room-alpha');
      await wait(200);

      const msgPromise = waitForEvent(client2, 'room-msg');
      inst1.io
        .of(NAMESPACE)
        .to('room-alpha')
        .emit('room-msg', { from: 'inst1' });

      const data = await msgPromise;
      expect(data).toEqual({ from: 'inst1' });
    });

    it('only room members receive the message, not outsiders', async () => {
      const member = trackClient(connectClient(inst2.port));
      const outsider = trackClient(connectClient(inst2.port));
      await Promise.all([waitConnected(member), waitConnected(outsider)]);

      const memberSocket = await getServerSocket(inst2, NAMESPACE, member.id!);
      memberSocket.join('vip-room');
      await wait(200);

      let outsiderGotMsg = false;
      outsider.on('vip-event', () => {
        outsiderGotMsg = true;
      });

      const memberPromise = waitForEvent(member, 'vip-event');
      inst1.io.of(NAMESPACE).to('vip-room').emit('vip-event', { secret: true });

      await memberPromise;
      await wait(300);
      expect(outsiderGotMsg).toBe(false);
    });
  });

  // ─── Cross-instance emitToSocket (by socket ID) ────────────────────

  describe('cross-instance emit to specific socket', () => {
    it('emit to socketId on inst1 reaches that specific client on inst2', async () => {
      const client2 = trackClient(connectClient(inst2.port));
      await waitConnected(client2);

      const msgPromise = waitForEvent(client2, 'direct-msg');
      inst1.io
        .of(NAMESPACE)
        .to(client2.id!)
        .emit('direct-msg', { private: true });

      const data = await msgPromise;
      expect(data).toEqual({ private: true });
    });

    it('emit to socketId does NOT reach other clients', async () => {
      const target = trackClient(connectClient(inst2.port));
      const other = trackClient(connectClient(inst1.port));
      await Promise.all([waitConnected(target), waitConnected(other)]);

      let otherGotMsg = false;
      other.on('direct-msg', () => {
        otherGotMsg = true;
      });

      const targetPromise = waitForEvent(target, 'direct-msg');
      inst1.io
        .of(NAMESPACE)
        .to(target.id!)
        .emit('direct-msg', { only: 'target' });

      await targetPromise;
      await wait(300);
      expect(otherGotMsg).toBe(false);
    });
  });

  // ─── Cross-instance user room pattern (emitToUser simulation) ──────

  describe('cross-instance emitToUser pattern', () => {
    it('emit to user room on inst1 reaches user client on inst2', async () => {
      const userId = 'user-777';
      const client2 = trackClient(connectClient(inst2.port));
      await waitConnected(client2);

      const serverSocket2 = await getServerSocket(
        inst2,
        NAMESPACE,
        client2.id!,
      );
      serverSocket2.join(`user_${userId}`);
      await wait(200);

      const msgPromise = waitForEvent(client2, 'notification');
      inst1.io
        .of(NAMESPACE)
        .to(`user_${userId}`)
        .emit('notification', { msg: 'you have mail' });

      const data = await msgPromise;
      expect(data).toEqual({ msg: 'you have mail' });
    });

    it('user connected on multiple instances receives on all', async () => {
      const userId = 'user-888';
      const client1 = trackClient(connectClient(inst1.port));
      const client2 = trackClient(connectClient(inst2.port));
      await Promise.all([waitConnected(client1), waitConnected(client2)]);

      const ss1 = await getServerSocket(inst1, NAMESPACE, client1.id!);
      const ss2 = await getServerSocket(inst2, NAMESPACE, client2.id!);
      ss1.join(`user_${userId}`);
      ss2.join(`user_${userId}`);
      await wait(200);

      const p1 = waitForEvent(client1, 'user-event');
      const p2 = waitForEvent(client2, 'user-event');
      inst1.io.of(NAMESPACE).to(`user_${userId}`).emit('user-event', { x: 1 });

      const [d1, d2] = await Promise.all([p1, p2]);
      expect(d1).toEqual({ x: 1 });
      expect(d2).toEqual({ x: 1 });
    });
  });

  // ─── NODE_NAME prefix isolation (multi-app) ────────────────────────

  describe('NODE_NAME prefix isolation', () => {
    let isolatedInst: Instance;

    afterEach(async () => {
      if (isolatedInst) await destroyInstance(isolatedInst);
    });

    it('instances with different key prefix do NOT see each other', async () => {
      const httpServer = createServer();
      const ioServer = new Server(httpServer, { cors: { origin: '*' } });

      const pub = new Redis(REDIS_URI, { lazyConnect: true });
      const sub = pub.duplicate();
      await Promise.all([pub.connect(), sub.connect()]);

      const differentPrefix = `other-app-${Date.now()}:socket.io:`;
      ioServer.adapter(createAdapter(pub, sub, { key: differentPrefix }));
      ioServer.of(NAMESPACE);

      const port = await new Promise<number>((resolve) => {
        httpServer.listen(0, () => resolve((httpServer.address() as any).port));
      });
      isolatedInst = { httpServer, io: ioServer, port, pub, sub };

      const clientOnIsolated = trackClient(connectClient(isolatedInst.port));
      const clientOnInst1 = trackClient(connectClient(inst1.port));
      await Promise.all([
        waitConnected(clientOnIsolated),
        waitConnected(clientOnInst1),
      ]);

      let isolatedGotMsg = false;
      clientOnIsolated.on('cross-app', () => {
        isolatedGotMsg = true;
      });

      const inst1Promise = waitForEvent(clientOnInst1, 'cross-app');
      inst1.io.of(NAMESPACE).emit('cross-app', { data: 'from inst1' });

      await inst1Promise;
      await wait(500);
      expect(isolatedGotMsg).toBe(false);
    });
  });

  // ─── Concurrent connections stress ─────────────────────────────────

  describe('concurrent cross-instance messaging', () => {
    it('handles many clients across both instances', async () => {
      const COUNT = 20;
      const allClients: ClientSocket[] = [];

      for (let i = 0; i < COUNT; i++) {
        const port = i % 2 === 0 ? inst1.port : inst2.port;
        const c = trackClient(connectClient(port));
        allClients.push(c);
      }
      await Promise.all(allClients.map((c) => waitConnected(c)));

      const promises = allClients.map((c) => waitForEvent(c, 'mass-broadcast'));
      inst1.io.of(NAMESPACE).emit('mass-broadcast', { seq: 999 });

      const results = await Promise.all(promises);
      expect(results).toHaveLength(COUNT);
      results.forEach((d) => expect(d).toEqual({ seq: 999 }));
    });

    it('room messages survive rapid join/emit cycles', async () => {
      const client2 = trackClient(connectClient(inst2.port));
      await waitConnected(client2);

      const ss2 = await getServerSocket(inst2, NAMESPACE, client2.id!);

      const received: any[] = [];
      client2.on('rapid', (d: any) => received.push(d));

      for (let i = 0; i < 10; i++) {
        const room = `rapid-room-${i}`;
        ss2.join(room);
      }
      await wait(300);

      for (let i = 0; i < 10; i++) {
        inst1.io.of(NAMESPACE).to(`rapid-room-${i}`).emit('rapid', { i });
      }
      await wait(1000);

      expect(received).toHaveLength(10);
      const indices = received
        .map((d) => d.i)
        .sort((a: number, b: number) => a - b);
      expect(indices).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    });
  });

  // ─── Disconnect resilience ─────────────────────────────────────────

  describe('disconnect and reconnect', () => {
    it('after disconnect, room emit no longer reaches old client', async () => {
      const client2 = trackClient(connectClient(inst2.port));
      await waitConnected(client2);

      const ss2 = await getServerSocket(inst2, NAMESPACE, client2.id!);
      ss2.join('ephemeral-room');
      await wait(200);

      client2.disconnect();
      await wait(300);

      let gotMsg = false;
      client2.on('ghost', () => {
        gotMsg = true;
      });
      inst1.io.of(NAMESPACE).to('ephemeral-room').emit('ghost', {});
      await wait(500);

      expect(gotMsg).toBe(false);
    });

    it('reconnected client in same room receives new messages', async () => {
      const client = trackClient(connectClient(inst2.port));
      await waitConnected(client);

      const ss = await getServerSocket(inst2, NAMESPACE, client.id!);
      ss.join('persistent-room');
      await wait(200);

      client.disconnect();
      await wait(200);

      const client2 = trackClient(connectClient(inst2.port));
      await waitConnected(client2);

      const ss2 = await getServerSocket(inst2, NAMESPACE, client2.id!);
      ss2.join('persistent-room');
      await wait(200);

      const msgPromise = waitForEvent(client2, 'revived');
      inst1.io
        .of(NAMESPACE)
        .to('persistent-room')
        .emit('revived', { ok: true });

      const data = await msgPromise;
      expect(data).toEqual({ ok: true });
    });
  });

  // ─── Enfyra emit helpers simulation ────────────────────────────────

  describe('Enfyra gateway emit methods (simulated)', () => {
    function emitToUser(
      server: Server,
      userId: number | string,
      event: string,
      data: any,
    ) {
      server.to(`user_${userId}`).emit(event, data);
    }

    function emitToRoom(
      server: Server,
      room: string,
      event: string,
      data: any,
    ) {
      server.to(room).emit(event, data);
    }

    function emitToNamespace(
      server: Server,
      path: string,
      event: string,
      data: any,
    ) {
      server.of(path).emit(event, data);
    }

    function emitToSocket(
      server: Server,
      path: string,
      socketId: string,
      event: string,
      data: any,
    ) {
      server.of(path).to(socketId).emit(event, data);
    }

    it('emitToUser cross-instance', async () => {
      const client = trackClient(connectClient(inst2.port));
      await waitConnected(client);
      const ss = await getServerSocket(inst2, NAMESPACE, client.id!);
      ss.join('user_42');
      await wait(200);

      const p = waitForEvent(client, 'notify');
      emitToUser(inst1.io.of(NAMESPACE) as any, 42, 'notify', { count: 5 });

      const d = await p;
      expect(d).toEqual({ count: 5 });
    });

    it('emitToRoom cross-instance', async () => {
      const client = trackClient(connectClient(inst2.port));
      await waitConnected(client);
      const ss = await getServerSocket(inst2, NAMESPACE, client.id!);
      ss.join('chat-room');
      await wait(200);

      const p = waitForEvent(client, 'chat');
      emitToRoom(inst1.io.of(NAMESPACE) as any, 'chat-room', 'chat', {
        msg: 'hi',
      });

      const d = await p;
      expect(d).toEqual({ msg: 'hi' });
    });

    it('emitToNamespace cross-instance', async () => {
      const client = trackClient(connectClient(inst2.port));
      await waitConnected(client);

      const p = waitForEvent(client, 'global');
      emitToNamespace(inst1.io, NAMESPACE, 'global', { all: true });

      const d = await p;
      expect(d).toEqual({ all: true });
    });

    it('emitToSocket cross-instance', async () => {
      const client = trackClient(connectClient(inst2.port));
      await waitConnected(client);

      const p = waitForEvent(client, 'dm');
      emitToSocket(inst1.io, NAMESPACE, client.id!, 'dm', { secret: 'shhh' });

      const d = await p;
      expect(d).toEqual({ secret: 'shhh' });
    });
  });
});

// ─── Helpers ───────────────────────────────────────────────────────────

async function getServerSocket(
  inst: Instance,
  namespace: string,
  clientId: string,
  timeoutMs = 3000,
) {
  const ns = inst.io.of(namespace);
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const sockets = await ns.fetchSockets();
    const found = sockets.find((s) => s.id === clientId);
    if (found) return found;
    await wait(50);
  }
  throw new Error(
    `Server socket ${clientId} not found on instance :${inst.port}`,
  );
}
