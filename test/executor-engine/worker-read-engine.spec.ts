import * as path from 'path';
import { Worker } from 'worker_threads';

const WORKER_SCRIPT = path.join(
  __dirname,
  '../../src/engine/executor-engine/workers/executor.worker.js',
);

function encodeMainThreadToIsolate(value: unknown): string {
  if (value === undefined) return JSON.stringify({ __e: 'u' });
  try {
    return JSON.stringify({ __e: 'v', d: value }, (_k, v) =>
      typeof v === 'bigint' ? v.toString() : v,
    );
  } catch {
    return JSON.stringify({ __e: 'v', d: { __serializationError: true } });
  }
}

function spawnWorkerWithData(
  messageType: string,
  payload: Record<string, any>,
  ctx: Record<string, any>,
  timeoutMs: number,
  workerDataPayload?: Record<string, any>,
  preMessages?: any[],
): Promise<any> {
  return new Promise((resolve, reject) => {
    const worker = new Worker(WORKER_SCRIPT, {
      ...(workerDataPayload ? { workerData: workerDataPayload } : {}),
    });
    const id = `test_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    let settled = false;

    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      worker.terminate();
      reject(new Error(`Timed out after ${timeoutMs}ms`));
    }, timeoutMs + 5000);

    const cleanup = () => {
      clearTimeout(timer);
      worker.terminate();
    };

    worker.on('message', async (msg) => {
      if (msg.type === 'result') {
        if (settled) return;
        settled = true;
        cleanup();
        if (msg.success) {
          const res: any = {
            value: msg.value,
            valueAbsent: msg.valueAbsent === true,
            ctxChanges: msg.ctxChanges,
          };
          if (msg.shortCircuit) res.shortCircuit = true;
          resolve(res);
        } else {
          const err: any = new Error(
            msg.error?.message || 'Handler execution failed',
          );
          err.statusCode = msg.error?.statusCode;
          reject(err);
        }
      } else if (msg.type === 'repoCall') {
        try {
          const args = JSON.parse(msg.argsJson);
          const repo = ctx?.$repos?.[msg.table];
          if (!repo || typeof repo[msg.method] !== 'function')
            throw new Error(`Repo not found: ${msg.table}.${msg.method}`);
          const result = await repo[msg.method](...args);
          worker.postMessage({
            type: 'callResult',
            callId: msg.callId,
            result: encodeMainThreadToIsolate(result),
          });
        } catch (e: any) {
          worker.postMessage({
            type: 'callError',
            callId: msg.callId,
            error: e.message,
          });
        }
      }
    });

    worker.on('error', (err) => {
      if (settled) return;
      settled = true;
      cleanup();
      reject(err);
    });

    worker.on('exit', (code) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      reject(new Error(`Worker exited with code ${code}`));
    });

    if (preMessages) {
      for (const m of preMessages) worker.postMessage(m);
    }

    worker.postMessage({ type: messageType, id, ...payload });
  });
}

describe('Worker Read Engine routing', () => {
  const defaultSnapshot = {
    $body: {},
    $query: {},
    $params: {},
    $user: null,
    $share: { $logs: [] },
    $data: {},
    $mainTableName: 'users',
  };

  describe('without workerData (no ReadEngine)', () => {
    it('find() bridges to main thread as before', async () => {
      const result = await spawnWorkerWithData(
        'execute',
        {
          code: 'return await $ctx.$repos.users.find({ limit: 5 });',
          pkgSources: [],
          snapshot: defaultSnapshot,
          timeoutMs: 10000,
          memoryLimitMb: 128,
        },
        {
          $repos: {
            users: {
              find: async (opts: any) => ({
                data: [{ id: 1 }],
                bridged: true,
                opts,
              }),
            },
          },
        },
        10000,
      );
      expect(result.value.bridged).toBe(true);
      expect(result.value.data).toEqual([{ id: 1 }]);
    });
  });

  describe('with workerData but no metadata (not ready)', () => {
    it('find() bridges to main when ReadEngine not ready', async () => {
      const result = await spawnWorkerWithData(
        'execute',
        {
          code: 'return await $ctx.$repos.users.find({ limit: 3 });',
          pkgSources: [],
          snapshot: defaultSnapshot,
          timeoutMs: 10000,
          memoryLimitMb: 128,
        },
        {
          $repos: {
            users: {
              find: async (_opts: any) => ({
                data: [{ id: 2 }],
                bridged: true,
              }),
            },
          },
        },
        10000,
        {
          dbConfig: {
            client: 'pg',
            connection: {
              host: 'localhost',
              port: 5432,
              user: 'test',
              password: 'test',
              database: 'test',
            },
          },
        },
      );
      expect(result.value.bridged).toBe(true);
    });
  });

  describe('lagUpdate message handling', () => {
    it('worker accepts lagUpdate without crashing', async () => {
      const result = await spawnWorkerWithData(
        'execute',
        {
          code: 'return "alive";',
          pkgSources: [],
          snapshot: defaultSnapshot,
          timeoutMs: 10000,
          memoryLimitMb: 128,
        },
        {},
        10000,
        undefined,
        [{ type: 'lagUpdate', lag: 5 }],
      );
      expect(result.value).toBe('alive');
    });
  });

  describe('metadataUpdate message handling', () => {
    it('worker accepts metadataUpdate without crashing', async () => {
      const result = await spawnWorkerWithData(
        'execute',
        {
          code: 'return "alive";',
          pkgSources: [],
          snapshot: defaultSnapshot,
          timeoutMs: 10000,
          memoryLimitMb: 128,
        },
        {},
        10000,
        undefined,
        [
          {
            type: 'metadataUpdate',
            metadata: {
              tablesEntries: [
                ['users', { name: 'users', columns: [], relations: [] }],
              ],
              tablesList: [{ name: 'users', columns: [], relations: [] }],
              version: 1,
              timestamp: new Date().toISOString(),
            },
          },
        ],
      );
      expect(result.value).toBe('alive');
    });
  });

  describe('$mainTableName in snapshot', () => {
    it('$mainTableName passes through to snapshot correctly', async () => {
      const result = await spawnWorkerWithData(
        'execute',
        {
          code: 'return $ctx.$mainTableName;',
          pkgSources: [],
          snapshot: { ...defaultSnapshot, $mainTableName: 'orders' },
          timeoutMs: 10000,
          memoryLimitMb: 128,
        },
        {},
        10000,
      );
      expect(result.value).toBe('orders');
    });
  });

  describe('write operations always bridge', () => {
    it('create() always bridges to main thread', async () => {
      const result = await spawnWorkerWithData(
        'execute',
        {
          code: 'return await $ctx.$repos.users.create({ name: "test" });',
          pkgSources: [],
          snapshot: defaultSnapshot,
          timeoutMs: 10000,
          memoryLimitMb: 128,
        },
        {
          $repos: {
            users: {
              create: async (data: any) => ({ id: 99, ...data, bridged: true }),
            },
          },
        },
        10000,
        undefined,
        [{ type: 'lagUpdate', lag: 100 }],
      );
      expect(result.value.bridged).toBe(true);
      expect(result.value.name).toBe('test');
    });

    it('update() always bridges to main thread', async () => {
      const result = await spawnWorkerWithData(
        'execute',
        {
          code: 'return await $ctx.$repos.users.update({ id: 1, name: "updated" });',
          pkgSources: [],
          snapshot: defaultSnapshot,
          timeoutMs: 10000,
          memoryLimitMb: 128,
        },
        {
          $repos: {
            users: {
              update: async (data: any) => ({ ...data, bridged: true }),
            },
          },
        },
        10000,
        undefined,
        [{ type: 'lagUpdate', lag: 100 }],
      );
      expect(result.value.bridged).toBe(true);
    });

    it('delete() always bridges to main thread', async () => {
      const result = await spawnWorkerWithData(
        'execute',
        {
          code: 'return await $ctx.$repos.users.delete({ id: 1 });',
          pkgSources: [],
          snapshot: defaultSnapshot,
          timeoutMs: 10000,
          memoryLimitMb: 128,
        },
        {
          $repos: {
            users: {
              delete: async (_opts: any) => ({ deleted: true, bridged: true }),
            },
          },
        },
        10000,
        undefined,
        [{ type: 'lagUpdate', lag: 100 }],
      );
      expect(result.value.bridged).toBe(true);
    });
  });

  describe('low lag routes find to bridge', () => {
    it('find() bridges when lag is 0', async () => {
      const result = await spawnWorkerWithData(
        'execute',
        {
          code: 'return await $ctx.$repos.users.find({ limit: 5 });',
          pkgSources: [],
          snapshot: defaultSnapshot,
          timeoutMs: 10000,
          memoryLimitMb: 128,
        },
        {
          $repos: {
            users: {
              find: async (_opts: any) => ({ data: [], bridged: true }),
            },
          },
        },
        10000,
        undefined,
        [{ type: 'lagUpdate', lag: 0 }],
      );
      expect(result.value.bridged).toBe(true);
    });
  });

  describe('mixed read/write in same handler', () => {
    it('find bridges, create bridges, both work together', async () => {
      const result = await spawnWorkerWithData(
        'execute',
        {
          code: `
            const found = await $ctx.$repos.users.find({ limit: 1 });
            const created = await $ctx.$repos.users.create({ name: "new" });
            return { found, created };
          `,
          pkgSources: [],
          snapshot: defaultSnapshot,
          timeoutMs: 10000,
          memoryLimitMb: 128,
        },
        {
          $repos: {
            users: {
              find: async () => ({ data: [{ id: 1 }], bridged: true }),
              create: async (d: any) => ({ ...d, id: 2, bridged: true }),
            },
          },
        },
        10000,
      );
      expect(result.value.found.bridged).toBe(true);
      expect(result.value.created.bridged).toBe(true);
    });
  });
});
