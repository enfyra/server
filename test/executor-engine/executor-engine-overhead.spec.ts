import { executeSingle, executeBatch } from '../helpers/spawn-worker';

describe('Executor engine overhead optimizations', () => {
  describe('unified __callRef bridge', () => {
    it('repo call works through unified ref', async () => {
      const result = await executeSingle({
        code: 'return await $ctx.$repos.users.find({ limit: 5 });',
        snapshot: {
          $body: {},
          $query: {},
          $params: {},
          $user: null,
          $share: { $logs: [] },
          $data: {},
        },
        ctx: {
          $repos: {
            users: { find: async (opts: any) => ({ data: [{ id: 1 }], opts }) },
          },
        },
      });
      expect(result.value.data).toEqual([{ id: 1 }]);
      expect(result.value.opts).toEqual({ limit: 5 });
    });

    it('helpers call works through unified ref', async () => {
      const result = await executeSingle({
        code: 'return await $ctx.$helpers.$bcrypt.hash("pw", 10);',
        snapshot: {
          $body: {},
          $query: {},
          $params: {},
          $user: null,
          $share: { $logs: [] },
          $data: {},
        },
        ctx: {
          $helpers: {
            $bcrypt: {
              hash: async (pw: string, rounds: number) =>
                `hashed_${pw}_${rounds}`,
            },
          },
        },
      });
      expect(result.value).toBe('hashed_pw_10');
    });

    it('cache call works through unified ref', async () => {
      const store: Record<string, string> = {};
      const result = await executeSingle({
        code: `
          await $ctx.$cache.set("k1", "v1");
          return await $ctx.$cache.get("k1");
        `,
        snapshot: {
          $body: {},
          $query: {},
          $params: {},
          $user: null,
          $share: { $logs: [] },
          $data: {},
        },
        ctx: {
          $cache: {
            set: async (k: string, v: string) => {
              store[k] = v;
            },
            get: async (k: string) => store[k],
          },
        },
      });
      expect(result.value).toBe('v1');
    });

    it('dispatch call works through unified ref', async () => {
      let triggered = '';
      const result = await executeSingle({
        code: 'return await $ctx.$dispatch.trigger("myFlow", { x: 1 });',
        snapshot: {
          $body: {},
          $query: {},
          $params: {},
          $user: null,
          $share: { $logs: [] },
          $data: {},
        },
        ctx: {
          $dispatch: {
            trigger: async (name: string, data: any) => {
              triggered = name;
              return { ok: true, ...data };
            },
          },
        },
      });
      expect(triggered).toBe('myFlow');
      expect(result.value).toEqual({ ok: true, x: 1 });
    });

    it('socket fire-and-forget works through unified __fireRef', async () => {
      const result = await executeSingle({
        code: `
          $ctx.$socket.emitToAll("hello", { msg: "world" });
          return "done";
        `,
        snapshot: {
          $body: {},
          $query: {},
          $params: {},
          $user: null,
          $share: { $logs: [] },
          $data: {},
        },
        ctx: {
          $socket: { emitToAll: () => {} },
        },
      });
      expect(result.value).toBe('done');
    });

    it('multiple proxy types in one handler', async () => {
      const result = await executeSingle({
        code: `
          const users = await $ctx.$repos.main.find();
          const hash = await $ctx.$helpers.$bcrypt.hash("test", 10);
          await $ctx.$cache.set("count", users.length);
          return { users, hash, cached: await $ctx.$cache.get("count") };
        `,
        snapshot: {
          $body: {},
          $query: {},
          $params: {},
          $user: null,
          $share: { $logs: [] },
          $data: {},
        },
        ctx: {
          $repos: {
            main: { find: async () => [{ id: 1 }, { id: 2 }] },
          },
          $helpers: {
            $bcrypt: { hash: async (pw: string, r: number) => `h_${pw}_${r}` },
          },
          $cache: {
            set: async () => {},
            get: async () => 2,
          },
        },
      });
      expect(result.value.users).toEqual([{ id: 1 }, { id: 2 }]);
      expect(result.value.hash).toBe('h_test_10');
      expect(result.value.cached).toBe(2);
    });
  });

  describe('__extractResult in setup code', () => {
    it('single execute uses __extractResult (no extra compile)', async () => {
      const result = await executeSingle({
        code: 'return { x: 1 };',
        snapshot: {
          $body: {},
          $query: {},
          $params: {},
          $user: null,
          $share: { $logs: [] },
          $data: {},
        },
      });
      expect(result.value).toEqual({ x: 1 });
      expect(result.valueAbsent).toBe(false);
    });

    it('undefined return preserves valueAbsent', async () => {
      const result = await executeSingle({
        code: 'const x = 1;',
        snapshot: {
          $body: {},
          $query: {},
          $params: {},
          $user: null,
          $share: { $logs: [] },
          $data: {},
        },
      });
      expect(result.valueAbsent).toBe(true);
    });

    it('batch extractFinalResult uses pre-compiled __extractResult', async () => {
      const result = await executeBatch({
        codeBlocks: [{ code: 'return { items: [1, 2, 3] };', type: 'handler' }],
        snapshot: {
          $body: {},
          $query: {},
          $params: {},
          $user: null,
          $share: { $logs: [] },
          $data: {},
        },
      });
      expect(result.value).toEqual({ items: [1, 2, 3] });
    });

    it('ctxChanges flow through __extractResult correctly', async () => {
      const result = await executeSingle({
        code: `
          $ctx.$body.modified = true;
          $ctx.$query.page = 2;
          return "ok";
        `,
        snapshot: {
          $body: {},
          $query: { page: 1 },
          $params: {},
          $user: null,
          $share: { $logs: [] },
          $data: {},
        },
      });
      expect(result.ctxChanges.$body.modified).toBe(true);
      expect(result.ctxChanges.$query.page).toBe(2);
    });

    it('batch preHook short-circuit uses __extractResult', async () => {
      const result = await executeBatch({
        codeBlocks: [
          { code: 'return { earlyExit: true };', type: 'preHook' },
          { code: 'return { shouldNotRun: true };', type: 'handler' },
        ],
        snapshot: {
          $body: {},
          $query: {},
          $params: {},
          $user: null,
          $share: { $logs: [] },
          $data: {},
        },
      });
      expect(result.shortCircuit).toBe(true);
      expect(result.value).toEqual({ earlyExit: true });
    });
  });

  describe('taskId counter-based', () => {
    it('concurrent tasks get unique ids (no collision)', async () => {
      const tasks = Array.from({ length: 20 }, (_, i) =>
        executeSingle({
          code: `return ${i};`,
          snapshot: {
            $body: {},
            $query: {},
            $params: {},
            $user: null,
            $share: { $logs: [] },
            $data: {},
          },
        }),
      );
      const results = await Promise.all(tasks);
      const values = results.map((r) => r.value).sort((a, b) => a - b);
      expect(values).toEqual(Array.from({ length: 20 }, (_, i) => i));
    });
  });

  describe('$uploadedFile excluded from snapshot serialization overhead', () => {
    it('snapshot with uploadedFile passes through', async () => {
      const result = await executeSingle({
        code: 'return $ctx.$uploadedFile ? $ctx.$uploadedFile.name : "none";',
        snapshot: {
          $body: {},
          $query: {},
          $params: {},
          $user: null,
          $share: { $logs: [] },
          $data: {},
          $uploadedFile: { name: 'test.png', size: 1024 },
        },
      });
      expect(result.value).toBe('test.png');
    });
  });

  describe('$logs captured via __extractResult ctxChanges', () => {
    it('console.log captured in $share.$logs', async () => {
      const result = await executeSingle({
        code: `
          console.log("hello");
          console.warn({ a: 1 });
          return "done";
        `,
        snapshot: {
          $body: {},
          $query: {},
          $params: {},
          $user: null,
          $share: { $logs: [] },
          $data: {},
        },
      });
      expect(result.ctxChanges.$share.$logs).toEqual(['hello', { a: 1 }]);
    });
  });
});
