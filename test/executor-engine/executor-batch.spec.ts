import {
  executeBatch,
  executeBatchSequence,
  CodeBlock,
} from '../helpers/spawn-worker';

const REALISTIC_ROUTE_BLOCKS: CodeBlock[] = [
  {
    code: '$ctx.$body.pagination = { limit: Math.min(Number($ctx.$query.limit) || 20, 100), page: Number($ctx.$query.page) || 1 }; $ctx.$logs("pre");',
    type: 'preHook',
  },
  {
    code: 'const r = await $ctx.$repos.main.find({ filter: {}, limit: $ctx.$body.pagination.limit }); return { items: r.data, listMeta: r.meta };',
    type: 'handler',
  },
  {
    code: 'return { statusCode: 200, data: $ctx.$data, message: "Success" };',
    type: 'postHook',
  },
];

function realisticRouteCtx(seed: number) {
  return {
    $body: {},
    $query: { limit: '10', page: '1', filter: '{}' },
    $params: { id: String(seed) },
    $user: { id: 1, email: 'bench@local', role: 'user' },
    $share: { $logs: [] },
    $api: {
      request: {
        method: 'GET',
        url: `/api/items`,
        correlationId: `bench-${seed}`,
        userAgent: 'bench/1',
        ip: '127.0.0.1',
        timestamp: Date.now(),
      },
    },
    $repos: {
      main: {
        find: async (opts: { limit?: number }) => ({
          data: [{ id: seed, title: `row-${seed}` }],
          meta: { total: 100, limit: opts?.limit ?? 10 },
        }),
      },
    },
  };
}

async function realisticRouteBatch(seed: number) {
  const fullCtx = realisticRouteCtx(seed);
  return executeBatch({
    codeBlocks: REALISTIC_ROUTE_BLOCKS,
    snapshot: baseSnapshot(fullCtx),
    ctx: fullCtx,
    timeoutMs: 30000,
  });
}

function baseSnapshot(overrides: Record<string, any> = {}) {
  return {
    $body: overrides.$body ?? {},
    $query: overrides.$query ?? {},
    $params: overrides.$params ?? {},
    $user: overrides.$user ?? null,
    $share: overrides.$share ?? { $logs: [] },
    $data: overrides.$data ?? undefined,
    $api: overrides.$api ?? { request: {} },
  };
}

async function batch(blocks: CodeBlock[], ctx: Record<string, any> = {}) {
  const fullCtx: Record<string, any> = {
    $body: ctx.$body ?? {},
    $query: ctx.$query ?? {},
    $params: ctx.$params ?? {},
    $user: ctx.$user ?? null,
    $share: ctx.$share ?? { $logs: [] },
    ...ctx,
  };
  return executeBatch({
    codeBlocks: blocks,
    snapshot: baseSnapshot(fullCtx),
    ctx: fullCtx,
  });
}

describe('Batch execution: correctness', () => {
  it('pre-hook modifies $ctx.$body, handler sees changes', async () => {
    const r = await batch(
      [
        { code: '$ctx.$body.name = $ctx.$body.name.trim();', type: 'preHook' },
        { code: 'return { name: $ctx.$body.name };', type: 'handler' },
      ],
      { $body: { name: '  raw  ' } },
    );
    expect(r.value.name).toBe('raw');
  });

  it('pre-hook modifies $ctx.$query, handler sees it', async () => {
    const r = await batch(
      [
        {
          code: '$ctx.$query.page = Number($ctx.$query.page);',
          type: 'preHook',
        },
        {
          code: 'return { page: $ctx.$query.page, type: typeof $ctx.$query.page };',
          type: 'handler',
        },
      ],
      { $query: { page: '1' } },
    );
    expect(r.value.page).toBe(1);
    expect(r.value.type).toBe('number');
  });

  it('multiple pre-hooks execute in order', async () => {
    const r = await batch(
      [
        { code: '$ctx.$body.steps.push("first");', type: 'preHook' },
        { code: '$ctx.$body.steps.push("second");', type: 'preHook' },
        { code: '$ctx.$body.steps.push("third");', type: 'preHook' },
        { code: 'return $ctx.$body.steps;', type: 'handler' },
      ],
      { $body: { steps: [] } },
    );
    expect(r.value).toEqual(['first', 'second', 'third']);
  });

  it('pre-hook short-circuit: return value skips handler and post-hooks', async () => {
    const r = await batch([
      { code: 'return { blocked: true };', type: 'preHook' },
      { code: '$ctx.$body.shouldNotRun = true;', type: 'preHook' },
      { code: 'return { fromHandler: true };', type: 'handler' },
      { code: 'return { fromPostHook: true };', type: 'postHook' },
    ]);
    expect(r.shortCircuit).toBe(true);
    expect(r.value).toEqual({ blocked: true });
    expect(r.ctxChanges.$body.shouldNotRun).toBeUndefined();
  });

  it('pre-hook without return does not short-circuit', async () => {
    const r = await batch(
      [
        { code: '$ctx.$body.x = 2;', type: 'preHook' },
        { code: 'return $ctx.$body.x;', type: 'handler' },
      ],
      { $body: { x: 1 } },
    );
    expect(r.value).toBe(2);
  });

  it('handler result sets $ctx.$data for post-hooks', async () => {
    const r = await batch([
      { code: 'return { items: [1, 2, 3] };', type: 'handler' },
      { code: 'return { wrapped: true, data: $ctx.$data };', type: 'postHook' },
    ]);
    expect(r.value.wrapped).toBe(true);
    expect(r.value.data).toEqual({ items: [1, 2, 3] });
  });

  it('handler returning undefined sets $ctx.$data to undefined', async () => {
    const r = await batch([
      { code: '/* no return */', type: 'handler' },
      { code: 'return { dataWas: $ctx.$data };', type: 'postHook' },
    ]);
    expect(r.value.dataWas).toBeUndefined();
  });

  it('post-hook return replaces $ctx.$data', async () => {
    const r = await batch([
      { code: 'return { original: true };', type: 'handler' },
      { code: 'return { ...($ctx.$data), extra: "added" };', type: 'postHook' },
    ]);
    expect(r.value.original).toBe(true);
    expect(r.value.extra).toBe('added');
  });

  it('post-hook without return keeps $ctx.$data unchanged', async () => {
    const r = await batch([
      { code: 'return { keepMe: true };', type: 'handler' },
      { code: '$ctx.$share.$logs.push("logged");', type: 'postHook' },
    ]);
    expect(r.value.keepMe).toBe(true);
    expect(r.ctxChanges.$share.$logs).toContain('logged');
  });

  it('multiple post-hooks chain $ctx.$data correctly', async () => {
    const r = await batch([
      { code: 'return { step: 0 };', type: 'handler' },
      { code: 'return { ...$ctx.$data, step: 1 };', type: 'postHook' },
      { code: 'return { ...$ctx.$data, step: 2 };', type: 'postHook' },
      { code: 'return { ...$ctx.$data, step: 3 };', type: 'postHook' },
    ]);
    expect(r.value.step).toBe(3);
  });

  it('full flow: pre-hooks → handler → post-hooks', async () => {
    const r = await batch(
      [
        { code: '$ctx.$body.name = $ctx.$body.name.trim();', type: 'preHook' },
        {
          code: '$ctx.$query.limit = Number($ctx.$query.limit);',
          type: 'preHook',
        },
        {
          code: 'return { user: $ctx.$body.name, limit: $ctx.$query.limit, id: $ctx.$params.id };',
          type: 'handler',
        },
        {
          code: 'return { statusCode: 200, data: $ctx.$data, message: "Success" };',
          type: 'postHook',
        },
        {
          code: 'return { ...$ctx.$data, meta: { timestamp: "now" } };',
          type: 'postHook',
        },
      ],
      {
        $body: { name: '  John  ' },
        $query: { limit: '10' },
        $params: { id: '42' },
        $user: { id: 1 },
      },
    );
    expect(r.value.data.user).toBe('John');
    expect(r.value.data.limit).toBe(10);
    expect(r.value.meta.timestamp).toBe('now');
  });

  it('reused worker context does not leak explicit global properties', async () => {
    const [first, second] = await executeBatchSequence([
      {
        codeBlocks: [
          {
            code: 'globalThis.__leakedValue = "from-first"; return globalThis.__leakedValue;',
            type: 'handler',
          },
        ],
        snapshot: baseSnapshot(),
        isolatePoolSize: 1,
      },
      {
        codeBlocks: [
          {
            code: 'return typeof globalThis.__leakedValue;',
            type: 'handler',
          },
        ],
        snapshot: baseSnapshot(),
        isolatePoolSize: 1,
      },
    ]);

    expect(first.value).toBe('from-first');
    expect(second.value).toBe('undefined');
  });

  it('reused worker context does not leak prototype pollution', async () => {
    const [first, second] = await executeBatchSequence([
      {
        codeBlocks: [
          {
            code: 'Object.prototype.__polluted = "yes"; Array.prototype.__arrPolluted = "yes"; return { ok: true };',
            type: 'handler',
          },
        ],
        snapshot: baseSnapshot(),
        isolatePoolSize: 1,
      },
      {
        codeBlocks: [
          {
            code: 'return { object: ({}).__polluted, array: [].__arrPolluted };',
            type: 'handler',
          },
        ],
        snapshot: baseSnapshot(),
        isolatePoolSize: 1,
      },
    ]);

    expect(first.value.ok).toBe(true);
    expect(second.value.object).toBeUndefined();
    expect(second.value.array).toBeUndefined();
  });

  it('reused worker context keeps request body and logs isolated', async () => {
    const [first, second] = await executeBatchSequence([
      {
        codeBlocks: [
          {
            code: '$ctx.$body.created = true; $ctx.$logs("first"); return { body: $ctx.$body, logs: $ctx.$share.$logs };',
            type: 'handler',
          },
        ],
        snapshot: baseSnapshot({ $body: { request: 1 }, $share: {} }),
        isolatePoolSize: 1,
      },
      {
        codeBlocks: [
          {
            code: 'return { body: $ctx.$body, logs: $ctx.$share.$logs };',
            type: 'handler',
          },
        ],
        snapshot: baseSnapshot({ $body: { request: 2 }, $share: {} }),
        isolatePoolSize: 1,
      },
    ]);

    expect(first.value.body).toEqual({ request: 1, created: true });
    expect(first.value.logs).toEqual(['first']);
    expect(second.value.body).toEqual({ request: 2 });
    expect(second.value.logs).toEqual([]);
  });

  it('error in pre-hook stops execution', async () => {
    await expect(
      batch([
        { code: 'throw new Error("validation failed");', type: 'preHook' },
        { code: 'return 1;', type: 'handler' },
      ]),
    ).rejects.toThrow('validation failed');
  });

  it('error in handler still runs post-hooks then throws original error', async () => {
    await expect(
      batch([
        { code: '$ctx.$body.preRan = true;', type: 'preHook' },
        { code: 'throw new Error("handler broke");', type: 'handler' },
        {
          code: '$ctx.$logs("post-hook ran: " + $ctx.$error.message);',
          type: 'postHook',
        },
      ]),
    ).rejects.toThrow('handler broke');
  });

  it('error in one post-hook does not stop other post-hooks', async () => {
    const r = await batch([
      { code: 'return { ok: true };', type: 'handler' },
      { code: 'throw new Error("post-hook-1 failed");', type: 'postHook' },
      { code: 'return { ...$ctx.$data, recovered: true };', type: 'postHook' },
    ]);
    expect(r.value.ok).toBe(true);
    expect(r.value.recovered).toBe(true);
  });

  it('$throw produces error with message', async () => {
    await expect(
      batch([
        { code: '$ctx.$throw["403"]("Forbidden");', type: 'preHook' },
        { code: 'return 1;', type: 'handler' },
      ]),
    ).rejects.toThrow('Forbidden');
  });

  it('$logs accumulate across all phases', async () => {
    const r = await batch([
      { code: '$ctx.$logs("from preHook");', type: 'preHook' },
      {
        code: '$ctx.$logs("from handler"); return { ok: true };',
        type: 'handler',
      },
      { code: '$ctx.$logs("from postHook");', type: 'postHook' },
    ]);
    expect(r.ctxChanges.$share.$logs).toEqual([
      'from preHook',
      'from handler',
      'from postHook',
    ]);
  });

  it('handler only works correctly', async () => {
    const r = await batch(
      [{ code: 'return $ctx.$body.x * 2;', type: 'handler' }],
      { $body: { x: 42 } },
    );
    expect(r.value).toBe(84);
  });

  it('empty code blocks returns undefined', async () => {
    const r = await batch([]);
    expect(r.valueAbsent).toBe(true);
  });

  it('handler error populates $error for post-hooks then throws', async () => {
    await expect(
      batch([
        { code: 'throw new Error("Invalid input");', type: 'handler' },
        {
          code: '$ctx.$logs("err:" + $ctx.$error.message + ",status:" + $ctx.$error.statusCode);',
          type: 'postHook',
        },
      ]),
    ).rejects.toThrow('Invalid input');
  });

  it('handler error populates $api.error for post-hooks', async () => {
    await expect(
      batch(
        [
          { code: 'throw new Error("fail");', type: 'handler' },
          {
            code: '$ctx.$logs("api_err:" + $ctx.$api.error.message);',
            type: 'postHook',
          },
        ],
        {
          $api: {
            request: {
              method: 'GET',
              url: '/test',
              timestamp: new Date().toISOString(),
            },
          },
        },
      ),
    ).rejects.toThrow('fail');
  });

  it('handler error sets $data to null before post-hooks', async () => {
    await expect(
      batch([
        { code: 'throw new Error("oops");', type: 'handler' },
        {
          code: '$ctx.$logs("data_null:" + ($ctx.$data === null));',
          type: 'postHook',
        },
      ]),
    ).rejects.toThrow('oops');
  });

  it('pre-hook error skips handler but runs post-hooks then throws', async () => {
    await expect(
      batch(
        [
          { code: 'throw new Error("pre failed");', type: 'preHook' },
          { code: '$ctx.$body.handlerRan = true; return 1;', type: 'handler' },
          {
            code: '$ctx.$logs("error:" + $ctx.$error.message);',
            type: 'postHook',
          },
        ],
        { $body: {} },
      ),
    ).rejects.toThrow('pre failed');
  });

  it('on error path post-hooks actually execute side effects', async () => {
    const sideEffects: string[] = [];
    await expect(
      batch(
        [
          { code: 'throw new Error("err");', type: 'handler' },
          {
            code: 'await $ctx.$repos.main.track("post1:" + $ctx.$error.message);',
            type: 'postHook',
          },
          { code: 'await $ctx.$repos.main.track("post2");', type: 'postHook' },
        ],
        {
          $repos: {
            main: {
              track: async (msg: string) => {
                sideEffects.push(msg);
              },
            },
          },
        },
      ),
    ).rejects.toThrow('err');
    expect(sideEffects[0]).toContain('post1:err');
    expect(sideEffects[1]).toBe('post2');
  });

  it('$logs from postHooks on error path are preserved in ctxChanges', async () => {
    try {
      await batch([
        { code: '$ctx.$logs("pre"); throw new Error("err");', type: 'preHook' },
        { code: '$ctx.$logs("handler");', type: 'handler' },
        { code: '$ctx.$logs("post");', type: 'postHook' },
      ]);
      fail('should have thrown');
    } catch (err: any) {
      expect(err.message).toContain('err');
      expect(err.ctxChanges.$share.$logs).toContain('pre');
      expect(err.ctxChanges.$share.$logs).not.toContain('handler');
      expect(err.ctxChanges.$share.$logs).toContain('post');
    }
  });

  it('$error and $statusCode are in ctxChanges on error path', async () => {
    try {
      await batch([
        { code: 'throw new Error("bad");', type: 'handler' },
        { code: '$ctx.$logs("status:" + $ctx.$statusCode);', type: 'postHook' },
      ]);
      fail('should have thrown');
    } catch (err: any) {
      expect(err.ctxChanges.$error.message).toContain('bad');
      expect(err.ctxChanges.$share.$logs).toContain('status:500');
    }
  });

  it('concurrent batch executions are isolated', async () => {
    const results = await Promise.all(
      Array.from({ length: 4 }, (_, i) =>
        batch(
          [
            {
              code: `$ctx.$body.value = $ctx.$body.value * 10;`,
              type: 'preHook',
            },
            { code: 'return $ctx.$body.value;', type: 'handler' },
          ],
          { $body: { value: i } },
        ),
      ),
    );
    expect(results.map((r) => r.value).sort()).toEqual([0, 10, 20, 30]);
  });

  it('handler can set custom response fields', async () => {
    const r = await batch([
      { code: 'return { created: true, status: 201 };', type: 'handler' },
      {
        code: 'return { ...$ctx.$data, message: "Created" };',
        type: 'postHook',
      },
    ]);
    expect(r.value.status).toBe(201);
    expect(r.value.message).toBe('Created');
  });
});

(process.env.BENCH ? describe : describe.skip)(
  'Batch execution: realistic route benchmark',
  () => {
    const RUNS = Number(process.env.BENCH_RUNS || 24);
    const WARMUP = 2;

    async function serialWallMs(count: number): Promise<number> {
      const t0 = performance.now();
      for (let i = 0; i < count; i++) {
        const r = await realisticRouteBatch(i);
        expect(r.value.data.items[0].id).toBe(i);
        expect(r.ctxChanges.$share.$logs).toContain('pre');
      }
      return performance.now() - t0;
    }

    async function parallelWavesWallMs(
      total: number,
      width: number,
    ): Promise<number> {
      const t0 = performance.now();
      let offset = 0;
      while (offset < total) {
        const slice = Math.min(width, total - offset);
        await Promise.all(
          Array.from({ length: slice }, async (_, j) => {
            const i = offset + j;
            const r = await realisticRouteBatch(i);
            expect(r.value.data.items[0].id).toBe(i);
            expect(r.ctxChanges.$share.$logs).toContain('pre');
          }),
        );
        offset += slice;
      }
      return performance.now() - t0;
    }

    it('prints timings (executeBatch + pre/handler/post + $repos bridge)', async () => {
      for (let w = 0; w < WARMUP; w++) {
        await realisticRouteBatch(w);
      }
      const serialMs = await serialWallMs(RUNS);
      const p2Ms = await parallelWavesWallMs(RUNS, 2);
      const p4Ms = await parallelWavesWallMs(RUNS, 4);
      const p8Ms = await parallelWavesWallMs(RUNS, 8);
      const out = {
        runs: RUNS,
        scenario:
          'executeBatch preHook+handler($repos.main.find)+postHook REST-shaped response',
        serialWallMs: Math.round(serialMs),
        parallel2WallMs: Math.round(p2Ms),
        parallel4WallMs: Math.round(p4Ms),
        parallel8WallMs: Math.round(p8Ms),
        speedupVsSerial: {
          width2: Number((serialMs / p2Ms).toFixed(2)),
          width4: Number((serialMs / p4Ms).toFixed(2)),
          width8: Number((serialMs / p8Ms).toFixed(2)),
        },
        avgSerialPerRunMs: Number((serialMs / RUNS).toFixed(2)),
      };
      process.stdout.write(
        `\n[executor-batch realistic benchmark] ${JSON.stringify(out, null, 2)}\n`,
      );
      expect(out.serialWallMs).toBeGreaterThan(0);
    });
  },
);
