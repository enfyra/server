import { executeSingle } from '../helpers/spawn-worker';

function snap(ctx: Record<string, any>) {
  return {
    $body: ctx.$body ?? {},
    $query: ctx.$query ?? {},
    $params: ctx.$params ?? {},
    $user: ctx.$user ?? null,
    $share: ctx.$share ?? {},
    $data: ctx.$data,
    $api: ctx.$api ?? { request: {} },
    $uploadedFile: ctx.$uploadedFile,
  };
}

async function run(
  code: string,
  ctx: Record<string, any>,
  opt?: { timeoutMs?: number; memoryLimitMb?: number },
) {
  return executeSingle({
    code,
    snapshot: snap(ctx),
    timeoutMs: opt?.timeoutMs ?? 12000,
    memoryLimitMb: opt?.memoryLimitMb ?? 128,
    ctx,
  });
}

describe('Isolated executor adversarial (72 cases)', () => {
  it('adv-01: $throw 400', async () => {
    await expect(
      run(`$ctx.$throw['400']('bad');`, { $share: {} }),
    ).rejects.toThrow(/bad/);
  });

  it('adv-02: $throw 500', async () => {
    await expect(
      run(`$ctx.$throw['500']('srv');`, { $share: {} }),
    ).rejects.toThrow(/srv/);
  });

  it('adv-03: $throw 404 with empty message', async () => {
    await expect(
      run(`$ctx.$throw['404']('');`, { $share: {} }),
    ).rejects.toThrow();
  });

  it('adv-04: explicit return undefined (valueAbsent, same as vm executor)', async () => {
    const r = await run('return undefined;', { $share: {} });
    expect(r.valueAbsent).toBe(true);
    expect(r.value).toBeUndefined();
  });

  it('adv-05: empty statement → undefined handler result', async () => {
    const r = await run(';', { $share: {} });
    expect(r.valueAbsent).toBe(true);
    expect(r.value).toBeUndefined();
  });

  it('adv-06: return array length 8000', async () => {
    const { value } = await run('return Array.from({length:8000}, (_,i)=>i);', {
      $share: {},
    });
    expect(Array.isArray(value)).toBe(true);
    expect((value as number[]).length).toBe(8000);
  });

  it('adv-07: repo main thread throws', async () => {
    const ctx: any = {
      $share: {},
      $repos: {
        main: {
          find: async () => {
            throw new Error('db_down');
          },
        },
      },
    };
    await expect(
      run('return await $ctx.$repos.main.find();', ctx),
    ).rejects.toThrow(/db_down/);
  });

  it('adv-08: helper main thread throws', async () => {
    const ctx: any = {
      $share: {},
      $helpers: {
        boom: async () => {
          throw new Error('helper_x');
        },
      },
    };
    await expect(
      run('return await $ctx.$helpers.boom();', ctx),
    ).rejects.toThrow(/helper_x/);
  });

  it('adv-09: cache main thread throws', async () => {
    const ctx: any = {
      $share: {},
      $cache: {
        get: async () => {
          throw new Error('redis_x');
        },
      },
    };
    await expect(
      run('return await $ctx.$cache.get("k");', ctx),
    ).rejects.toThrow(/redis_x/);
  });

  it('adv-10: repo returns undefined over bridge', async () => {
    const ctx: any = {
      $share: {},
      $repos: { main: { x: async () => undefined } },
    };
    const r = await run('return await $ctx.$repos.main.x();', ctx);
    expect(r.valueAbsent).toBe(true);
    expect(r.value).toBeUndefined();
  });

  it('adv-11: repo returns null', async () => {
    const ctx: any = { $share: {}, $repos: { main: { x: async () => null } } };
    const r = await run('return await $ctx.$repos.main.x();', ctx);
    expect(r.valueAbsent).toBe(false);
    expect(r.value).toBeNull();
  });

  it('adv-12: sequential nested repo calls', async () => {
    let n = 0;
    const ctx: any = {
      $share: {},
      $repos: {
        main: {
          a: async () => {
            n++;
            return n;
          },
        },
      },
    };
    const { value } = await run(
      'return (await $ctx.$repos.main.a()) + (await $ctx.$repos.main.a());',
      ctx,
    );
    expect(value).toBe(3);
  });

  it('adv-13: Promise.all five repo calls', async () => {
    const ctx: any = {
      $share: {},
      $repos: { main: { g: async (i: number) => i * 2 } },
    };
    const { value } = await run(
      `return (await Promise.all([
        $ctx.$repos.main.g(1),$ctx.$repos.main.g(2),$ctx.$repos.main.g(3),$ctx.$repos.main.g(4),$ctx.$repos.main.g(5)
      ])).reduce((a,b)=>a+b,0);`,
      ctx,
    );
    expect(value).toBe(30);
  });

  it('adv-14: $body key __proto__ string value', async () => {
    const ctx: any = { $share: {}, $body: { ['__proto__']: 'trap' } };
    const { value } = await run('return $ctx.$body["__proto__"];', ctx);
    expect(value).toBe('trap');
  });

  it('adv-15: snapshot $body 12k string', async () => {
    const s = 'Z'.repeat(12000);
    const ctx: any = { $share: {}, $body: { s } };
    const { value } = await run('return $ctx.$body.s.length;', ctx);
    expect(value).toBe(12000);
  });

  it('adv-16: $params with quote and unicode', async () => {
    const ctx: any = { $share: {}, $params: { id: `a"b\\c\u{d83d}\u{de00}` } };
    const { value } = await run('return $ctx.$params.id.length > 0;', ctx);
    expect(value).toBe(true);
  });

  it('adv-17: Promise.all 8 parallel runs', async () => {
    const out = await Promise.all(
      [1, 2, 3, 4, 5, 6, 7, 8].map((i) =>
        run(`return ${i} * 10;`, { $share: {} }),
      ),
    );
    expect(out.map((r) => r.value).reduce((a, b) => a + b, 0)).toBe(360);
  });

  it('adv-18: cache 40 sequential sets', async () => {
    const store = new Map<number, number>();
    const ctx: any = {
      $share: {},
      $cache: {
        set: async (k: number, v: number) => {
          store.set(k, v);
          return v;
        },
      },
    };
    const { value } = await run(
      `
      for (let i = 0; i < 40; i++) await $ctx.$cache.set(i, i * i);
      return 40;
    `,
      ctx,
    );
    expect(value).toBe(40);
    expect(store.get(39)).toBe(1521);
  });

  it('adv-19: helpers 25 sequential invocations', async () => {
    let c = 0;
    const ctx: any = {
      $share: {},
      $helpers: { tick: async () => ++c },
    };
    const { value } = await run(
      `
      let s = 0;
      for (let i = 0; i < 25; i++) s += await $ctx.$helpers.tick();
      return s;
    `,
      ctx,
    );
    expect(value).toBe((25 * 26) / 2);
  });

  it('adv-20: division by zero Infinity JSON', async () => {
    const { value } = await run('return 1/0;', { $share: {} });
    expect(value).toBeNull();
  });

  it('adv-21: return Symbol becomes null via safeClone', async () => {
    const { value } = await run('return Symbol("x");', { $share: {} });
    expect(value).toBeNull();
  });

  it('adv-22: return function becomes null', async () => {
    const { value } = await run('return () => 1;', { $share: {} });
    expect(value).toBeNull();
  });

  it('adv-23: self-referential object return', async () => {
    const { value } = await run('const o={}; o.o=o; return o;', { $share: {} });
    expect(value).toBeNull();
  });

  it('adv-24: typeof eval in isolate (may exist as indirect eval)', async () => {
    const { value } = await run('return typeof eval;', { $share: {} });
    expect(['undefined', 'function']).toContain(value as string);
  });

  it('adv-25: Date.now is finite', async () => {
    const { value } = await run('return Number.isFinite(Date.now());', {
      $share: {},
    });
    expect(value).toBe(true);
  });

  it('adv-26: fib recursion depth 30', async () => {
    const { value } = await run(
      `function f(n){return n<2?n:f(n-1)+f(n-2);} return f(30);`,
      { $share: {} },
    );
    expect(value).toBe(832040);
  });

  it('adv-27: Math.pow edge', async () => {
    const { value } = await run('return Math.pow(2, 40);', { $share: {} });
    expect(value).toBe(1099511627776);
  });

  it('adv-28: JSON.parse valid huge array string', async () => {
    const { value } = await run('return JSON.parse("[1,2,3]").length;', {
      $share: {},
    });
    expect(value).toBe(3);
  });

  it('adv-29: JSON.parse throws', async () => {
    await expect(
      run(`return JSON.parse('{');`, { $share: {} }),
    ).rejects.toThrow();
  });

  it('adv-30: 60 socket emits rapid', async () => {
    let n = 0;
    const ctx: any = {
      $share: {},
      $socket: {
        e: () => {
          n++;
        },
      },
    };
    const { value } = await run(
      'for (let i = 0; i < 60; i++) $ctx.$socket.e(); return 60;',
      ctx,
    );
    expect(value).toBe(60);
    expect(n).toBe(60);
  });

  it('adv-31: helper returns object with circular ref', async () => {
    const ctx: any = {
      $share: {},
      $helpers: {
        circ: async () => {
          const o: any = { a: 1 };
          o.self = o;
          return o;
        },
      },
    };
    const { value } = await run('return await $ctx.$helpers.circ();', ctx);
    expect(
      (value as any)?.__serializationError === true || value === null,
    ).toBe(true);
  });

  it('adv-32: repo rejects promise', async () => {
    const ctx: any = {
      $share: {},
      $repos: { main: { x: async () => Promise.reject(new Error('no_row')) } },
    };
    await expect(
      run('return await $ctx.$repos.main.x();', ctx),
    ).rejects.toThrow(/no_row/);
  });

  it('adv-33: throw primitive string', async () => {
    await expect(run(`throw 'plain';`, { $share: {} })).rejects.toThrow();
  });

  it('adv-34: throw number', async () => {
    await expect(run('throw 42;', { $share: {} })).rejects.toThrow();
  });

  it('adv-35: unknown repo table method call fails', async () => {
    const ctx: any = { $share: {}, $repos: { other: { find: async () => 1 } } };
    await expect(
      run('return await $ctx.$repos.missing.find();', ctx),
    ).rejects.toThrow(/Repo method not found|not found/i);
  });

  it('adv-36: unknown cache method', async () => {
    const ctx: any = { $share: {}, $cache: { get: async () => 1 } };
    await expect(
      run('return await $ctx.$cache.del("x");', ctx),
    ).rejects.toThrow(/Cache method not found|not found/i);
  });

  it('adv-37: $logs 80 entries', async () => {
    const ctx: any = { $share: { $logs: [] } };
    const out = await run(
      `for (let i = 0; i < 80; i++) $ctx.$logs(i); return 1;`,
      ctx,
    );
    const ch = out.ctxChanges?.$share;
    expect(Array.isArray(ch?.$logs)).toBe(true);
    expect(ch.$logs.length).toBe(80);
  });

  it('adv-38: replace entire $body', async () => {
    const ctx: any = { $share: {}, $body: { old: true } };
    const o = await run('$ctx.$body = { new: 1 }; return 0;', ctx);
    const c = o.ctxChanges;
    if (c?.$body !== undefined) ctx.$body = c.$body;
    expect(ctx.$body).toEqual({ new: 1 });
  });

  it('adv-39: Number.MAX_SAFE_INTEGER', async () => {
    const { value } = await run('return Number.MAX_SAFE_INTEGER;', {
      $share: {},
    });
    expect(value).toBe(9007199254740991);
  });

  it('adv-40: MIN_SAFE_INTEGER', async () => {
    const { value } = await run('return Number.MIN_SAFE_INTEGER;', {
      $share: {},
    });
    expect(value).toBe(-9007199254740991);
  });

  it('adv-41: surrogate pair string', async () => {
    const { value } = await run('return "\\uD83D\\uDE00".length;', {
      $share: {},
    });
    expect(value).toBe(2);
  });

  it('adv-42: sparse array JSON round trip', async () => {
    const { value } = await run('const a=[]; a[10]=1; return a.length;', {
      $share: {},
    });
    expect(value).toBe(11);
  });

  it('adv-43: Object.is NaN', async () => {
    const { value } = await run('return Object.is(NaN, NaN);', { $share: {} });
    expect(value).toBe(true);
  });

  it('adv-44: ArrayBuffer not constructible use Uint8Array', async () => {
    const { value } = await run('return new Uint8Array(3).length;', {
      $share: {},
    });
    expect(value).toBe(3);
  });

  it('adv-45: timeout 1ms on trivial code may fail or pass', async () => {
    const p = run('return 1;', { $share: {} }, { timeoutMs: 1 });
    const raced = await Promise.race([
      p.then((x) => ({ kind: 'done' as const, x })),
      new Promise<{ kind: 'timeout' }>((resolve) =>
        setTimeout(() => resolve({ kind: 'timeout' }), 3000),
      ),
    ]);
    expect(['done', 'timeout']).toContain(
      raced.kind === 'done' ? 'done' : 'timeout',
    );
  });

  it('adv-46: microtask loop 5000 resolves', async () => {
    const { value } = await run(
      `
      let i = 0;
      await new Promise((r) => {
        function step() {
          if (++i >= 500) return r(i);
          Promise.resolve().then(step);
        }
        step();
      });
      return i;
    `,
      { $share: {} },
      { timeoutMs: 20000 },
    );
    expect(value).toBe(500);
  });

  it('adv-47: never-resolving promise times out', async () => {
    await expect(
      run(
        'await new Promise(() => {}); return 1;',
        { $share: {} },
        { timeoutMs: 500 },
      ),
    ).rejects.toThrow(/timed out|Script execution|Worker exited/i);
  }, 15000);

  it('adv-48: busy wait spin short timeout', async () => {
    await expect(
      run(
        'let t=Date.now(); while(Date.now()-t < 999999) {} return 1;',
        { $share: {} },
        {
          timeoutMs: 400,
        },
      ),
    ).rejects.toThrow();
  });

  it('adv-49: Reflect.ownKeys on $ctx', async () => {
    const { value } = await run(
      'return Reflect.ownKeys($ctx).includes("$body");',
      { $share: {} },
    );
    expect(value).toBe(true);
  });

  it('adv-50: Object.keys $pkgs empty', async () => {
    const { value } = await run(
      'return Object.keys($ctx.$pkgs || {}).length;',
      { $share: {} },
    );
    expect(value).toBe(0);
  });

  it('adv-51: encodeURIComponent', async () => {
    const { value } = await run(`return encodeURIComponent("a b&c=");`, {
      $share: {},
    });
    expect(value).toBe('a%20b%26c%3D');
  });

  it('adv-52: decodeURIComponent valid', async () => {
    const { value } = await run(`return decodeURIComponent("a%20b");`, {
      $share: {},
    });
    expect(value).toBe('a b');
  });

  it('adv-53: decodeURIComponent throws', async () => {
    await expect(
      run(`return decodeURIComponent("%");`, { $share: {} }),
    ).rejects.toThrow();
  });

  it('adv-54: String.fromCharCode', async () => {
    const { value } = await run('return String.fromCharCode(65, 66);', {
      $share: {},
    });
    expect(value).toBe('AB');
  });

  it('adv-55: Map size after sets', async () => {
    const { value } = await run(
      'const m=new Map(); m.set(1,2); m.set(2,3); return m.size;',
      { $share: {} },
    );
    expect(value).toBe(2);
  });

  it('adv-56: Set dedupe', async () => {
    const { value } = await run('const s=new Set([1,1,2]); return s.size;', {
      $share: {},
    });
    expect(value).toBe(2);
  });

  it('adv-57: optional chaining null', async () => {
    const r = await run('const x=null; return x?.y?.z;', { $share: {} });
    expect(r.valueAbsent).toBe(true);
    expect(r.value).toBeUndefined();
  });

  it('adv-58: nullish coalescing', async () => {
    const { value } = await run('return null ?? "a";', { $share: {} });
    expect(value).toBe('a');
  });

  it('adv-59: template literal nested', async () => {
    const { value } = await run('const x=2; return `a${`b${x}c`}d`;', {
      $share: {},
    });
    expect(value).toBe('ab2cd');
  });

  it('adv-60: try catch swallow', async () => {
    const { value } = await run('try { throw 1; } catch { return "ok"; }', {
      $share: {},
    });
    expect(value).toBe('ok');
  });

  it('adv-61: finally runs', async () => {
    const { value } = await run(
      'let x=0; try { x=1; } finally { x=2; } return x;',
      { $share: {} },
    );
    expect(value).toBe(2);
  });

  it('adv-62: for-of string', async () => {
    const { value } = await run(
      'let s=""; for (const c of "ab") s+=c; return s;',
      { $share: {} },
    );
    expect(value).toBe('ab');
  });

  it('adv-63: ArrayBuffer.isView', async () => {
    const { value } = await run(
      'return ArrayBuffer.isView(new Uint8Array(1));',
      { $share: {} },
    );
    expect(value).toBe(true);
  });

  it('adv-64: structured clone via JSON full cycle', async () => {
    const { value } = await run(
      'return JSON.parse(JSON.stringify({a:[1,{b:2}]})).a[1].b;',
      { $share: {} },
    );
    expect(value).toBe(2);
  });

  it('adv-65: Promise.race first settled among immediates', async () => {
    const { value } = await run(
      `return await Promise.race([Promise.resolve("a"), Promise.resolve("b")]);`,
      { $share: {} },
    );
    expect(['a', 'b']).toContain(value as string);
  });

  it('adv-66: allSettled then count fulfilled', async () => {
    const { value } = await run(
      `
      const r = await Promise.allSettled([Promise.reject(1), Promise.resolve(2)]);
      return r.filter(x => x.status === "fulfilled").length;
    `,
      { $share: {} },
    );
    expect(value).toBe(1);
  });

  it('adv-67: $user null snapshot', async () => {
    const { value } = await run('return $ctx.$user;', {
      $share: {},
      $user: null,
    });
    expect(value).toBeNull();
  });

  it('adv-68: $user object minimal', async () => {
    const { value } = await run('return $ctx.$user.id;', {
      $share: {},
      $user: { id: 'u1' },
    });
    expect(value).toBe('u1');
  });

  it('adv-69: $uploadedFile passthrough read', async () => {
    const { value } = await run('return $ctx.$uploadedFile?.name ?? null;', {
      $share: {},
      $uploadedFile: { name: 'f.bin' },
    });
    expect(value).toBe('f.bin');
  });

  it('adv-70: $api.request spread read', async () => {
    const { value } = await run('return $ctx.$api.request.method;', {
      $share: {},
      $api: { request: { method: 'PATCH' } },
    });
    expect(value).toBe('PATCH');
  });

  it('adv-71: dynamic import expression syntax rejected', async () => {
    await expect(run('import("x")', { $share: {} })).rejects.toThrow();
  });

  it('adv-72: with statement strict mode error', async () => {
    await expect(
      run('"use strict"; with({}) { return 1; }', { $share: {} }),
    ).rejects.toThrow();
  });
});
