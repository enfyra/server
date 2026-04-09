import { executeSingle } from '../helpers/spawn-worker';

function snap(ctx: Record<string, any> = {}) {
  return {
    $body: ctx.$body ?? {},
    $query: ctx.$query ?? {},
    $params: ctx.$params ?? {},
    $user: ctx.$user ?? null,
    $share: ctx.$share ?? { $logs: [] },
    $data: ctx.$data,
    $api: ctx.$api ?? { request: {} },
  };
}

function exec(code: string, ctx: Record<string, any> = {}) {
  return executeSingle({
    code,
    snapshot: snap(ctx),
    timeoutMs: 15000,
    ctx,
  });
}

function ms(hrtime: [number, number]): number {
  return hrtime[0] * 1000 + hrtime[1] / 1e6;
}

function percentile(sorted: number[], p: number): number {
  const idx = Math.ceil((p / 100) * sorted.length) - 1;
  return sorted[Math.max(0, idx)];
}

function stats(times: number[]) {
  const sorted = [...times].sort((a, b) => a - b);
  const sum = sorted.reduce((a, b) => a + b, 0);
  return {
    min: sorted[0],
    max: sorted[sorted.length - 1],
    avg: sum / sorted.length,
    median: percentile(sorted, 50),
    p95: percentile(sorted, 95),
    p99: percentile(sorted, 99),
  };
}

function fmt(n: number): string {
  return n.toFixed(1) + 'ms';
}

function printStats(label: string, times: number[]) {
  const s = stats(times);
  console.log(
    `  ${label.padEnd(35)} | min ${fmt(s.min).padStart(8)} | avg ${fmt(s.avg).padStart(8)} | median ${fmt(s.median).padStart(8)} | p95 ${fmt(s.p95).padStart(8)} | p99 ${fmt(s.p99).padStart(8)} | max ${fmt(s.max).padStart(8)}`,
  );
}

describe('Spawn-per-request benchmark', () => {
  jest.setTimeout(120000);

  it('1. Baseline: trivial return', async () => {
    const times: number[] = [];
    // warmup
    await exec('return 1;');

    for (let i = 0; i < 50; i++) {
      const start = process.hrtime();
      await exec('return 1;');
      times.push(ms(process.hrtime(start)));
    }
    console.log('\n--- Sequential: trivial handler (return 1) x50 ---');
    printStats('spawn + isolate + return', times);
  });

  it('2. CPU-bound: fib(25)', async () => {
    const times: number[] = [];
    const code = 'function f(n){return n<2?n:f(n-1)+f(n-2);} return f(25);';

    for (let i = 0; i < 30; i++) {
      const start = process.hrtime();
      await exec(code);
      times.push(ms(process.hrtime(start)));
    }
    console.log('\n--- Sequential: CPU-bound fib(25) x30 ---');
    printStats('spawn + isolate + fib(25)', times);
  });

  it('3. RPC bridge: single repo call', async () => {
    const times: number[] = [];
    const ctx: any = {
      $share: {},
      $repos: {
        main: {
          find: async () => ({
            data: Array.from({ length: 20 }, (_, i) => ({
              id: i,
              name: `item_${i}`,
            })),
            meta: { total: 20 },
          }),
        },
      },
    };

    for (let i = 0; i < 50; i++) {
      const start = process.hrtime();
      await exec('return await $ctx.$repos.main.find();', ctx);
      times.push(ms(process.hrtime(start)));
    }
    console.log('\n--- Sequential: 1 repo call (20 rows) x50 ---');
    printStats('spawn + isolate + 1 RPC', times);
  });

  it('4. RPC bridge: 5 sequential repo calls', async () => {
    const times: number[] = [];
    const ctx: any = {
      $share: {},
      $repos: {
        main: { find: async () => ({ data: [{ id: 1 }] }) },
        users: { findOne: async () => ({ id: 1, email: 'a@b.c' }) },
      },
    };
    const code = `
      const r1 = await $ctx.$repos.main.find();
      const r2 = await $ctx.$repos.users.findOne();
      const r3 = await $ctx.$repos.main.find();
      const r4 = await $ctx.$repos.users.findOne();
      const r5 = await $ctx.$repos.main.find();
      return { count: 5 };
    `;

    for (let i = 0; i < 30; i++) {
      const start = process.hrtime();
      await exec(code, ctx);
      times.push(ms(process.hrtime(start)));
    }
    console.log('\n--- Sequential: 5 repo calls x30 ---');
    printStats('spawn + isolate + 5 RPCs', times);
  });

  it('5. Concurrent: 10 parallel trivial', async () => {
    const times: number[] = [];

    for (let round = 0; round < 5; round++) {
      const start = process.hrtime();
      await Promise.all(Array.from({ length: 10 }, () => exec('return 1;')));
      times.push(ms(process.hrtime(start)));
    }
    console.log('\n--- Concurrent: 10 parallel trivial handlers x5 rounds ---');
    printStats('10 workers parallel (wall)', times);
  });

  it('6. Concurrent: 25 parallel trivial', async () => {
    const times: number[] = [];

    for (let round = 0; round < 5; round++) {
      const start = process.hrtime();
      await Promise.all(Array.from({ length: 25 }, () => exec('return 1;')));
      times.push(ms(process.hrtime(start)));
    }
    console.log('\n--- Concurrent: 25 parallel trivial handlers x5 rounds ---');
    printStats('25 workers parallel (wall)', times);
  });

  it('7. Concurrent: 50 parallel trivial', async () => {
    const times: number[] = [];

    for (let round = 0; round < 3; round++) {
      const start = process.hrtime();
      await Promise.all(Array.from({ length: 50 }, () => exec('return 1;')));
      times.push(ms(process.hrtime(start)));
    }
    console.log('\n--- Concurrent: 50 parallel trivial handlers x3 rounds ---');
    printStats('50 workers parallel (wall)', times);
  });

  it('8. Concurrent: 100 parallel trivial', async () => {
    const times: number[] = [];

    for (let round = 0; round < 3; round++) {
      const start = process.hrtime();
      await Promise.all(Array.from({ length: 100 }, () => exec('return 1;')));
      times.push(ms(process.hrtime(start)));
    }
    console.log(
      '\n--- Concurrent: 100 parallel trivial handlers x3 rounds ---',
    );
    printStats('100 workers parallel (wall)', times);
  });

  it('9. Concurrent: 10 parallel with RPC', async () => {
    const times: number[] = [];
    const ctx: any = {
      $share: {},
      $repos: {
        main: {
          find: async () => ({
            data: Array.from({ length: 10 }, (_, i) => ({ id: i })),
          }),
        },
      },
    };
    const code = 'return await $ctx.$repos.main.find();';

    for (let round = 0; round < 5; round++) {
      const start = process.hrtime();
      await Promise.all(Array.from({ length: 10 }, () => exec(code, ctx)));
      times.push(ms(process.hrtime(start)));
    }
    console.log('\n--- Concurrent: 10 parallel with 1 RPC each x5 rounds ---');
    printStats('10 workers + RPC parallel', times);
  });

  it('10. Concurrent: 25 parallel with RPC', async () => {
    const times: number[] = [];
    const ctx: any = {
      $share: {},
      $repos: {
        main: {
          find: async () => ({
            data: Array.from({ length: 10 }, (_, i) => ({ id: i })),
          }),
        },
      },
    };
    const code = 'return await $ctx.$repos.main.find();';

    for (let round = 0; round < 5; round++) {
      const start = process.hrtime();
      await Promise.all(Array.from({ length: 25 }, () => exec(code, ctx)));
      times.push(ms(process.hrtime(start)));
    }
    console.log('\n--- Concurrent: 25 parallel with 1 RPC each x5 rounds ---');
    printStats('25 workers + RPC parallel', times);
  });

  it('11. Throughput: max sequential ops in 3s', async () => {
    let count = 0;
    const deadline = Date.now() + 3000;
    while (Date.now() < deadline) {
      await exec('return 1;');
      count++;
    }
    console.log(`\n--- Throughput: sequential trivial handlers in 3s ---`);
    console.log(`  Total: ${count} ops | ${(count / 3).toFixed(1)} ops/s`);
  });

  it('12. Throughput: max concurrent (batch of 10) in 3s', async () => {
    let count = 0;
    const deadline = Date.now() + 3000;
    while (Date.now() < deadline) {
      await Promise.all(Array.from({ length: 10 }, () => exec('return 1;')));
      count += 10;
    }
    console.log(`\n--- Throughput: concurrent (batch 10) trivial in 3s ---`);
    console.log(`  Total: ${count} ops | ${(count / 3).toFixed(1)} ops/s`);
  });
});
