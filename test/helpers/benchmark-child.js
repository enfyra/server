'use strict';

const { Worker } = require('worker_threads');

const WORKER_SCRIPT = require.resolve('@enfyra/kernel/execution/worker.js');

const SNAPSHOT = {
  $body: {},
  $query: {},
  $params: {},
  $user: null,
  $share: { $logs: [] },
  $api: { request: {} },
};

function encodeMainThreadToIsolate(value) {
  if (value === undefined) return JSON.stringify({ __e: 'u' });
  try {
    return JSON.stringify({ __e: 'v', d: value }, (_k, v) => (typeof v === 'bigint' ? v.toString() : v));
  } catch {
    return JSON.stringify({ __e: 'v', d: { __serializationError: true } });
  }
}

function execOnce(ctx) {
  return new Promise((resolve, reject) => {
    const worker = new Worker(WORKER_SCRIPT);
    const id = `b_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    let settled = false;

    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      worker.terminate();
      reject(new Error('timeout'));
    }, 20000);

    const cleanup = () => { clearTimeout(timer); worker.terminate(); };

    worker.on('message', async (msg) => {
      if (msg.type === 'result') {
        if (settled) return;
        settled = true;
        cleanup();
        if (msg.success) resolve(msg);
        else reject(new Error(msg.error?.message || 'fail'));
      } else if (msg.type === 'repoCall') {
        try {
          const args = JSON.parse(msg.argsJson);
          const repo = ctx?.$repos?.[msg.table];
          if (!repo || typeof repo[msg.method] !== 'function') throw new Error('not found');
          const result = await repo[msg.method](...args);
          worker.postMessage({ type: 'callResult', callId: msg.callId, result: encodeMainThreadToIsolate(result) });
        } catch (e) {
          worker.postMessage({ type: 'callError', callId: msg.callId, error: e.message });
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
      reject(new Error(`Worker exited ${code}`));
    });

    worker.postMessage({
      type: 'execute',
      id,
      code: 'return await $ctx.$repos.main.find();',
      pkgSources: [],
      snapshot: SNAPSHOT,
      timeoutMs: 15000,
      memoryLimitMb: 128,
    });
  });
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

process.on('message', async (msg) => {
  const { concurrency, durationMs, dbLatencyMs = 0 } = msg;
  const ctx = {
    $repos: {
      main: {
        find: async () => {
          if (dbLatencyMs > 0) await sleep(dbLatencyMs);
          return {
            data: Array.from({ length: 10 }, (_, i) => ({ id: i, name: `item_${i}` })),
            meta: { total: 10 },
          };
        },
      },
    },
  };

  const latencies = [];
  let totalOps = 0;
  const startTime = Date.now();
  const deadline = startTime + durationMs;

  async function worker() {
    while (Date.now() < deadline) {
      const t0 = process.hrtime.bigint();
      await execOnce(ctx);
      const elapsed = Number(process.hrtime.bigint() - t0) / 1e6;
      latencies.push(elapsed);
      totalOps++;
    }
  }

  const workers = Array.from({ length: concurrency }, () => worker());
  await Promise.all(workers);

  const elapsedMs = Date.now() - startTime;
  process.send({ type: 'result', data: { totalOps, elapsedMs, latencies } });
});
