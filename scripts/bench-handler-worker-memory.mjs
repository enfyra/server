import { Worker } from 'worker_threads';
import os from 'node:os';
import path from 'path';
import { fileURLToPath } from 'url';
import v8 from 'v8';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const WORKER_SCRIPT = path.join(
  __dirname,
  '../src/engine/executor-engine/workers/executor.worker.js',
);

function isolateMemoryLimitMbFromTotalBytes(totalBytes) {
  const totalMb = Math.max(1, totalBytes / (1024 * 1024));
  return Math.min(128, Math.max(32, Math.round(totalMb / 40)));
}

function encodeMainThreadToIsolate(value) {
  if (value === undefined) return JSON.stringify({ __e: 'u' });
  try {
    return JSON.stringify(
      { __e: 'v', d: value },
      (_k, v) => (typeof v === 'bigint' ? v.toString() : v),
    );
  } catch {
    return JSON.stringify({ __e: 'v', d: { __serializationError: true } });
  }
}

function rssMb() {
  return process.memoryUsage().rss / (1024 * 1024);
}

function runWorkerExecute({
  code,
  snapshot,
  pkgSources = [],
  timeoutMs = 15000,
  memoryLimitMb,
  ctx = {},
}) {
  return new Promise((resolve, reject) => {
    const worker = new Worker(WORKER_SCRIPT);
    const id = `bench_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    let settled = false;

    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      worker.terminate();
      reject(new Error('timeout'));
    }, timeoutMs + 8000);

    const cleanup = () => {
      clearTimeout(timer);
      worker.terminate();
    };

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
          if (!repo || typeof repo[msg.method] !== 'function') {
            throw new Error(`repo ${msg.table}.${msg.method}`);
          }
          const result = await repo[msg.method](...args);
          worker.postMessage({
            type: 'callResult',
            callId: msg.callId,
            result: encodeMainThreadToIsolate(result),
          });
        } catch (e) {
          worker.postMessage({
            type: 'callError',
            callId: msg.callId,
            error: e.message,
          });
        }
      } else if (msg.type === 'helpersCall') {
        worker.postMessage({
          type: 'callError',
          callId: msg.callId,
          error: 'no helpers in bench',
        });
      } else if (msg.type === 'socketCall') {
      } else if (msg.type === 'cacheCall') {
        worker.postMessage({
          type: 'callError',
          callId: msg.callId,
          error: 'no cache in bench',
        });
      } else if (msg.type === 'dispatchCall') {
        worker.postMessage({
          type: 'callError',
          callId: msg.callId,
          error: 'no dispatch in bench',
        });
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
      reject(new Error(`exit ${code}`));
    });

    worker.postMessage({
      type: 'execute',
      id,
      code,
      pkgSources,
      snapshot,
      timeoutMs,
      memoryLimitMb,
    });
  });
}

async function peakRssDuring(label, fn) {
  let peak = rssMb();
  const iv = setInterval(() => {
    const r = rssMb();
    if (r > peak) peak = r;
  }, 1);
  try {
    await fn();
  } finally {
    clearInterval(iv);
  }
  if (typeof global.gc === 'function') {
    global.gc();
    await new Promise((r) => setImmediate(r));
  }
  const settled = rssMb();
  return { label, peakRssMb: peak, settledRssMb: settled };
}

function fatSnapshot() {
  const rows = Array.from({ length: 200 }, (_, i) => ({
    id: i,
    name: `row_${i}`,
    meta: { x: 'y'.repeat(40) },
  }));
  return {
    $body: {},
    $query: { filter: JSON.stringify({ id: { _in: rows.map((r) => r.id) } }) },
    $params: {},
    $user: { id: 1, email: 'bench@enfyra.local', role: { name: 'admin' } },
    $share: { $logs: [] },
    $data: {},
    $api: { request: { method: 'GET', url: '/api/table_definition' } },
    $repos: { main: { preview: rows } },
  };
}

const scenarios = [
  {
    name: 'minimal',
    code: 'return 1;',
    snapshot: {
      $body: {},
      $query: {},
      $params: {},
      $user: null,
      $share: { $logs: [] },
      $data: {},
      $api: { request: {} },
    },
  },
  {
    name: 'default_crud_get',
    code: 'return await $ctx.$repos.main.find();',
    snapshot: null,
    buildSnapshot: () => ({
      $body: {},
      $query: { limit: 10 },
      $params: {},
      $user: null,
      $share: { $logs: [] },
      $data: {},
      $api: { request: {} },
    }),
    ctx: {
      $repos: {
        main: {
          find: async () => ({ data: [{ id: 1, name: 'a' }], meta: {} }),
        },
      },
    },
  },
  {
    name: 'fat_context',
    code: 'return ($ctx.$query.filter ? 1 : 0);',
    snapshot: null,
    buildSnapshot: fatSnapshot,
  },
];

async function main() {
  const gcAvailable = typeof global.gc === 'function';
  if (!gcAvailable) {
    console.error(
      'Warning: global.gc unavailable; RSS between runs may be noisier. Prefer: yarn bench:executor (sets --expose-gc).',
    );
  }

  const isolateMemoryLimitMb = isolateMemoryLimitMbFromTotalBytes(os.totalmem());

  const report = {
    node: process.version,
    v8: process.versions.v8,
    isolateMemoryLimitMb,
    heapLimitMb: Math.round(v8.getHeapStatistics().heap_size_limit / (1024 * 1024)),
    scenarios: [],
    parallel: null,
    note:
      'peakRssMb is process RSS (main + worker threads). Use as relative comparison between scenarios; absolute MB varies by OS allocator.',
  };

  for (const sc of scenarios) {
    const snapshot = sc.buildSnapshot ? sc.buildSnapshot() : sc.snapshot;
    const ctx = sc.ctx ?? {};
    const samples = [];
    for (let i = 0; i < 12; i++) {
      const r = await peakRssDuring(sc.name, () =>
        runWorkerExecute({
          code: sc.code,
          snapshot,
          ctx,
          memoryLimitMb: isolateMemoryLimitMb,
        }),
      );
      samples.push(r.peakRssMb);
    }
    samples.sort((a, b) => a - b);
    const p95 = samples[Math.floor(samples.length * 0.95)] ?? samples[samples.length - 1];
    const median = samples[Math.floor(samples.length * 0.5)];
    report.scenarios.push({
      name: sc.name,
      runs: samples.length,
      rssPeakMb_median: Math.round(median * 10) / 10,
      rssPeakMb_p95: Math.round(p95 * 10) / 10,
      rssPeakMb_min: Math.round(samples[0] * 10) / 10,
      rssPeakMb_max: Math.round(samples[samples.length - 1] * 10) / 10,
    });
  }

  const baseline = rssMb();
  const parallelCount = 8;
  const workers = [];
  for (let i = 0; i < parallelCount; i++) {
    workers.push(
      runWorkerExecute({
        code: 'return ' + i + ';',
        snapshot: scenarios[0].snapshot,
        memoryLimitMb: isolateMemoryLimitMb,
      }),
    );
  }
  let peak = baseline;
  const sampler = setInterval(() => {
    const r = rssMb();
    if (r > peak) peak = r;
  }, 1);
  await Promise.all(workers);
  clearInterval(sampler);
  if (global.gc) {
    global.gc();
    await new Promise((r) => setImmediate(r));
  }
  const afterParallel = rssMb();
  report.parallel = {
    concurrentWorkers: parallelCount,
    baselineRssMb: Math.round(baseline * 10) / 10,
    peakRssMb: Math.round(peak * 10) / 10,
    deltaPeakMb: Math.round((peak - baseline) * 10) / 10,
    afterRssMb: Math.round(afterParallel * 10) / 10,
    approxMbPerWorkerFromDelta:
      Math.round(((peak - baseline) / parallelCount) * 10) / 10,
  };

  report.recommendation = {
    isolatedVmMemoryLimitMb: isolateMemoryLimitMb,
    planningPerWorkerProcessMb:
      'Use parallel.deltaPeakMb / concurrentWorkers as a rough RSS increment per concurrent handler worker on this machine; add headroom for DB pool and caches.',
  };

  console.log(JSON.stringify(report, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exitCode = 1;
});
