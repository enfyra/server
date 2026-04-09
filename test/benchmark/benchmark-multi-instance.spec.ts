import { fork } from 'child_process';
import * as path from 'path';

const CHILD_SCRIPT = path.join(__dirname, '../helpers/benchmark-child.js');
const RUN_BENCH = process.env.RUN_BENCHMARK_TESTS === '1';

interface ChildResult {
  totalOps: number;
  elapsedMs: number;
  latencies: number[];
}

function spawnChild(
  concurrency: number,
  durationMs: number,
  dbLatencyMs = 0,
): Promise<ChildResult> {
  return new Promise((resolve, reject) => {
    const child = fork(CHILD_SCRIPT, [], {
      execArgv: ['--no-node-snapshot'],
      env: { ...process.env },
      stdio: 'pipe',
    });
    child.send({ concurrency, durationMs, dbLatencyMs });
    child.on('message', (msg: any) => {
      if (msg.type === 'result') {
        child.kill();
        resolve(msg.data);
      }
    });
    child.on('error', reject);
    child.on('exit', (code) => {
      if (code !== 0 && code !== null)
        reject(new Error(`Child exited ${code}`));
    });
    setTimeout(() => {
      child.kill();
      reject(new Error('Child timeout'));
    }, durationMs + 60000);
  });
}

function percentile(sorted: number[], p: number): number {
  const idx = Math.ceil((p / 100) * sorted.length) - 1;
  return sorted[Math.max(0, idx)];
}

function fmt(n: number): string {
  return n.toFixed(1);
}

function report(label: string, results: ChildResult[]) {
  const totalOps = results.reduce((s, r) => s + r.totalOps, 0);
  const maxElapsed = Math.max(...results.map((r) => r.elapsedMs));
  const allLatencies = results
    .flatMap((r) => r.latencies)
    .sort((a, b) => a - b);
  const avg = allLatencies.reduce((a, b) => a + b, 0) / allLatencies.length;
  const p95 = percentile(allLatencies, 95);

  console.log(`  ${label}`);
  console.log(
    `    Throughput: ${fmt(totalOps / (maxElapsed / 1000))} ops/s | Avg: ${fmt(avg)}ms | Median: ${fmt(percentile(allLatencies, 50))}ms | p95: ${fmt(p95)}ms`,
  );
  return { opsPerSec: totalOps / (maxElapsed / 1000), avgLatency: avg, p95 };
}

function diff(label: string, single: any, multi: any) {
  const tpDiff = (multi.opsPerSec / single.opsPerSec - 1) * 100;
  const latDiff = (1 - multi.avgLatency / single.avgLatency) * 100;
  const p95Diff = (1 - multi.p95 / single.p95) * 100;
  console.log(
    `    → Throughput ${tpDiff > 0 ? '+' : ''}${fmt(tpDiff)}% | Avg latency ${latDiff > 0 ? '' : '-'}${fmt(Math.abs(latDiff))}% ${latDiff > 0 ? 'faster' : 'slower'} | p95 ${p95Diff > 0 ? '' : '-'}${fmt(Math.abs(p95Diff))}% ${p95Diff > 0 ? 'faster' : 'slower'}`,
  );
}

describe(
  RUN_BENCH
    ? 'Multi-instance with simulated DB I/O'
    : 'Multi-instance with simulated DB I/O (skipped)',
  () => {
    if (!RUN_BENCH) {
      it('skipped (set RUN_BENCHMARK_TESTS=1)', () => undefined);
      return;
    }
  jest.setTimeout(180000);
  const D = 5000;
  const C = 20; // total concurrent per test

  it('DB 0ms (instant) — baseline', async () => {
    console.log('\n=== DB latency: 0ms (instant mock) ===\n');
    const s1 = report('1×20', [await spawnChild(C, D, 0)]);
    const s2 = report(
      '2×10',
      await Promise.all([spawnChild(C / 2, D, 0), spawnChild(C / 2, D, 0)]),
    );
    diff('', s1, s2);
  });

  it('DB 5ms — fast cache / in-memory', async () => {
    console.log('\n=== DB latency: 5ms ===\n');
    const s1 = report('1×20', [await spawnChild(C, D, 5)]);
    const s2 = report(
      '2×10',
      await Promise.all([spawnChild(C / 2, D, 5), spawnChild(C / 2, D, 5)]),
    );
    diff('', s1, s2);
  });

  it('DB 10ms — local DB / Redis', async () => {
    console.log('\n=== DB latency: 10ms ===\n');
    const s1 = report('1×20', [await spawnChild(C, D, 10)]);
    const s2 = report(
      '2×10',
      await Promise.all([spawnChild(C / 2, D, 10), spawnChild(C / 2, D, 10)]),
    );
    diff('', s1, s2);
  });

  it('DB 30ms — typical SQL query', async () => {
    console.log('\n=== DB latency: 30ms ===\n');
    const s1 = report('1×20', [await spawnChild(C, D, 30)]);
    const s2 = report(
      '2×10',
      await Promise.all([spawnChild(C / 2, D, 30), spawnChild(C / 2, D, 30)]),
    );
    diff('', s1, s2);
  });

  it('DB 50ms — moderate query', async () => {
    console.log('\n=== DB latency: 50ms ===\n');
    const s1 = report('1×20', [await spawnChild(C, D, 50)]);
    const s2 = report(
      '2×10',
      await Promise.all([spawnChild(C / 2, D, 50), spawnChild(C / 2, D, 50)]),
    );
    diff('', s1, s2);
  });

  it('DB 100ms — heavy query / remote DB', async () => {
    console.log('\n=== DB latency: 100ms ===\n');
    const s1 = report('1×20', [await spawnChild(C, D, 100)]);
    const s2 = report(
      '2×10',
      await Promise.all([spawnChild(C / 2, D, 100), spawnChild(C / 2, D, 100)]),
    );
    diff('', s1, s2);
  });

  it('Summary table', async () => {
    console.log('\n=== High concurrency: 40 total, DB 20ms ===\n');
    const s1 = report('1×40', [await spawnChild(40, D, 20)]);
    const s2 = report(
      '2×20',
      await Promise.all([spawnChild(20, D, 20), spawnChild(20, D, 20)]),
    );
    const s4 = report(
      '4×10',
      await Promise.all([
        spawnChild(10, D, 20),
        spawnChild(10, D, 20),
        spawnChild(10, D, 20),
        spawnChild(10, D, 20),
      ]),
    );
    console.log('');
    diff('1→2', s1, s2);
    diff('1→4', s1, s4);
  });
});
