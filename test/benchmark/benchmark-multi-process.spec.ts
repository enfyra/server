import { fork } from 'child_process';
import * as path from 'path';

const CHILD_SCRIPT = path.join(__dirname, '../helpers/benchmark-child.js');

interface ChildResult {
  totalOps: number;
  elapsedMs: number;
  latencies: number[];
}

function spawnChild(
  concurrency: number,
  durationMs: number,
): Promise<ChildResult> {
  return new Promise((resolve, reject) => {
    const child = fork(CHILD_SCRIPT, [], {
      execArgv: ['--no-node-snapshot'],
      env: { ...process.env },
      stdio: 'pipe',
    });
    child.send({ concurrency, durationMs });
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

  console.log(`  ${label}`);
  console.log(
    `    Processes: ${results.length} | Total ops: ${totalOps} | Wall: ${fmt(maxElapsed)}ms | Throughput: ${fmt(totalOps / (maxElapsed / 1000))} ops/s`,
  );
  console.log(
    `    Latency — min: ${fmt(allLatencies[0])}ms | avg: ${fmt(avg)}ms | median: ${fmt(percentile(allLatencies, 50))}ms | p95: ${fmt(percentile(allLatencies, 95))}ms | p99: ${fmt(percentile(allLatencies, 99))}ms | max: ${fmt(allLatencies[allLatencies.length - 1])}ms`,
  );
  return {
    totalOps,
    opsPerSec: totalOps / (maxElapsed / 1000),
    avgLatency: avg,
    p95: percentile(allLatencies, 95),
  };
}

describe('Multi-process benchmark', () => {
  jest.setTimeout(120000);

  it('1 process vs 2 processes — concurrency 10 per process, 5s', async () => {
    console.log('\n=== Concurrency 10 per process, 5s duration ===\n');

    const r1 = await spawnChild(10, 5000);
    const single = report('1 process', [r1]);

    const [r2a, r2b] = await Promise.all([
      spawnChild(10, 5000),
      spawnChild(10, 5000),
    ]);
    const dual = report('2 processes', [r2a, r2b]);

    const speedup = dual.opsPerSec / single.opsPerSec;
    console.log(
      `\n    Speedup: ${fmt(speedup)}x throughput | Latency change: ${fmt(((dual.avgLatency - single.avgLatency) / single.avgLatency) * 100)}%`,
    );
  });

  it('1 process vs 2 processes — concurrency 25 per process, 5s', async () => {
    console.log('\n=== Concurrency 25 per process, 5s duration ===\n');

    const r1 = await spawnChild(25, 5000);
    const single = report('1 process', [r1]);

    const [r2a, r2b] = await Promise.all([
      spawnChild(25, 5000),
      spawnChild(25, 5000),
    ]);
    const dual = report('2 processes', [r2a, r2b]);

    const speedup = dual.opsPerSec / single.opsPerSec;
    console.log(
      `\n    Speedup: ${fmt(speedup)}x throughput | Latency change: ${fmt(((dual.avgLatency - single.avgLatency) / single.avgLatency) * 100)}%`,
    );
  });

  it('1 vs 2 vs 4 processes — concurrency 10, 5s', async () => {
    console.log('\n=== Scale-out: concurrency 10, 5s duration ===\n');

    const r1 = await spawnChild(10, 5000);
    const s1 = report('1 process', [r1]);

    const r2 = await Promise.all([spawnChild(10, 5000), spawnChild(10, 5000)]);
    const s2 = report('2 processes', r2);

    const r4 = await Promise.all([
      spawnChild(10, 5000),
      spawnChild(10, 5000),
      spawnChild(10, 5000),
      spawnChild(10, 5000),
    ]);
    const s4 = report('4 processes', r4);

    console.log(
      `\n    Scale: 1→2 = ${fmt(s2.opsPerSec / s1.opsPerSec)}x | 1→4 = ${fmt(s4.opsPerSec / s1.opsPerSec)}x`,
    );
  });
});
