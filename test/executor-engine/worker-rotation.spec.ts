import { describe, it, expect, afterEach } from 'vitest';
import * as path from 'path';
import {
  WorkerPool,
  PoolEntry,
} from '@enfyra/kernel';

const FAKE_WORKER = path.join(__dirname, 'rotation-worker-fixture.js');

const GB = 1024 * 1024 * 1024;
const RSS_CEILING = 0.99;
const TASKS_CAP = 64;

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

async function waitFor(
  cond: () => boolean,
  timeoutMs = 2000,
  stepMs = 10,
): Promise<void> {
  const start = Date.now();
  while (!cond()) {
    if (Date.now() - start > timeoutMs) {
      throw new Error(`waitFor timeout after ${timeoutMs}ms`);
    }
    await sleep(stepMs);
  }
}

function runTask(
  pool: WorkerPool,
  entry: PoolEntry,
  taskId: string,
  payload: { triggerHeapRatio?: number; delayMs?: number },
): Promise<any> {
  return new Promise((resolve) => {
    const abortController = new AbortController();
    pool.registerTask(entry, taskId, {
      abortController,
      onResult: (msg) => {
        pool.unregisterTask(entry, taskId);
        resolve(msg);
      },
      onIoCall: () => {},
    });
    entry.worker.postMessage({ type: 'execute', id: taskId, ...payload });
  });
}

describe('WorkerPool rotation (heap-driven)', () => {
  let pool: WorkerPool | null = null;

  afterEach(() => {
    if (pool) {
      pool.destroyAll();
      pool = null;
    }
  });

  it('never dispatches queued work onto an exiting worker from a result callback', async () => {
    pool = new WorkerPool(1, GB, RSS_CEILING, 1, FAKE_WORKER);
    const entry = await pool.dispatch();
    pool.registerTask(entry, 'running', {
      abortController: new AbortController(),
      onResult: () => pool!.unregisterTask(entry, 'running'),
      onIoCall: () => {},
    });
    const waiting = pool.dispatch();
    void waiting.catch(() => {});
    process.kill(entry.worker.pid!, 'SIGKILL');
    const replacement = await waiting;
    expect(replacement).not.toBe(entry);
    pool.releaseReservation(replacement);
    entry.worker.terminate();
  });

  it.each(['explicit', 'rotation', 'shutdown'] as const)(
    'aborts running host I/O before %s worker termination',
    async (reason) => {
      pool = new WorkerPool(1, GB, RSS_CEILING, 1, FAKE_WORKER, undefined, undefined, 30);
      const entry = await pool.dispatch();
      const controller = new AbortController();
      let abortEvents = 0;
      let abortedAtTermination: boolean | undefined;
      controller.signal.addEventListener('abort', () => { abortEvents++; });
      const terminate = entry.worker.terminate.bind(entry.worker);
      vi.spyOn(entry.worker, 'terminate').mockImplementation(() => {
        abortedAtTermination = controller.signal.aborted;
        terminate();
      });
      const finished = new Promise<void>((resolve) => {
        pool!.registerTask(entry, 'pending-io', {
          abortController: controller,
          onResult: () => {
            pool!.unregisterTask(entry, 'pending-io');
            resolve();
          },
          onIoCall: () => {},
        });
      });
      if (reason === 'rotation') {
        (pool as any).rotateEntry(entry, 'test');
        expect(controller.signal.aborted).toBe(false);
      } else if (reason === 'shutdown') {
        pool.destroyAll();
      } else {
        pool.terminateEntry(entry, 'test');
      }
      await finished;
      expect(abortedAtTermination).toBe(true);
      expect(abortEvents).toBe(1);
    },
  );

  it('keeps reserved work alive during rotation', async () => {
    pool = new WorkerPool(1, GB, RSS_CEILING, 1, FAKE_WORKER);
    const entry = await pool.dispatch();
    const terminate = vi.spyOn(entry.worker, 'terminate');
    (pool as any).rotateEntry(entry, 'test');
    expect(terminate).not.toHaveBeenCalled();
    pool.releaseReservation(entry);
    expect(terminate).toHaveBeenCalledTimes(1);
  });

  it('wakes queued dispatches as soon as a rotation replacement exists', async () => {
    pool = new WorkerPool(1, GB, RSS_CEILING, 1, FAKE_WORKER);
    const entry = await pool.dispatch();
    pool.registerTask(entry, 'running', {
      abortController: new AbortController(), onResult: () => {}, onIoCall: () => {},
    });
    let assigned: PoolEntry | undefined;
    const waiting = pool.dispatch().then((next) => { assigned = next; });
    void waiting.catch(() => {});
    (pool as any).rotateEntry(entry, 'test');
    await Promise.resolve();
    expect(assigned).toBeDefined();
    expect(assigned).not.toBe(entry);
    await waiting;
    pool.releaseReservation(assigned!);
  });

  it('does NOT rotate when heapRatio below threshold', async () => {
    pool = new WorkerPool(1, GB, RSS_CEILING, TASKS_CAP, FAKE_WORKER);
    await sleep(50);

    const before = pool.getEntries()[0];
    const entry = await pool.dispatch();
    const result = await runTask(pool, entry, 't1', { triggerHeapRatio: 0.3 });

    expect(result.success).toBe(true);
    await sleep(50);

    const after = pool.getEntries();
    expect(after.length).toBe(1);
    expect(after[0]).toBe(before);
    expect(after[0].draining).toBe(false);
  });

  it('rotates when heapRatio >= threshold (spawns replacement, old drains)', async () => {
    const rotateCalls: Array<{ reason: string; ageMs: number }> = [];
    pool = new WorkerPool(
      1,
      GB,
      RSS_CEILING,
      TASKS_CAP,
      FAKE_WORKER,
      undefined,
      (info) => rotateCalls.push(info),
    );
    await sleep(50);

    const originalEntry = pool.getEntries()[0];

    const dispatched = await pool.dispatch();
    expect(dispatched).toBe(originalEntry);

    const result = await runTask(pool, dispatched, 't1', {
      triggerHeapRatio: 0.85,
    });
    expect(result.success).toBe(true);
    expect(result.heapRatio).toBe(0.85);

    await waitFor(
      () =>
        pool!.getEntries().length === 1 &&
        pool!.getEntries()[0] !== originalEntry,
    );

    expect(rotateCalls.length).toBe(1);
    expect(rotateCalls[0].reason).toMatch(/heap=85%/);

    const after = pool.getEntries();
    expect(after.length).toBe(1);
    expect(after[0]).not.toBe(originalEntry);
    expect(after[0].draining).toBe(false);
  });

  it('routes NEW tasks to replacement during drain (not to draining worker)', async () => {
    pool = new WorkerPool(1, GB, RSS_CEILING, TASKS_CAP, FAKE_WORKER);
    await sleep(50);

    const originalEntry = pool.getEntries()[0];

    const longTaskEntry = await pool.dispatch();
    expect(longTaskEntry).toBe(originalEntry);

    const longTaskPromise = runTask(pool, longTaskEntry, 'long', {
      triggerHeapRatio: 0.3,
      delayMs: 300,
    });

    const trigger = await runTask(pool, originalEntry, 'trigger', {
      triggerHeapRatio: 0.9,
    });
    expect(trigger.success).toBe(true);

    await waitFor(() => originalEntry.draining === true);
    await waitFor(() => pool!.getEntries().length === 2);

    const nextEntry = await pool.dispatch();
    expect(nextEntry).not.toBe(originalEntry);
    expect(nextEntry.draining).toBe(false);

    const nextResult = await runTask(pool, nextEntry, 'new-task', {
      triggerHeapRatio: 0.2,
    });
    expect(nextResult.success).toBe(true);

    await longTaskPromise;

    await waitFor(
      () =>
        pool!.getEntries().length === 1 &&
        pool!.getEntries()[0] !== originalEntry,
      3000,
    );
  });

  it('terminates draining worker ONLY after in-flight tasks complete', async () => {
    pool = new WorkerPool(1, GB, RSS_CEILING, TASKS_CAP, FAKE_WORKER);
    await sleep(50);

    const originalEntry = pool.getEntries()[0];

    const inflightPromise = runTask(pool, originalEntry, 'inflight', {
      triggerHeapRatio: 0.1,
      delayMs: 250,
    });

    const trigger = await runTask(pool, originalEntry, 'trigger', {
      triggerHeapRatio: 0.85,
    });
    expect(trigger.success).toBe(true);

    await waitFor(() => originalEntry.draining === true);
    await waitFor(() => pool!.getEntries().length === 2);

    expect(pool.getEntries().includes(originalEntry)).toBe(true);
    expect(originalEntry.tasks.size).toBe(1);

    await inflightPromise;

    await waitFor(() => !pool!.getEntries().includes(originalEntry), 2000);
    expect(pool.getEntries().length).toBe(1);
  });

  it('force-terminates draining worker after drain timeout', async () => {
    pool = new WorkerPool(
      1,
      GB,
      RSS_CEILING,
      TASKS_CAP,
      FAKE_WORKER,
      undefined,
      undefined,
      100,
    );
    await sleep(50);

    const originalEntry = pool.getEntries()[0];

    const stuckTaskPromise = runTask(pool, originalEntry, 'stuck', {
      triggerHeapRatio: 0.1,
      delayMs: 10_000,
    });

    const trigger = await runTask(pool, originalEntry, 'trigger', {
      triggerHeapRatio: 0.9,
    });
    expect(trigger.success).toBe(true);

    await waitFor(() => originalEntry.draining === true);
    expect(originalEntry.tasks.size).toBe(1);

    const stuckResult = await stuckTaskPromise;
    expect(stuckResult.success).toBe(false);
    expect(stuckResult.error?.message).toMatch(/crashed/i);

    await waitFor(() => !pool!.getEntries().includes(originalEntry), 1000);
  });

  it('crash handler does NOT re-fire for graceful drain exit', async () => {
    let crashCount = 0;
    pool = new WorkerPool(
      1,
      GB,
      RSS_CEILING,
      TASKS_CAP,
      FAKE_WORKER,
      () => crashCount++,
    );
    await sleep(50);

    const entry = await pool.dispatch();
    await runTask(pool, entry, 't1', { triggerHeapRatio: 0.9 });

    await waitFor(() => pool!.getEntries().length === 1);
    await sleep(100);

    expect(crashCount).toBe(0);
  });

  it('removes and rejects a queued dispatch when its signal aborts', async () => {
    pool = new WorkerPool(1, GB, RSS_CEILING, 1, FAKE_WORKER);
    await sleep(50);
    const entry = await pool.dispatch();
    const active = runTask(pool, entry, 'active-waiter-control', {
      triggerHeapRatio: 0.1,
      delayMs: 200,
    });
    const controller = new AbortController();
    const waiting = pool.dispatch(controller.signal);
    expect(pool.getWaitingCount()).toBe(1);

    controller.abort();

    await expect(waiting).rejects.toThrow('Execution aborted before dispatch');
    expect(pool.getWaitingCount()).toBe(0);
    await active;
  });

  it('rejects queued dispatches when the pool is destroyed', async () => {
    pool = new WorkerPool(1, GB, RSS_CEILING, 1, FAKE_WORKER);
    await sleep(50);
    const entry = await pool.dispatch();
    void runTask(pool, entry, 'destroy-active', {
      triggerHeapRatio: 0.1,
      delayMs: 10_000,
    });
    const waiting = pool.dispatch();
    expect(pool.getWaitingCount()).toBe(1);

    pool.destroyAll();

    await expect(waiting).rejects.toThrow('Executor worker pool was destroyed');
    pool = null;
  });

  it('drains excess workers when the pool scales down', async () => {
    pool = new WorkerPool(3, GB, RSS_CEILING, TASKS_CAP, FAKE_WORKER);
    await sleep(50);
    expect(pool.getEntries()).toHaveLength(3);

    pool.resize(1);

    await waitFor(() => pool!.getEntries().length === 1);
    expect(pool.getEntries()[0].draining).toBe(false);
  });

  it('destroyAll terminates workers without crash replacement', async () => {
    let crashCount = 0;
    pool = new WorkerPool(
      2,
      GB,
      RSS_CEILING,
      TASKS_CAP,
      FAKE_WORKER,
      () => crashCount++,
    );
    await sleep(50);

    expect(pool.getEntries().length).toBe(2);

    pool.destroyAll();

    await sleep(100);

    expect(crashCount).toBe(0);
    expect(pool.getEntries().length).toBe(0);
    pool = null;
  });
});
