import { Logger } from '@nestjs/common';
import { Worker } from 'worker_threads';
import { appendIsolatedExecutorRuntimeLog } from './executor-runtime-log';

export function encodeMainThreadToIsolate(value: unknown): string {
  if (value === undefined) {
    return JSON.stringify({ __e: 'u' });
  }
  try {
    return JSON.stringify(
      { __e: 'v', d: value },
      (_k, v) => (typeof v === 'bigint' ? v.toString() : v),
    );
  } catch {
    return JSON.stringify({
      __e: 'v',
      d: { __serializationError: true, message: 'Result is not JSON-serializable' },
    });
  }
}

export interface WorkerExecutePayload {
  code: string;
  pkgSources: Array<{ name: string; safeName: string; sourceCode: string }>;
  snapshot: Record<string, any>;
  timeoutMs: number;
  memoryLimitMb: number;
  ctx: any;
}

interface PendingTask {
  resolve: (value: any) => void;
  reject: (error: any) => void;
  timer: ReturnType<typeof setTimeout> | null;
  ctx: any;
}

interface PooledWorker {
  worker: Worker;
  busy: boolean;
  pendingTaskId: string | null;
}

interface QueuedTask {
  id: string;
  payload: WorkerExecutePayload;
  resolve: (value: any) => void;
  reject: (error: any) => void;
}

let taskCounter = 0;

export class WorkerPool {
  private readonly logger = new Logger(WorkerPool.name);
  private workers: PooledWorker[] = [];
  private queue: QueuedTask[] = [];
  private pendingTasks = new Map<string, PendingTask>();
  private shuttingDown = false;

  constructor(
    private readonly workerScript: string,
    private readonly size: number,
  ) {
    this.spawnWorkers(this.size);
  }

  private spawnWorkers(count: number) {
    for (let i = 0; i < count; i++) {
      this.spawnWorker();
    }
  }

  private spawnWorker(): PooledWorker {
    const worker = new Worker(this.workerScript);
    const pooled: PooledWorker = { worker, busy: false, pendingTaskId: null };

    worker.on('message', (msg) => this.handleWorkerMessage(pooled, msg));
    worker.on('error', (err) => this.handleWorkerError(pooled, err));
    worker.on('exit', (code) => this.handleWorkerExit(pooled, code));

    this.workers.push(pooled);
    return pooled;
  }

  private handleWorkerMessage(pooled: PooledWorker, msg: any) {
    switch (msg.type) {
      case 'result':
        this.onResult(pooled, msg);
        break;
      case 'repoCall':
        this.onRepoCall(pooled, msg);
        break;
      case 'helpersCall':
        this.onHelpersCall(pooled, msg);
        break;
      case 'socketCall':
        this.onSocketCall(pooled, msg);
        break;
      case 'cacheCall':
        this.onCacheCall(pooled, msg);
        break;
    }
  }

  private onResult(pooled: PooledWorker, msg: any) {
    const task = this.pendingTasks.get(msg.id);
    if (!task) return;

    if (task.timer) clearTimeout(task.timer);
    this.pendingTasks.delete(msg.id);
    this.releaseWorker(pooled);

    if (msg.success) {
      appendIsolatedExecutorRuntimeLog({ event: 'task_done', id: msg.id, ok: true });
      task.resolve({
        value: msg.value,
        valueAbsent: msg.valueAbsent === true,
        ctxChanges: msg.ctxChanges,
      });
    } else {
      appendIsolatedExecutorRuntimeLog({
        event: 'task_done',
        id: msg.id,
        ok: false,
        message: msg.error?.message,
        code: msg.error?.code,
      });
      const err: any = new Error(msg.error?.message || 'Handler execution failed');
      err.statusCode = msg.error?.statusCode;
      err.code = msg.error?.code;
      if (msg.error?.stack) err.stack = msg.error.stack;
      task.reject(err);
    }
  }

  private async onRepoCall(pooled: PooledWorker, msg: any) {
    const task = this.pendingTasks.get(msg.id);
    if (!task) return;

    try {
      const args = JSON.parse(msg.argsJson);
      const repo = task.ctx?.$repos?.[msg.table];
      if (!repo || typeof repo[msg.method] !== 'function') {
        throw new Error(`Repo method not found: ${msg.table}.${msg.method}`);
      }
      const result = await repo[msg.method](...args);
      pooled.worker.postMessage({ type: 'callResult', callId: msg.callId, result: encodeMainThreadToIsolate(result) });
    } catch (error) {
      pooled.worker.postMessage({ type: 'callError', callId: msg.callId, error: error.message });
    }
  }

  private async onHelpersCall(pooled: PooledWorker, msg: any) {
    const task = this.pendingTasks.get(msg.id);
    if (!task) return;

    try {
      const args = JSON.parse(msg.argsJson);
      const parts = msg.name.split('.');
      let fn: any = task.ctx?.$helpers;
      for (const key of parts) {
        fn = fn?.[key];
      }
      if (typeof fn !== 'function') {
        throw new Error(`Helper not found: ${msg.name}`);
      }
      const result = await fn(...args);
      pooled.worker.postMessage({ type: 'callResult', callId: msg.callId, result: encodeMainThreadToIsolate(result) });
    } catch (error) {
      pooled.worker.postMessage({ type: 'callError', callId: msg.callId, error: error.message });
    }
  }

  private onSocketCall(pooled: PooledWorker, msg: any) {
    const task = this.pendingTasks.get(msg.id);
    if (!task) return;

    try {
      const args = JSON.parse(msg.argsJson);
      const fn = task.ctx?.$socket?.[msg.method];
      if (typeof fn === 'function') fn(...args);
    } catch {}
  }

  private async onCacheCall(pooled: PooledWorker, msg: any) {
    const task = this.pendingTasks.get(msg.id);
    if (!task) return;

    try {
      const args = JSON.parse(msg.argsJson);
      const fn = task.ctx?.$cache?.[msg.method];
      if (typeof fn !== 'function') throw new Error(`Cache method not found: ${msg.method}`);
      const result = await fn(...args);
      pooled.worker.postMessage({ type: 'callResult', callId: msg.callId, result: encodeMainThreadToIsolate(result) });
    } catch (error) {
      pooled.worker.postMessage({ type: 'callError', callId: msg.callId, error: error.message });
    }
  }

  private handleWorkerError(pooled: PooledWorker, err: Error) {
    this.logger.error(`Worker error: ${err.message}`);
    appendIsolatedExecutorRuntimeLog({ event: 'worker_error', message: err.message });
    this.failPendingTask(pooled, err);
    this.replaceWorker(pooled);
  }

  private handleWorkerExit(pooled: PooledWorker, code: number) {
    if (this.shuttingDown) return;
    if (code !== 0) {
      this.logger.warn(`Worker exited with code ${code}, replacing`);
      appendIsolatedExecutorRuntimeLog({ event: 'worker_exit', code });
      this.failPendingTask(pooled, new Error(`Worker exited with code ${code}`));
      this.replaceWorker(pooled);
    }
  }

  private failPendingTask(pooled: PooledWorker, err: Error) {
    if (!pooled.pendingTaskId) return;
    const task = this.pendingTasks.get(pooled.pendingTaskId);
    if (task) {
      if (task.timer) clearTimeout(task.timer);
      this.pendingTasks.delete(pooled.pendingTaskId);
      task.reject(err);
    }
    pooled.pendingTaskId = null;
    pooled.busy = false;
  }

  private replaceWorker(pooled: PooledWorker) {
    const idx = this.workers.indexOf(pooled);
    if (idx !== -1) this.workers.splice(idx, 1);
    if (!this.shuttingDown) {
      this.spawnWorker();
      this.drainQueue();
    }
  }

  private releaseWorker(pooled: PooledWorker) {
    pooled.busy = false;
    pooled.pendingTaskId = null;
    this.drainQueue();
  }

  private drainQueue() {
    while (this.queue.length > 0) {
      const idle = this.workers.find((w) => !w.busy);
      if (!idle) break;
      const task = this.queue.shift()!;
      appendIsolatedExecutorRuntimeLog({ event: 'queue_drain', taskId: task.id, remaining: this.queue.length });
      this.assignToWorker(idle, task.id, task.payload, task.resolve, task.reject);
    }
  }

  private assignToWorker(
    pooled: PooledWorker,
    id: string,
    payload: WorkerExecutePayload,
    resolve: (v: any) => void,
    reject: (e: any) => void,
  ) {
    appendIsolatedExecutorRuntimeLog({ event: 'task_assign', id, timeoutMs: payload.timeoutMs });
    pooled.busy = true;
    pooled.pendingTaskId = id;

    const workerTimeoutMs = payload.timeoutMs + 5000;
    const timer = setTimeout(() => {
      if (!this.pendingTasks.has(id)) return;
      this.pendingTasks.delete(id);
      this.logger.warn(`Task ${id} timed out (${payload.timeoutMs}ms), terminating worker`);
      appendIsolatedExecutorRuntimeLog({ event: 'task_timeout', id, timeoutMs: payload.timeoutMs });
      pooled.worker.terminate();
      const err: any = new Error(`Script execution timed out after ${payload.timeoutMs}ms`);
      err.code = 'ERR_SCRIPT_EXECUTION_TIMEOUT';
      err.isTimeout = true;
      reject(err);
    }, workerTimeoutMs);

    this.pendingTasks.set(id, { resolve, reject, timer, ctx: payload.ctx });

    pooled.worker.postMessage({
      type: 'execute',
      id,
      code: payload.code,
      pkgSources: payload.pkgSources,
      snapshot: payload.snapshot,
      timeoutMs: payload.timeoutMs,
      memoryLimitMb: payload.memoryLimitMb,
    });
  }

  execute(payload: WorkerExecutePayload): Promise<any> {
    return new Promise((resolve, reject) => {
      if (this.shuttingDown) {
        return reject(new Error('Worker pool is shutting down'));
      }

      const id = `task_${++taskCounter}`;
      const idle = this.workers.find((w) => !w.busy);

      if (idle) {
        this.assignToWorker(idle, id, payload, resolve, reject);
      } else {
        appendIsolatedExecutorRuntimeLog({ event: 'task_queue', id, depth: this.queue.length + 1 });
        this.queue.push({ id, payload, resolve, reject });
      }
    });
  }

  get activeCount(): number {
    return this.workers.filter((w) => w.busy).length;
  }

  get idleCount(): number {
    return this.workers.filter((w) => !w.busy).length;
  }

  get queuedCount(): number {
    return this.queue.length;
  }

  async destroy(): Promise<void> {
    this.shuttingDown = true;
    const qErr = new Error('Worker pool is shutting down');
    for (const t of this.queue) {
      t.reject(qErr);
    }
    this.queue = [];
    for (const [, pending] of this.pendingTasks) {
      if (pending.timer) clearTimeout(pending.timer);
      pending.reject(qErr);
    }
    this.pendingTasks.clear();
    appendIsolatedExecutorRuntimeLog({ event: 'pool_destroy' });
    await Promise.allSettled(this.workers.map((w) => w.worker.terminate()));
    this.workers = [];
  }
}
