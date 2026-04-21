import { Logger } from '../../../shared/logger';
import { AsyncLocalStorage } from 'async_hooks';
import * as path from 'path';
import { Worker } from 'worker_threads';
import { TDynamicContext } from '../../../shared/types';
import { PackageCacheService } from '../../cache/services/package-cache.service';
import { PackageCdnLoaderService } from '../../cache/services/package-cdn-loader.service';
import { ErrorHandler } from '../utils/error-handler';
import { ScriptTimeoutException } from '../../../core/exceptions/custom-exceptions';
import { appendIsolatedExecutorRuntimeLog } from '../utils/executor-runtime-log';
import {
  getEffectiveMemoryBytes,
  getEngineTuning,
} from '../utils/engine-tuning.util';
import {
  WORKER_TUNE_INTERVAL_MS,
  WORKER_RSS_HIGH,
  WORKER_RSS_LOW,
  WORKER_CPU_HIGH,
  WORKER_CPU_LOW,
  WORKER_FLOOR,
  WORKER_HYSTERESIS_TICKS,
  WORKER_DISPATCH_RSS_CEILING,
  WORKER_HEAP_ROTATE_THRESHOLD,
  WORKER_DRAIN_TIMEOUT_MS,
} from '../../../shared/utils/auto-scaling.constants';

const WORKER_SCRIPT = path.join(__dirname, '../workers/executor.worker.js');

const ioAbortContext = new AsyncLocalStorage<AbortSignal>();

export function getIoAbortSignal(): AbortSignal | undefined {
  return ioAbortContext.getStore();
}

interface TaskReg {
  onResult: (msg: any) => void;
  onIoCall: (msg: any) => void;
  abortController: AbortController;
}

export interface PoolEntry {
  worker: Worker;
  tasks: Map<string, TaskReg>;
  draining: boolean;
  spawnedAt: number;
  drainTimeout?: ReturnType<typeof setTimeout>;
}

export class WorkerPool {
  private readonly entries: PoolEntry[] = [];
  private readonly waiting: Array<(e: PoolEntry) => void> = [];
  private max: number;
  private readonly effectiveMemory: number;
  private readonly rssCeiling: number;
  private readonly tasksCap: number;
  private readonly drainTimeoutMs: number;
  private cachedMemoryOk = true;
  private lastMemoryCheckMs = 0;
  private readonly memorySampleIntervalMs = 200;

  constructor(
    poolSize: number,
    effectiveMemory: number,
    rssCeiling: number,
    tasksCap: number,
    private readonly scriptPath: string,
    private readonly onCrash?: () => void,
    private readonly onRotate?: (info: {
      reason: string;
      ageMs: number;
    }) => void,
    drainTimeoutMs: number = WORKER_DRAIN_TIMEOUT_MS,
  ) {
    this.max = poolSize;
    this.effectiveMemory = effectiveMemory;
    this.rssCeiling = rssCeiling;
    this.tasksCap = tasksCap;
    this.drainTimeoutMs = drainTimeoutMs;
    for (let i = 0; i < poolSize; i++) this.spawnEntry();
  }

  getEntries(): readonly PoolEntry[] {
    return this.entries;
  }

  private spawnEntry(): PoolEntry {
    const worker = new Worker(this.scriptPath);
    const entry: PoolEntry = {
      worker,
      tasks: new Map(),
      draining: false,
      spawnedAt: Date.now(),
    };

    worker.on('message', (msg: any) => {
      const reg = entry.tasks.get(msg.id);
      if (!reg) return;
      if (msg.type === 'result') {
        reg.onResult(msg);
        if (
          !entry.draining &&
          typeof msg.heapRatio === 'number' &&
          msg.heapRatio >= WORKER_HEAP_ROTATE_THRESHOLD
        ) {
          this.rotateEntry(
            entry,
            `heap=${Math.round(msg.heapRatio * 100)}%`,
          );
        }
      } else {
        reg.onIoCall(msg);
      }
    });

    worker.on('exit', () => {
      const wasDraining = entry.draining;
      if (entry.drainTimeout) {
        clearTimeout(entry.drainTimeout);
        entry.drainTimeout = undefined;
      }
      for (const [, reg] of entry.tasks) {
        reg.abortController.abort();
        reg.onResult({
          type: 'result',
          success: false,
          error: { message: 'Worker crashed' },
        });
      }
      entry.tasks.clear();
      const idx = this.entries.indexOf(entry);
      if (idx !== -1) this.entries.splice(idx, 1);
      if (!wasDraining) {
        if (this.onCrash) this.onCrash();
        if (this.entries.length < this.max) this.spawnEntry();
      }
      this.drainWaiting();
    });

    this.entries.push(entry);
    return entry;
  }

  private rotateEntry(entry: PoolEntry, reason: string): void {
    if (entry.draining) return;
    entry.draining = true;
    const ageMs = Date.now() - entry.spawnedAt;
    if (this.onRotate) this.onRotate({ reason, ageMs });
    this.spawnEntry();
    if (entry.tasks.size === 0) {
      entry.worker.terminate();
      return;
    }
    entry.drainTimeout = setTimeout(() => {
      entry.drainTimeout = undefined;
      entry.worker.terminate();
    }, this.drainTimeoutMs);
  }

  private isMemoryAvailable(): boolean {
    const now = Date.now();
    if (now - this.lastMemoryCheckMs >= this.memorySampleIntervalMs) {
      this.cachedMemoryOk =
        process.memoryUsage().rss / this.effectiveMemory < this.rssCeiling;
      this.lastMemoryCheckMs = now;
    }
    return this.cachedMemoryOk;
  }

  private findLeastBusy(): PoolEntry | null {
    let best: PoolEntry | null = null;
    for (const e of this.entries) {
      if (e.draining) continue;
      if (e.tasks.size < this.tasksCap) {
        if (!best || e.tasks.size < best.tasks.size) best = e;
      }
    }
    return best;
  }

  dispatch(): Promise<PoolEntry> {
    const best = this.findLeastBusy();
    if (best && (best.tasks.size === 0 || this.isMemoryAvailable())) {
      return Promise.resolve(best);
    }
    return new Promise((resolve) => this.waiting.push(resolve));
  }

  registerTask(entry: PoolEntry, taskId: string, reg: TaskReg): void {
    entry.tasks.set(taskId, reg);
  }

  unregisterTask(entry: PoolEntry, taskId: string): void {
    entry.tasks.delete(taskId);
    if (entry.draining && entry.tasks.size === 0) {
      if (entry.drainTimeout) {
        clearTimeout(entry.drainTimeout);
        entry.drainTimeout = undefined;
      }
      entry.worker.terminate();
    }
    this.drainWaiting();
  }

  private drainWaiting(): void {
    while (this.waiting.length > 0) {
      const best = this.findLeastBusy();
      if (best && (best.tasks.size === 0 || this.isMemoryAvailable())) {
        this.waiting.shift()!(best);
      } else {
        break;
      }
    }
  }

  terminateEntry(entry: PoolEntry): void {
    entry.worker.terminate();
  }

  resize(newMax: number): void {
    this.max = Math.max(1, newMax);
    while (this.entries.length < this.max) this.spawnEntry();
  }

  getMax(): number {
    return this.max;
  }
  getWaitingCount(): number {
    return this.waiting.length;
  }
  getTotalActiveTasks(): number {
    let n = 0;
    for (const e of this.entries) n += e.tasks.size;
    return n;
  }

  destroyAll(): void {
    for (const e of this.entries) e.worker.terminate();
    this.entries.length = 0;
    this.waiting.length = 0;
  }
}

export interface CodeBlock {
  code: string;
  type: 'preHook' | 'handler' | 'postHook';
}

export function encodeMainThreadToIsolate(value: unknown): string {
  if (value === undefined) {
    return JSON.stringify({ __e: 'u' });
  }
  try {
    return JSON.stringify({ __e: 'v', d: value }, (_k, v) =>
      typeof v === 'bigint' ? v.toString() : v,
    );
  } catch {
    return JSON.stringify({
      __e: 'v',
      d: {
        __serializationError: true,
        message: 'Result is not JSON-serializable',
      },
    });
  }
}

export class IsolatedExecutorService {
  private readonly logger = new Logger(IsolatedExecutorService.name);
  private readonly packageCacheService: PackageCacheService;
  private readonly packageCdnLoaderService: PackageCdnLoaderService;
  private readonly effectiveMemory = getEffectiveMemoryBytes();
  private readonly isolationTuning = getEngineTuning();
  private readonly pool = new WorkerPool(
    this.isolationTuning.maxConcurrentWorkers,
    this.effectiveMemory,
    WORKER_DISPATCH_RSS_CEILING,
    this.isolationTuning.tasksPerWorkerCap,
    WORKER_SCRIPT,
    () => this.logger.warn('Worker crashed, replacement spawned'),
    ({ reason, ageMs }) =>
      this.logger.log(
        `Worker rotation: reason=${reason} age=${Math.round(ageMs / 1000)}s`,
      ),
  );
  private readonly ceiling = this.isolationTuning.maxConcurrentWorkers;
  private prevCpuUsage = process.cpuUsage();
  private prevCpuTime = process.hrtime.bigint();
  private tuneTimer?: ReturnType<typeof setInterval>;
  private pressureTicks = 0;
  private recoveryTicks = 0;
  private taskCounter = 0;

  constructor(deps: {
    packageCacheService: PackageCacheService;
    packageCdnLoaderService: PackageCdnLoaderService;
  }) {
    this.packageCacheService = deps.packageCacheService;
    this.packageCdnLoaderService = deps.packageCdnLoaderService;
    this.logger.log(
      `Worker pool started: ${this.isolationTuning.maxConcurrentWorkers} workers, ${this.isolationTuning.isolateMemoryLimitMb}MB per isolate, ${this.isolationTuning.isolatePoolSize} isolates/worker, ${this.isolationTuning.tasksPerWorkerCap} tasks/worker cap`,
    );
    this.tuneTimer = setInterval(
      () => this.autoTune(),
      WORKER_TUNE_INTERVAL_MS,
    );
  }

  onDestroy(): void {
    if (this.tuneTimer) {
      clearInterval(this.tuneTimer);
    }
    this.pool.destroyAll();
  }

  private autoTune(): void {
    const rss = process.memoryUsage().rss;
    const rssRatio = rss / this.effectiveMemory;

    const now = process.hrtime.bigint();
    const cpu = process.cpuUsage(this.prevCpuUsage);
    const elapsedUs = Number(now - this.prevCpuTime) / 1000;
    const cpuRatio = elapsedUs > 0 ? (cpu.user + cpu.system) / elapsedUs : 0;
    this.prevCpuUsage = process.cpuUsage();
    this.prevCpuTime = now;

    const current = this.pool.getMax();
    const queueDepth = this.pool.getWaitingCount();
    const underPressure =
      rssRatio > WORKER_RSS_HIGH || cpuRatio > WORKER_CPU_HIGH;
    const resourcesOk = rssRatio < WORKER_RSS_LOW && cpuRatio < WORKER_CPU_LOW;
    const hasDemand = queueDepth > 0;

    let next = current;

    if (underPressure) {
      this.recoveryTicks = 0;
      this.pressureTicks++;
      if (this.pressureTicks >= WORKER_HYSTERESIS_TICKS) {
        next = Math.max(WORKER_FLOOR, current - 1);
      }
    } else if (resourcesOk && hasDemand) {
      this.pressureTicks = 0;
      this.recoveryTicks++;
      if (this.recoveryTicks >= WORKER_HYSTERESIS_TICKS) {
        next = Math.min(this.ceiling, current + 2);
      }
    } else {
      this.pressureTicks = 0;
      this.recoveryTicks = 0;
    }

    if (next !== current) {
      this.pool.resize(next);
      this.pressureTicks = 0;
      this.recoveryTicks = 0;
      this.logger.log(
        `Worker pool adjusted: ${current} -> ${next} (rss=${Math.round(rssRatio * 100)}% cpu=${Math.round(cpuRatio * 100)}% queue=${queueDepth})`,
      );
    }
  }

  private createSnapshot(ctx: TDynamicContext): Record<string, unknown> {
    const snapshot: Record<string, unknown> = {
      $body: ctx.$body,
      $query: ctx.$query,
      $params: ctx.$params,
      $user: ctx.$user,
      $share: ctx.$share,
      $data: ctx.$data,
      $api: { request: ctx.$api?.request },
      $uploadedFile: ctx.$uploadedFile,
    };
    const flow = (ctx as any).$flow;
    if (flow !== undefined && flow !== null) {
      try {
        snapshot.$flow = JSON.parse(
          JSON.stringify(flow, (_k, val) =>
            typeof val === 'bigint' ? String(val) : val,
          ),
        );
      } catch {
        snapshot.$flow = {};
      }
    }
    return snapshot;
  }

  private mergeCtxChanges(
    ctx: TDynamicContext,
    changes: Record<string, any>,
  ): void {
    if (!changes) return;
    if (changes.$body !== undefined) ctx.$body = changes.$body;
    if (changes.$query !== undefined) ctx.$query = changes.$query;
    if (changes.$params !== undefined) ctx.$params = changes.$params;
    if (changes.$data !== undefined) ctx.$data = changes.$data;
    if (changes.$error !== undefined) ctx.$error = changes.$error;
    if (changes.$statusCode !== undefined)
      ctx.$statusCode = changes.$statusCode;
    if (changes.$share !== undefined) ctx.$share = changes.$share;
    if (changes.$api !== undefined && ctx.$api) {
      if (changes.$api.error) ctx.$api.error = changes.$api.error;
      if (changes.$api.response) ctx.$api.response = changes.$api.response;
    }
    if (
      changes.$flow !== undefined &&
      changes.$flow !== null &&
      typeof changes.$flow === 'object' &&
      (ctx as any).$flow != null &&
      typeof (ctx as any).$flow === 'object'
    ) {
      Object.assign((ctx as any).$flow, changes.$flow);
    }
    delete (ctx as any).$pkgs;
  }

  private spawnAndExecute(
    messageType: string,
    payload: Record<string, any>,
    ctx: any,
    timeoutMs: number,
  ): Promise<any> {
    return (async () => {
      const entry = await this.pool.dispatch();
      const taskId = `t_${++this.taskCounter}`;

      return new Promise<any>((resolve, reject) => {
        let settled = false;
        const abortController = new AbortController();

        const timer = setTimeout(() => {
          if (settled) return;
          settled = true;
          abortController.abort();
          this.pool.unregisterTask(entry, taskId);
          appendIsolatedExecutorRuntimeLog({
            event: 'task_timeout',
            id: taskId,
            timeoutMs,
          });
          this.pool.terminateEntry(entry);
          const err: any = new Error(
            `Script execution timed out after ${timeoutMs}ms`,
          );
          err.code = 'ERR_SCRIPT_EXECUTION_TIMEOUT';
          err.isTimeout = true;
          reject(err);
        }, timeoutMs + 5000);

        this.pool.registerTask(entry, taskId, {
          abortController,
          onResult: (msg) => {
            if (settled) return;
            settled = true;
            clearTimeout(timer);
            this.pool.unregisterTask(entry, taskId);

            if (msg.success) {
              appendIsolatedExecutorRuntimeLog({
                event: 'task_done',
                id: taskId,
                ok: true,
              });
              const resolved: any = {
                value: msg.value,
                valueAbsent: msg.valueAbsent === true,
                ctxChanges: msg.ctxChanges,
              };
              if (msg.shortCircuit) resolved.shortCircuit = true;
              resolve(resolved);
            } else {
              appendIsolatedExecutorRuntimeLog({
                event: 'task_done',
                id: taskId,
                ok: false,
                message: msg.error?.message,
              });
              const err: any = new Error(
                msg.error?.message || 'Handler execution failed',
              );
              err.statusCode = msg.error?.statusCode;
              err.code = msg.error?.code;
              err.details = msg.error?.details;
              if (msg.error?.stack) err.stack = msg.error.stack;
              if (msg.ctxChanges) err.ctxChanges = msg.ctxChanges;
              reject(err);
            }
          },
          onIoCall: (msg) => {
            if (settled) return;
            const signal = abortController.signal;
            if (msg.type === 'repoCall') {
              this.handleIoCall(
                () => this.execRepoCall(msg, ctx),
                entry.worker,
                msg.callId,
                signal,
              );
            } else if (msg.type === 'helpersCall') {
              this.handleIoCall(
                () => this.execHelpersCall(msg, ctx),
                entry.worker,
                msg.callId,
                signal,
              );
            } else if (msg.type === 'socketCall') {
              this.handleIoCall(
                () => this.execSocketCall(msg, ctx),
                entry.worker,
                msg.callId,
                signal,
              );
            } else if (msg.type === 'cacheCall') {
              this.handleIoCall(
                () => this.execCacheCall(msg, ctx),
                entry.worker,
                msg.callId,
                signal,
              );
            } else if (msg.type === 'dispatchCall') {
              this.handleIoCall(
                () => this.execDispatchCall(msg, ctx),
                entry.worker,
                msg.callId,
                signal,
              );
            }
          },
        });

        entry.worker.postMessage({ type: messageType, id: taskId, ...payload });
      });
    })();
  }

  private async handleIoCall(
    fn: () => Promise<unknown>,
    worker: Worker,
    callId: string,
    signal?: AbortSignal,
  ): Promise<void> {
    if (signal?.aborted) return;
    try {
      const result = await (signal ? ioAbortContext.run(signal, fn) : fn());
      if (signal?.aborted) return;
      try {
        worker.postMessage({
          type: 'callResult',
          callId,
          result: encodeMainThreadToIsolate(result),
        });
      } catch {}
    } catch (error) {
      if (signal?.aborted) return;
      try {
        // Encode error metadata as JSON in message so it survives the
        // isolated-vm boundary (isolate drops custom Error properties).
        const payload = {
          __userThrow: true,
          message: (error as Error).message,
          statusCode: (error as any).statusCode,
          code: (error as any).errorCode ?? (error as any).code,
          details: (error as any).details,
          messages: (error as any).messages,
        };
        worker.postMessage({
          type: 'callError',
          callId,
          error: JSON.stringify(payload),
        });
      } catch {}
    }
  }

  private async execRepoCall(msg: any, ctx: any): Promise<unknown> {
    const args = JSON.parse(msg.argsJson);
    const repo = ctx?.$repos?.[msg.table];
    if (!repo || typeof repo[msg.method] !== 'function') {
      throw new Error(`Repo method not found: ${msg.table}.${msg.method}`);
    }
    return repo[msg.method](...args);
  }

  private async execHelpersCall(msg: any, ctx: any): Promise<unknown> {
    const args = JSON.parse(msg.argsJson);
    const parts = msg.name.split('.');
    let fn: any = ctx?.$helpers;
    for (const key of parts) {
      fn = fn?.[key];
    }
    if (typeof fn !== 'function') {
      throw new Error(`Helper not found: ${msg.name}`);
    }
    return fn(...args);
  }

  private async execSocketCall(msg: any, ctx: any): Promise<unknown> {
    const args = JSON.parse(msg.argsJson);
    const fn = ctx?.$socket?.[msg.method];
    if (typeof fn !== 'function') {
      throw new Error(`Socket method not found: ${msg.method}`);
    }
    return fn(...args);
  }

  private async execCacheCall(msg: any, ctx: any): Promise<unknown> {
    const args = JSON.parse(msg.argsJson);
    const fn = ctx?.$cache?.[msg.method];
    if (typeof fn !== 'function')
      throw new Error(`Cache method not found: ${msg.method}`);
    return fn(...args);
  }

  private async execDispatchCall(msg: any, ctx: any): Promise<unknown> {
    const args = JSON.parse(msg.argsJson);
    const fn = ctx?.$trigger;
    if (typeof fn !== 'function') {
      throw new Error(`$trigger is not available`);
    }
    return fn(...args);
  }

  async run(
    code: string,
    ctx: TDynamicContext,
    timeoutMs: number,
  ): Promise<any> {
    const safeTimeoutMs = Math.max(1, Math.trunc(Number(timeoutMs) || 30000));
    const packages = await this.packageCacheService.getPackages();
    const pkgSources = this.packageCdnLoaderService.getPackageSources(packages);
    const snapshot = this.createSnapshot(ctx);

    appendIsolatedExecutorRuntimeLog({
      event: 'isolated_run_start',
      timeoutMs: safeTimeoutMs,
      codeLen: code?.length ?? 0,
    });

    let result: any;
    try {
      result = await this.spawnAndExecute(
        'execute',
        {
          code,
          pkgSources,
          snapshot,
          timeoutMs: safeTimeoutMs,
          memoryLimitMb: this.isolationTuning.isolateMemoryLimitMb,
          isolatePoolSize: this.isolationTuning.isolatePoolSize,
        },
        ctx,
        safeTimeoutMs,
      );
    } catch (error) {
      appendIsolatedExecutorRuntimeLog({
        event: 'isolated_run_error',
        message: (error as Error)?.message,
        code: (error as any)?.code,
        isTimeout: !!(error as any)?.isTimeout,
      });
      if (error.isTimeout || error.code === 'ERR_SCRIPT_EXECUTION_TIMEOUT') {
        throw new ScriptTimeoutException(safeTimeoutMs, code);
      }
      throw ErrorHandler.createException(
        undefined,
        error.statusCode || error.status,
        error.message || 'Unknown error',
        code,
        error.details || {},
      );
    }

    this.mergeCtxChanges(ctx, result.ctxChanges || {});
    appendIsolatedExecutorRuntimeLog({ event: 'isolated_run_ok' });
    return result.valueAbsent ? undefined : result.value;
  }

  async runBatch(
    codeBlocks: CodeBlock[],
    ctx: TDynamicContext,
    timeoutMs: number,
  ): Promise<any> {
    const safeTimeoutMs = Math.max(1, Math.trunc(Number(timeoutMs) || 30000));
    const packages = await this.packageCacheService.getPackages();
    const pkgSources = this.packageCdnLoaderService.getPackageSources(packages);
    const snapshot = this.createSnapshot(ctx);

    appendIsolatedExecutorRuntimeLog({
      event: 'isolated_batch_start',
      timeoutMs: safeTimeoutMs,
      blocks: codeBlocks.length,
    });

    let result: any;
    try {
      result = await this.spawnAndExecute(
        'executeBatch',
        {
          codeBlocks,
          pkgSources,
          snapshot,
          timeoutMs: safeTimeoutMs,
          memoryLimitMb: this.isolationTuning.isolateMemoryLimitMb,
          isolatePoolSize: this.isolationTuning.isolatePoolSize,
        },
        ctx,
        safeTimeoutMs,
      );
    } catch (error) {
      appendIsolatedExecutorRuntimeLog({
        event: 'isolated_batch_error',
        message: (error as Error)?.message,
        code: (error as any)?.code,
        isTimeout: !!(error as any)?.isTimeout,
      });
      if ((error as any).ctxChanges) {
        this.mergeCtxChanges(ctx, (error as any).ctxChanges);
      }
      if (error.isTimeout || error.code === 'ERR_SCRIPT_EXECUTION_TIMEOUT') {
        throw new ScriptTimeoutException(safeTimeoutMs, '(batch execution)');
      }
      throw ErrorHandler.createException(
        undefined,
        error.statusCode || error.status,
        error.message || 'Unknown error',
        '(batch execution)',
        error.details || {},
      );
    }

    this.mergeCtxChanges(ctx, result.ctxChanges || {});
    appendIsolatedExecutorRuntimeLog({ event: 'isolated_batch_ok' });

    return {
      value: result.valueAbsent ? undefined : result.value,
      shortCircuit: result.shortCircuit === true,
    };
  }
}
