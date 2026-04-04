import { Injectable, Logger, OnModuleDestroy } from '@nestjs/common';
import * as path from 'path';
import { Worker } from 'worker_threads';
import { TDynamicContext } from '../../../shared/types';
import { PackageCacheService } from '../../cache/services/package-cache.service';
import { PackageCdnLoaderService } from '../../cache/services/package-cdn-loader.service';
import { ErrorHandler } from '../utils/error-handler';
import { ScriptTimeoutException } from '../../../core/exceptions/custom-exceptions';
import { appendIsolatedExecutorRuntimeLog } from '../utils/executor-runtime-log';
import {
  AsyncSemaphore,
  getEffectiveMemoryBytes,
  getHandlerIsolationTuning,
} from '../utils/handler-isolation-tuning.util';
import {
  WORKER_TUNE_INTERVAL_MS,
  WORKER_RSS_HIGH,
  WORKER_RSS_LOW,
  WORKER_CPU_HIGH,
  WORKER_CPU_LOW,
  WORKER_FLOOR,
  WORKER_HYSTERESIS_TICKS,
  WORKER_QUEUE_REJECT_MULTIPLIER,
} from '../../../shared/utils/auto-scaling.constants';

const WORKER_SCRIPT = path.join(__dirname, '../workers/handler.worker.js');

export interface CodeBlock {
  code: string;
  type: 'preHook' | 'handler' | 'postHook';
}

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

@Injectable()
export class IsolatedExecutorService implements OnModuleDestroy {
  private readonly logger = new Logger(IsolatedExecutorService.name);
  private readonly isolationTuning = getHandlerIsolationTuning();
  private readonly workerSemaphore = new AsyncSemaphore(
    this.isolationTuning.maxConcurrentWorkers,
  );
  private readonly effectiveMemory = getEffectiveMemoryBytes();
  private readonly ceiling = this.isolationTuning.maxConcurrentWorkers;
  private readonly queueRejectThreshold =
    this.ceiling * WORKER_QUEUE_REJECT_MULTIPLIER;
  private prevCpuUsage = process.cpuUsage();
  private prevCpuTime = process.hrtime.bigint();
  private tuneTimer?: ReturnType<typeof setInterval>;
  private pressureTicks = 0;
  private recoveryTicks = 0;

  constructor(
    private readonly packageCacheService: PackageCacheService,
    private readonly cdnLoader: PackageCdnLoaderService,
  ) {
    this.tuneTimer = setInterval(() => this.autoTune(), WORKER_TUNE_INTERVAL_MS);
  }

  onModuleDestroy(): void {
    if (this.tuneTimer) {
      clearInterval(this.tuneTimer);
    }
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

    const current = this.workerSemaphore.getMax();
    const queueDepth = this.workerSemaphore.getWaitingCount();
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
        next = Math.min(this.ceiling, current + (hasDemand ? 2 : 1));
      }
    } else {
      this.pressureTicks = 0;
      this.recoveryTicks = 0;
    }

    if (next !== current) {
      this.workerSemaphore.resize(next);
      this.pressureTicks = 0;
      this.recoveryTicks = 0;
      this.logger.log(
        `Worker limit adjusted: ${current} -> ${next} (rss=${Math.round(rssRatio * 100)}% cpu=${Math.round(cpuRatio * 100)}% queue=${queueDepth})`,
      );
    }
  }

  private createSnapshot(ctx: TDynamicContext): Record<string, unknown> {
    const cloneJson = (v: unknown): unknown => {
      if (v === undefined) return undefined;
      try {
        return JSON.parse(
          JSON.stringify(v, (_k, val) => (typeof val === 'bigint' ? String(val) : val)),
        );
      } catch {
        return {};
      }
    };

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
      snapshot.$flow = cloneJson(flow);
    }
    return snapshot;
  }

  private mergeCtxChanges(ctx: TDynamicContext, changes: Record<string, any>): void {
    if (!changes) return;
    if (changes.$body !== undefined) ctx.$body = changes.$body;
    if (changes.$query !== undefined) ctx.$query = changes.$query;
    if (changes.$params !== undefined) ctx.$params = changes.$params;
    if (changes.$data !== undefined) ctx.$data = changes.$data;
    if (changes.$share !== undefined) ctx.$share = changes.$share;
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
      if (this.workerSemaphore.getWaitingCount() >= this.queueRejectThreshold) {
        const err: any = new Error(
          'Server overloaded: handler worker queue is full. Try again later.',
        );
        err.statusCode = 503;
        throw err;
      }
      await this.workerSemaphore.acquire();
      try {
        return await this.runWorkerThread(messageType, payload, ctx, timeoutMs);
      } finally {
        this.workerSemaphore.release();
      }
    })();
  }

  private runWorkerThread(
    messageType: string,
    payload: Record<string, any>,
    ctx: any,
    timeoutMs: number,
  ): Promise<any> {
    return new Promise((resolve, reject) => {
      const worker = new Worker(WORKER_SCRIPT);
      const id = `task_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
      let settled = false;

      const workerTimeoutMs = timeoutMs + 5000;
      const timer = setTimeout(() => {
        if (settled) return;
        settled = true;
        appendIsolatedExecutorRuntimeLog({ event: 'task_timeout', id, timeoutMs });
        worker.terminate();
        const err: any = new Error(`Script execution timed out after ${timeoutMs}ms`);
        err.code = 'ERR_SCRIPT_EXECUTION_TIMEOUT';
        err.isTimeout = true;
        reject(err);
      }, workerTimeoutMs);

      const cleanup = () => {
        clearTimeout(timer);
        worker.terminate();
      };

      worker.on('message', (msg) => {
        if (msg.type === 'result') {
          if (settled) return;
          settled = true;
          cleanup();

          if (msg.success) {
            appendIsolatedExecutorRuntimeLog({ event: 'task_done', id, ok: true });
            const resolved: any = {
              value: msg.value,
              valueAbsent: msg.valueAbsent === true,
              ctxChanges: msg.ctxChanges,
            };
            if (msg.shortCircuit) resolved.shortCircuit = true;
            resolve(resolved);
          } else {
            appendIsolatedExecutorRuntimeLog({ event: 'task_done', id, ok: false, message: msg.error?.message });
            const err: any = new Error(msg.error?.message || 'Handler execution failed');
            err.statusCode = msg.error?.statusCode;
            err.code = msg.error?.code;
            if (msg.error?.stack) err.stack = msg.error.stack;
            reject(err);
          }
        } else if (msg.type === 'repoCall') {
          this.handleRepoCall(worker, msg, ctx);
        } else if (msg.type === 'helpersCall') {
          this.handleHelpersCall(worker, msg, ctx);
        } else if (msg.type === 'socketCall') {
          this.handleSocketCall(msg, ctx);
        } else if (msg.type === 'cacheCall') {
          this.handleCacheCall(worker, msg, ctx);
        } else if (msg.type === 'dispatchCall') {
          this.handleDispatchCall(worker, msg, ctx);
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
        reject(new Error(`Worker exited unexpectedly with code ${code}`));
      });

      worker.postMessage({ type: messageType, id, ...payload });
    });
  }

  private async handleRepoCall(worker: Worker, msg: any, ctx: any) {
    try {
      const args = JSON.parse(msg.argsJson);
      const repo = ctx?.$repos?.[msg.table];
      if (!repo || typeof repo[msg.method] !== 'function') {
        throw new Error(`Repo method not found: ${msg.table}.${msg.method}`);
      }
      const result = await repo[msg.method](...args);
      worker.postMessage({ type: 'callResult', callId: msg.callId, result: encodeMainThreadToIsolate(result) });
    } catch (error) {
      worker.postMessage({ type: 'callError', callId: msg.callId, error: (error as Error).message });
    }
  }

  private async handleHelpersCall(worker: Worker, msg: any, ctx: any) {
    try {
      const args = JSON.parse(msg.argsJson);
      const parts = msg.name.split('.');
      let fn: any = ctx?.$helpers;
      for (const key of parts) {
        fn = fn?.[key];
      }
      if (typeof fn !== 'function') {
        throw new Error(`Helper not found: ${msg.name}`);
      }
      const result = await fn(...args);
      worker.postMessage({ type: 'callResult', callId: msg.callId, result: encodeMainThreadToIsolate(result) });
    } catch (error) {
      worker.postMessage({ type: 'callError', callId: msg.callId, error: (error as Error).message });
    }
  }

  private handleSocketCall(msg: any, ctx: any) {
    try {
      const args = JSON.parse(msg.argsJson);
      const fn = ctx?.$socket?.[msg.method];
      if (typeof fn === 'function') fn(...args);
    } catch {}
  }

  private async handleCacheCall(worker: Worker, msg: any, ctx: any) {
    try {
      const args = JSON.parse(msg.argsJson);
      const fn = ctx?.$cache?.[msg.method];
      if (typeof fn !== 'function') throw new Error(`Cache method not found: ${msg.method}`);
      const result = await fn(...args);
      worker.postMessage({ type: 'callResult', callId: msg.callId, result: encodeMainThreadToIsolate(result) });
    } catch (error) {
      worker.postMessage({ type: 'callError', callId: msg.callId, error: (error as Error).message });
    }
  }

  private async handleDispatchCall(worker: Worker, msg: any, ctx: any) {
    try {
      const args = JSON.parse(msg.argsJson);
      const fn = ctx?.$dispatch?.[msg.method];
      if (typeof fn !== 'function') {
        throw new Error(`$dispatch.${msg.method} is not available`);
      }
      const result = await fn(...args);
      worker.postMessage({ type: 'callResult', callId: msg.callId, result: encodeMainThreadToIsolate(result) });
    } catch (error) {
      worker.postMessage({ type: 'callError', callId: msg.callId, error: (error as Error).message });
    }
  }

  async run(code: string, ctx: TDynamicContext, timeoutMs: number): Promise<any> {
    const safeTimeoutMs = Math.max(1, Math.trunc(Number(timeoutMs) || 30000));
    const packages = await this.packageCacheService.getPackages();
    const pkgSources = this.cdnLoader.getPackageSources(packages);
    const snapshot = this.createSnapshot(ctx);

    appendIsolatedExecutorRuntimeLog({ event: 'isolated_run_start', timeoutMs: safeTimeoutMs, codeLen: code?.length ?? 0 });

    let result: any;
    try {
      result = await this.spawnAndExecute('execute', {
        code,
        pkgSources,
        snapshot,
        timeoutMs: safeTimeoutMs,
        memoryLimitMb: this.isolationTuning.isolateMemoryLimitMb,
      }, ctx, safeTimeoutMs);
    } catch (error) {
      appendIsolatedExecutorRuntimeLog({ event: 'isolated_run_error', message: (error as Error)?.message, code: (error as any)?.code, isTimeout: !!(error as any)?.isTimeout });
      if (error.isTimeout || error.code === 'ERR_SCRIPT_EXECUTION_TIMEOUT') {
        throw new ScriptTimeoutException(safeTimeoutMs, code);
      }
      throw ErrorHandler.createException(undefined, error.statusCode || error.status, error.message || 'Unknown error', code, {});
    }

    this.mergeCtxChanges(ctx, result.ctxChanges || {});
    appendIsolatedExecutorRuntimeLog({ event: 'isolated_run_ok' });
    return result.valueAbsent ? undefined : result.value;
  }

  async runBatch(codeBlocks: CodeBlock[], ctx: TDynamicContext, timeoutMs: number): Promise<any> {
    const safeTimeoutMs = Math.max(1, Math.trunc(Number(timeoutMs) || 30000));
    const packages = await this.packageCacheService.getPackages();
    const pkgSources = this.cdnLoader.getPackageSources(packages);
    const snapshot = this.createSnapshot(ctx);

    appendIsolatedExecutorRuntimeLog({ event: 'isolated_batch_start', timeoutMs: safeTimeoutMs, blocks: codeBlocks.length });

    let result: any;
    try {
      result = await this.spawnAndExecute('executeBatch', {
        codeBlocks,
        pkgSources,
        snapshot,
        timeoutMs: safeTimeoutMs,
        memoryLimitMb: this.isolationTuning.isolateMemoryLimitMb,
      }, ctx, safeTimeoutMs);
    } catch (error) {
      appendIsolatedExecutorRuntimeLog({ event: 'isolated_batch_error', message: (error as Error)?.message, code: (error as any)?.code, isTimeout: !!(error as any)?.isTimeout });
      if (error.isTimeout || error.code === 'ERR_SCRIPT_EXECUTION_TIMEOUT') {
        throw new ScriptTimeoutException(safeTimeoutMs, '(batch execution)');
      }
      throw ErrorHandler.createException(undefined, error.statusCode || error.status, error.message || 'Unknown error', '(batch execution)', {});
    }

    this.mergeCtxChanges(ctx, result.ctxChanges || {});
    appendIsolatedExecutorRuntimeLog({ event: 'isolated_batch_ok' });

    return {
      value: result.valueAbsent ? undefined : result.value,
      shortCircuit: result.shortCircuit === true,
    };
  }
}
