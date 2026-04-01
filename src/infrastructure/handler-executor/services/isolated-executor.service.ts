import { Injectable, Logger, OnApplicationBootstrap, OnApplicationShutdown } from '@nestjs/common';
import * as path from 'path';
import { TDynamicContext } from '../../../shared/types';
import { PackageCacheService } from '../../cache/services/package-cache.service';
import { PackageCdnLoaderService } from '../../cache/services/package-cdn-loader.service';
import { ErrorHandler } from '../utils/error-handler';
import { ScriptTimeoutException } from '../../../core/exceptions/custom-exceptions';
import { WorkerPool } from '../utils/worker-pool';
import { appendIsolatedExecutorRuntimeLog } from '../utils/executor-runtime-log';

const WORKER_SCRIPT = path.join(__dirname, '../workers/handler.worker.js');

@Injectable()
export class IsolatedExecutorService implements OnApplicationBootstrap, OnApplicationShutdown {
  private readonly logger = new Logger(IsolatedExecutorService.name);
  private pool: WorkerPool;

  constructor(
    private readonly packageCacheService: PackageCacheService,
    private readonly cdnLoader: PackageCdnLoaderService,
  ) {}

  onApplicationBootstrap() {
    const size = parseInt(process.env.HANDLER_WORKER_POOL_SIZE || '4', 10);
    this.pool = new WorkerPool(WORKER_SCRIPT, size);
    this.logger.log(`Worker pool initialized: ${size} workers`);
  }

  async onApplicationShutdown() {
    await this.pool?.destroy();
  }

  async run(code: string, ctx: TDynamicContext, timeoutMs: number): Promise<any> {
    const safeTimeoutMs = Math.max(1, Math.trunc(Number(timeoutMs) || 30000));
    const packages = await this.packageCacheService.getPackages();
    const pkgSources = this.cdnLoader.getPackageSources(packages);

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
      $statusCode: (ctx as any).$statusCode,
      $api: { request: ctx.$api?.request },
      $uploadedFile: ctx.$uploadedFile,
    };
    const flow = (ctx as any).$flow;
    if (flow !== undefined && flow !== null) {
      snapshot.$flow = cloneJson(flow);
    }

    appendIsolatedExecutorRuntimeLog({
      event: 'isolated_run_start',
      timeoutMs: safeTimeoutMs,
      codeLen: code?.length ?? 0,
    });
    let result: any;
    try {
      result = await this.pool.execute({
        code,
        pkgSources,
        snapshot,
        timeoutMs: safeTimeoutMs,
        memoryLimitMb: parseInt(process.env.HANDLER_MEMORY_LIMIT_MB || '128', 10),
        ctx,
      });
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

      if (error.constructor?.name?.includes('Exception')) throw error;

      throw ErrorHandler.createException(
        undefined,
        error.statusCode || error.status,
        error.message || 'Unknown error',
        code,
        {},
      );
    }

    const changes = result.ctxChanges || {};
    if (changes.$body !== undefined) ctx.$body = changes.$body;
    if (changes.$query !== undefined) ctx.$query = changes.$query;
    if (changes.$params !== undefined) ctx.$params = changes.$params;
    if (changes.$data !== undefined) ctx.$data = changes.$data;
    if (changes.$statusCode !== undefined) (ctx as any).$statusCode = changes.$statusCode;
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

    appendIsolatedExecutorRuntimeLog({ event: 'isolated_run_ok' });
    return result.valueAbsent ? undefined : result.value;
  }
}
