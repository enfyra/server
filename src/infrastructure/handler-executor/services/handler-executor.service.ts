import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { TDynamicContext } from '../../../shared/types';
import { PackageCacheService } from '../../cache/services/package-cache.service';
import { ChildProcessManager } from '../utils/child-process-manager';
import { wrapCtx } from '../utils/wrap-ctx';
import { ExecutorPoolService } from './executor-pool.service';

@Injectable()
export class HandlerExecutorService {
  constructor(
    private executorPoolService: ExecutorPoolService,
    private packageCacheService: PackageCacheService,
    private configService: ConfigService,
  ) {}

  async run(
    code: string,
    ctx: TDynamicContext,
    timeoutMs = this.configService.get<number>('DEFAULT_HANDLER_TIMEOUT', 30000),
  ): Promise<any> {
    const packages = await this.packageCacheService.getPackages();
    const pool = this.executorPoolService.getPool();
    const healthService = this.executorPoolService.getHealthService();

    const child = await pool.acquire();
    const startTime = Date.now();
    const isDone = { value: false };

    this.executorPoolService.checkAndScale().catch(() => {});

    return new Promise((resolve, reject) => {
      try {
        healthService.recordExecutionStart(child);

        const timeout = ChildProcessManager.setupTimeout(
          child,
          timeoutMs,
          code,
          isDone,
          reject,
          pool,
        );

        ChildProcessManager.setupChildProcessListeners(
          child,
          ctx,
          timeout,
          pool,
          isDone,
          async (data) => {
            const durationMs = Date.now() - startTime;
            healthService.recordExecutionEnd(child, durationMs, true);
            await this.releaseOrRecycle(child, pool, healthService);
            this.executorPoolService.checkAndScale().catch(() => {});
            resolve(data);
          },
          async (error) => {
            const durationMs = Date.now() - startTime;
            healthService.recordExecutionEnd(child, durationMs, false);
            healthService.recordError(child, error.message || 'Unknown error');
            await this.releaseOrRecycle(child, pool, healthService);
            this.executorPoolService.checkAndScale().catch(() => {});
            reject(error);
          },
          code,
        );

        ChildProcessManager.sendExecuteMessage(child, wrapCtx(ctx), code, packages);
      } catch (error) {
        healthService.recordError(child, error.message || 'Unknown error');
        pool.release(child).catch(() => {});
        reject(error);
      }
    });
  }

  private async releaseOrRecycle(
    child: any,
    pool: any,
    healthService: any,
  ): Promise<void> {
    if (!child.connected || child.killed) {
      await pool.destroy(child).catch(() => {});
      return;
    }

    const { shouldRecycle, reasons } = healthService.checkAndDecide(child);

    if (shouldRecycle) {
      const metadata = healthService.getAllMetadata().get(child);
      healthService.logRecycle(child.pid, reasons, metadata);
      await pool.destroy(child).catch(() => {});
    } else {
      await pool.release(child).catch(() => {});
    }
  }
}