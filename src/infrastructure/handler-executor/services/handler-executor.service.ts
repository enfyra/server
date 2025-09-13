// @nestjs packages
import { Injectable } from '@nestjs/common';

// Internal imports
import { TDynamicContext } from '../../../shared/interfaces/dynamic-context.interface';
import { PackageCacheService } from '../../redis/services/package-cache.service';

// Relative imports
import { ChildProcessManager } from '../utils/child-process-manager';
import { wrapCtx } from '../utils/wrap-ctx';
import { ExecutorPoolService } from './executor-pool.service';

@Injectable()
export class HandlerExecutorService {

  constructor(
    private executorPoolService: ExecutorPoolService,
    private packageCacheService: PackageCacheService,
  ) {}

  async run(
    code: string,
    ctx: TDynamicContext,
    timeoutMs = 5000,
  ): Promise<any> {
    // Get packages for runner
    const packages = await this.packageCacheService.getPackagesWithSWR();
    const pool = this.executorPoolService.getPool();
    const isDone = { value: false };
    return new Promise(async (resolve, reject) => {
      const child = await pool.acquire();
      const timeout = ChildProcessManager.setupTimeout(
        child,
        timeoutMs,
        code,
        isDone,
        reject,
      );

      ChildProcessManager.setupChildProcessListeners(
        child,
        ctx,
        timeout,
        pool,
        isDone,
        resolve,
        reject,
        code,
      );

      ChildProcessManager.sendExecuteMessage(child, wrapCtx(ctx), code, packages);
    });
  }
}
