// @nestjs packages
import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';

// Internal imports
import { TDynamicContext } from '../../../shared/interfaces/dynamic-context.interface';
import { PackageCacheService } from '../../cache/services/package-cache.service';

// Relative imports
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
    timeoutMs = this.configService.get<number>('DEFAULT_HANDLER_TIMEOUT', 5000),
  ): Promise<any> {
    const startTime = Date.now();
    console.log(`[SCRIPT-EXEC] Starting script execution | timeout: ${timeoutMs}ms`);

    // Get packages for runner
    const packages = await this.packageCacheService.getPackages();
    console.log(`[SCRIPT-EXEC] Packages loaded: ${packages.length} packages | elapsed: ${Date.now() - startTime}ms`);

    const pool = this.executorPoolService.getPool();
    const isDone = { value: false };

    return new Promise(async (resolve, reject) => {
      console.log(`[SCRIPT-EXEC] Acquiring child process | elapsed: ${Date.now() - startTime}ms`);
      const child = await pool.acquire();
      console.log(`[SCRIPT-EXEC] Child process acquired | elapsed: ${Date.now() - startTime}ms`);

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
        startTime,
      );

      console.log(`[SCRIPT-EXEC] Sending execute message | elapsed: ${Date.now() - startTime}ms`);
      ChildProcessManager.sendExecuteMessage(child, wrapCtx(ctx), code, packages);
    });
  }
}
