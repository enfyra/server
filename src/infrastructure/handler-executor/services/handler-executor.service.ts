// @nestjs packages
import { Injectable, Logger } from '@nestjs/common';

// Internal imports
import { TDynamicContext } from '../../../shared/interfaces/dynamic-context.interface';

// Relative imports
import { ChildProcessManager } from '../utils/child-process-manager';
import { ErrorHandler } from '../utils/error-handler';
import { wrapCtx } from '../utils/wrap-ctx';
import { ExecutorPoolService } from './executor-pool.service';

@Injectable()
export class HandlerExecutorService {
  private readonly logger = new Logger(HandlerExecutorService.name);

  constructor(private executorPoolService: ExecutorPoolService) {}

  async run(
    code: string,
    ctx: TDynamicContext,
    timeoutMs = 5000,
  ): Promise<any> {
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

      ChildProcessManager.sendExecuteMessage(child, wrapCtx(ctx), code);
    });
  }
}
