import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Worker } from 'worker_threads';
import { randomUUID } from 'crypto';
import { TDynamicContext } from '../../../shared/types';
import { PackageCacheService } from '../../cache/services/package-cache.service';
import { ExecutorPoolService } from './executor-pool.service';
import { WorkerManager } from '../utils/worker-manager';
import { wrapCtx } from '../utils/wrap-ctx';

@Injectable()
export class HandlerExecutorService {
  constructor(
    private packageCacheService: PackageCacheService,
    private configService: ConfigService,
    private executorPoolService: ExecutorPoolService,
  ) {}

  async run(
    code: string,
    ctx: TDynamicContext,
    timeoutMs = this.configService.get<number>('DEFAULT_HANDLER_TIMEOUT', 30000),
  ): Promise<any> {
    const packages = await this.packageCacheService.getPackages();
    const worker = await this.executorPoolService.acquire();

    const returnWorker = (w: Worker) => this.executorPoolService.returnToPool(w);
    const terminateWorker = (w: Worker) => this.executorPoolService.terminateWorker(w);
    const isDone = { value: false };

    return new Promise((resolve, reject) => {
      const timeout = WorkerManager.setupTimeout(worker, timeoutMs, code, isDone, reject, terminateWorker);
      WorkerManager.setupListeners(worker, ctx, timeout, isDone, resolve, reject, code, returnWorker, terminateWorker);
      WorkerManager.sendExecute(worker, wrapCtx(ctx), code, packages);
    });
  }
}
