import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Worker } from 'worker_threads';
import * as path from 'path';

@Injectable()
export class ExecutorPoolService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(ExecutorPoolService.name);
  private readonly idleWorkers: Worker[] = [];
  private destroyed = false;

  private readonly workerPath = path.resolve(__dirname, '..', 'worker.js');
  private readonly maxOldSpaceMb = parseInt(process.env.HANDLER_EXECUTOR_MAX_MEMORY || '512', 10);
  private readonly poolMin = parseInt(process.env.HANDLER_EXECUTOR_POOL_MIN || '2', 10);
  private readonly poolMax = parseInt(process.env.HANDLER_EXECUTOR_POOL_MAX || '10', 10);

  onModuleInit() {
    for (let i = 0; i < this.poolMin; i++) {
      this.replenish();
    }
    this.logger.log(`Executor pool warming (min=${this.poolMin})`);
  }

  private spawnWorker(): Promise<Worker> {
    return new Promise((resolve, reject) => {
      const worker = new Worker(this.workerPath, {
        resourceLimits: { maxOldGenerationSizeMb: this.maxOldSpaceMb },
        stderr: true,
      });
      worker.once('online', () => resolve(worker));
      worker.once('error', reject);
    });
  }

  private replenish(): void {
    if (this.destroyed) return;
    this.spawnWorker()
      .then((w) => {
        if (this.destroyed) { w.terminate(); return; }
        this.idleWorkers.push(w);
      })
      .catch((err) => this.logger.warn(`Worker spawn failed: ${err?.message}`));
  }

  async acquire(): Promise<Worker> {
    if (this.idleWorkers.length > 0) {
      const worker = this.idleWorkers.pop()!;
      if (this.idleWorkers.length < this.poolMin) {
        this.replenish();
      }
      return worker;
    }
    return this.spawnWorker();
  }

  returnToPool(worker: Worker): void {
    if (this.destroyed || this.idleWorkers.length >= this.poolMax) {
      worker.terminate().catch(() => {});
      return;
    }
    worker.once('exit', () => {
      const idx = this.idleWorkers.indexOf(worker);
      if (idx !== -1) this.idleWorkers.splice(idx, 1);
    });
    this.idleWorkers.push(worker);
  }

  terminateWorker(worker: Worker): void {
    worker.terminate().catch(() => {});
    if (!this.destroyed && this.idleWorkers.length < this.poolMin) {
      this.replenish();
    }
  }

  async onModuleDestroy() {
    this.destroyed = true;
    const workers = this.idleWorkers.splice(0);
    await Promise.allSettled(workers.map((w) => w.terminate()));
  }
}
