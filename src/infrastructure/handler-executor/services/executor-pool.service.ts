import { Injectable, Logger, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { fork, ChildProcess } from 'child_process';
import { createPool, Pool } from 'generic-pool';
import * as path from 'path';
import { ChildProcessHealthService } from './child-process-health.service';
import { ExecutorHealthLogger } from '../utils/executor-health.logger';
import { PoolAutoScaleService } from './pool-auto-scale.service';
import { AUTO_SCALE_CONFIG, PoolMetrics } from '../types/auto-scale.types';

@Injectable()
export class ExecutorPoolService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(ExecutorPoolService.name);
  private executorPool: Pool<ChildProcess>;
  private healthService: ChildProcessHealthService;
  private healthLogger: ExecutorHealthLogger;
  private autoScaleService: PoolAutoScaleService;
  private poolMin: number;
  private poolMax: number;

  onModuleInit() {
    this.healthLogger = new ExecutorHealthLogger();
    this.healthService = new ChildProcessHealthService(this.healthLogger);

    const maxOldSpaceSize = process.env.HANDLER_EXECUTOR_MAX_MEMORY || '512';

    this.poolMin = parseInt(process.env.HANDLER_EXECUTOR_POOL_MIN || String(AUTO_SCALE_CONFIG.minProcesses), 10);
    this.poolMax = parseInt(process.env.HANDLER_EXECUTOR_POOL_MAX || String(AUTO_SCALE_CONFIG.configMax), 10);

    this.autoScaleService = new PoolAutoScaleService(this.poolMax);

    const factory = {
      create: async (): Promise<ChildProcess> => {
        const child = fork(path.resolve(__dirname, '..', 'runner.js'), {
          execArgv: [`--max-old-space-size=${maxOldSpaceSize}`],
          silent: true,
        });
        this.healthService.registerProcess(child);
        return child;
      },

      destroy: async (child: ChildProcess): Promise<void> => {
        this.healthService.unregisterProcess(child);
        child.removeAllListeners();
        (child as any).stderr?.removeAllListeners?.();
        child.kill();
      },

      validate: async (child: ChildProcess): Promise<boolean> => {
        if (!child || child.killed || !child.connected) {
          return false;
        }
        return !this.healthService.shouldRecycle(child);
      },
    };

    this.executorPool = createPool(factory, {
      min: this.poolMin,
      max: this.poolMax * 2,
      idleTimeoutMillis: 30000,
      evictionRunIntervalMillis: 5000,
      numTestsPerEvictionRun: 2,
      softIdleTimeoutMillis: 10000,
      testOnBorrow: true,
      testOnReturn: false,
    });
  }

  onModuleDestroy() {
    // Pool will be drained automatically
  }

  getPool(): Pool<ChildProcess> {
    return this.executorPool;
  }

  getHealthService(): ChildProcessHealthService {
    return this.healthService;
  }

  getHealthStats() {
    return this.healthService.getStats();
  }

  async getProcessDetails(): Promise<any[]> {
    const details: any[] = [];
    const allMetadata = this.healthService.getAllMetadata();

    for (const [_child, metadata] of allMetadata) {
      details.push({
        pid: metadata.pid,
        executionCount: metadata.executionCount,
        errorCount: metadata.errorCount,
        ageMs: Date.now() - metadata.createdAt,
        avgExecutionTimeMs: Math.round(metadata.avgExecutionTimeMs),
      });
    }

    return details;
  }

  async recycleAll(): Promise<void> {
    const allMetadata = this.healthService.getAllMetadata();
    const destroyPromises: Promise<void>[] = [];

    for (const [child] of allMetadata) {
      destroyPromises.push(
        this.executorPool.destroy(child).catch((err) => {
          this.logger.error(`recycleAll: pool.destroy failed (pid=${(child as any).pid}): ${err?.message}`);
        }),
      );
    }

    await Promise.all(destroyPromises);
  }

  getAutoScaleService(): PoolAutoScaleService {
    return this.autoScaleService;
  }

  getPoolMetrics(): PoolMetrics {
    return this.autoScaleService.getMetrics(this.executorPool);
  }

  async checkAndScale(): Promise<{ scaled: boolean; direction: string; reason: string }> {
    const decision = this.autoScaleService.evaluate(this.executorPool);

    if (!decision.shouldScale) {
      return { scaled: false, direction: 'none', reason: decision.reason };
    }

    if (decision.direction === 'up') {
      const autoScaleMax = this.autoScaleService.getAutoScaleMax();
      if (decision.targetSize <= autoScaleMax) {
        this.autoScaleService.setMaxSize(decision.targetSize);
        this.healthLogger.logScaleUp(decision.currentSize, decision.targetSize, decision.reason, this.poolMax, autoScaleMax);
        return { scaled: true, direction: 'up', reason: decision.reason };
      }
    }

    if (decision.direction === 'down') {
      const idleProcess = this.getIdleProcess();
      if (idleProcess) {
        try {
          await this.executorPool.destroy(idleProcess);
          this.autoScaleService.setMaxSize(decision.targetSize);
          this.healthLogger.logScaleDown(decision.currentSize, decision.targetSize, decision.reason);
          return { scaled: true, direction: 'down', reason: decision.reason };
        } catch (err) {
          this.logger.error(`scale-down: pool.destroy failed (pid=${(idleProcess as any).pid}): ${(err as Error)?.message}`);
        }
      }
    }

    return { scaled: false, direction: decision.direction, reason: decision.reason };
  }

  private getIdleProcess(): ChildProcess | null {
    const allMetadata = this.healthService.getAllMetadata();
    const borrowed = this.executorPool.borrowed;
    let borrowedCount = 0;

    for (const [child] of allMetadata) {
      if (borrowedCount >= borrowed) {
        return child;
      }
      borrowedCount++;
    }

    if (allMetadata.size > borrowed) {
      const [[firstChild]] = allMetadata;
      return firstChild || null;
    }

    return null;
  }
}