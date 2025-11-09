import { Injectable, OnModuleInit } from '@nestjs/common';
import { fork } from 'child_process';
import { createPool, Pool } from 'generic-pool';
import * as path from 'path';

@Injectable()
export class ExecutorPoolService implements OnModuleInit {
  private executorPool: Pool<any>;
  async onModuleInit() {
    const maxOldSpaceSize = process.env.HANDLER_EXECUTOR_MAX_MEMORY || '512';
    const factory = {
      async create() {
        const child = fork(path.resolve(__dirname, '..', 'runner.js'), {
          execArgv: [`--max-old-space-size=${maxOldSpaceSize}`],
          silent: true,
        });
        return child;
      },
      async destroy(child) {
        child.kill();
      },
      async validate(child) {
        return child && !child.killed && child.connected;
      },
    };
    const poolMin = parseInt(process.env.HANDLER_EXECUTOR_POOL_MIN || '2', 10);
    const poolMax = parseInt(process.env.HANDLER_EXECUTOR_POOL_MAX || '4', 10);
    this.executorPool = createPool(factory, {
      min: poolMin,
      max: poolMax,
      idleTimeoutMillis: 30000,
      evictionRunIntervalMillis: 5000,
      numTestsPerEvictionRun: 2,
      softIdleTimeoutMillis: 10000,
      testOnBorrow: true,
      testOnReturn: false,
    });
  }
  getPool() {
    return this.executorPool;
  }
}
