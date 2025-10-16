import { Injectable, OnModuleInit } from '@nestjs/common';
import { fork } from 'child_process';
import { createPool, Pool } from 'generic-pool';
import * as path from 'path';

@Injectable()
export class ExecutorPoolService implements OnModuleInit {
  private executorPool: Pool<any>;
  async onModuleInit() {
    const factory = {
      async create() {
        const child = fork(path.resolve(__dirname, '..', 'runner.js'), {
          execArgv: ['--max-old-space-size=256'],
        });
        console.log('[EXECUTOR-POOL] Child process created');
        return child;
      },
      async destroy(child) {
        child.kill();
      },
    };
    this.executorPool = createPool(factory, {
      min: 2,
      max: 4,
      idleTimeoutMillis: 30000,
      acquireTimeoutMillis: 10000, // Timeout acquire after 10s
    });
  }
  getPool() {
    return this.executorPool;
  }
}
