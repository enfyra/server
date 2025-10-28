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
    this.executorPool = createPool(factory, {
      min: 2,
      max: 4,
      idleTimeoutMillis: 30000,
      acquireTimeoutMillis: 10000,
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
