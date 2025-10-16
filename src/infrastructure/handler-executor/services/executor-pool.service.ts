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
        const createStart = Date.now();
        const child = fork(path.resolve(__dirname, '..', 'runner.js'), {
          execArgv: ['--max-old-space-size=256'],
        });
        console.log(`[EXECUTOR-POOL] Child process created | took: ${Date.now() - createStart}ms`);
        return child;
      },
      async destroy(child) {
        const destroyStart = Date.now();
        child.kill();
        console.log(`[EXECUTOR-POOL] Child process destroyed | took: ${Date.now() - destroyStart}ms`);
      },
      async validate(child) {
        const validateStart = Date.now();
        // Check if child process is still alive and connected
        const isValid = child && !child.killed && child.connected;
        console.log(`[EXECUTOR-POOL] Child validation: ${isValid} | took: ${Date.now() - validateStart}ms`);
        return isValid;
      },
    };
    this.executorPool = createPool(factory, {
      min: 2,
      max: 4,
      idleTimeoutMillis: 30000,
      acquireTimeoutMillis: 10000, // Timeout acquire after 10s
      evictionRunIntervalMillis: 5000, // Check for idle resources every 5s
      numTestsPerEvictionRun: 2, // Test 2 resources per eviction run
      softIdleTimeoutMillis: 10000, // Min idle time before eviction
      testOnBorrow: true, // Validate before borrowing
      testOnReturn: false, // Don't validate on return (faster)
    });
  }
  getPool() {
    return this.executorPool;
  }
}
