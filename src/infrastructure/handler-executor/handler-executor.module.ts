import { Global, Module } from '@nestjs/common';
import { HandlerExecutorService } from './services/handler-executor.service';
import { ExecutorPoolService } from './services/executor-pool.service';

@Global()
@Module({
  providers: [HandlerExecutorService, ExecutorPoolService],
  exports: [HandlerExecutorService, ExecutorPoolService],
})
export class HandlerExecutorModule {}
