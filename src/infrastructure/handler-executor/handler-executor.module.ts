import { Global, Module } from '@nestjs/common';
import { HandlerExecutorService } from './services/handler-executor.service';
import { ExecutorPoolService } from './services/executor-pool.service';
import { CacheModule } from '../cache/cache.module';

@Global()
@Module({
  imports: [CacheModule],
  providers: [ExecutorPoolService, HandlerExecutorService],
  exports: [HandlerExecutorService],
})
export class HandlerExecutorModule {}
