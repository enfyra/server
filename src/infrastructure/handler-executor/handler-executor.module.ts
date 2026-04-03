import { Global, Module } from '@nestjs/common';
import { HandlerExecutorService } from './services/handler-executor.service';
import { IsolatedExecutorService } from './services/isolated-executor.service';
import { CacheModule } from '../cache/cache.module';

@Global()
@Module({
  imports: [CacheModule],
  providers: [IsolatedExecutorService, HandlerExecutorService],
  exports: [HandlerExecutorService],
})
export class HandlerExecutorModule {}
