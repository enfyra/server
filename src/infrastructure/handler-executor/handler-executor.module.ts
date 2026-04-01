import { Global, Module } from '@nestjs/common';
import { HandlerExecutorService } from './services/handler-executor.service';
import { VmExecutorService } from './services/vm-executor.service';
import { IsolatedExecutorService } from './services/isolated-executor.service';
import { CacheModule } from '../cache/cache.module';

@Global()
@Module({
  imports: [CacheModule],
  providers: [VmExecutorService, IsolatedExecutorService, HandlerExecutorService],
  exports: [HandlerExecutorService],
})
export class HandlerExecutorModule {}
