import { Global, Module } from '@nestjs/common';
import { ExecutorEngineService } from './services/executor-engine.service';
import { IsolatedExecutorService } from './services/isolated-executor.service';
import { CacheModule } from '../cache/cache.module';

@Global()
@Module({
  imports: [CacheModule],
  providers: [IsolatedExecutorService, ExecutorEngineService],
  exports: [ExecutorEngineService],
})
export class ExecutorEngineModule {}
