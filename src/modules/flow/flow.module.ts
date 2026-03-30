import { Global, Module } from '@nestjs/common';
import { BullModule } from '@nestjs/bullmq';
import { FlowService } from './services/flow.service';
import { FlowSchedulerService } from './services/flow-scheduler.service';
import { FlowExecutionQueueService } from './queues/flow-execution-queue.service';
import { CacheModule } from '../../infrastructure/cache/cache.module';
import { HandlerExecutorModule } from '../../infrastructure/handler-executor/handler-executor.module';
import { QueryBuilderModule } from '../../infrastructure/query-builder/query-builder.module';
import { SYSTEM_QUEUES } from '../../shared/utils/constant';

@Global()
@Module({
  imports: [
    QueryBuilderModule,
    CacheModule,
    HandlerExecutorModule,
    BullModule.registerQueue({
      name: SYSTEM_QUEUES.FLOW_EXECUTION,
      defaultJobOptions: {
        attempts: 1,
        removeOnComplete: { count: 200, age: 3600 * 24 },
        removeOnFail: { count: 500, age: 3600 * 24 * 7 },
      },
    }),
  ],
  providers: [
    FlowService,
    FlowSchedulerService,
    FlowExecutionQueueService,
  ],
  exports: [
    FlowService,
  ],
})
export class FlowModule {}
