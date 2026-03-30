import { Global, Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { BullModule } from '@nestjs/bullmq';
import { FlowService } from './services/flow.service';
import { FlowSchedulerService } from './services/flow-scheduler.service';
import { FlowExecutionQueueService } from './queues/flow-execution-queue.service';
import { CacheModule } from '../../infrastructure/cache/cache.module';
import { HandlerExecutorModule } from '../../infrastructure/handler-executor/handler-executor.module';
import { QueryBuilderModule } from '../../infrastructure/query-builder/query-builder.module';

@Global()
@Module({
  imports: [
    QueryBuilderModule,
    CacheModule,
    HandlerExecutorModule,
    BullModule.forRootAsync({
      imports: [ConfigModule],
      inject: [ConfigService],
      useFactory: (configService: ConfigService) => ({
        connection: {
          host: configService.get('REDIS_HOST') || 'localhost',
          port: configService.get<number>('REDIS_PORT') || 6379,
          db: configService.get<number>('REDIS_DB') || 0,
          password: configService.get('REDIS_PASSWORD'),
          url: configService.get('REDIS_URI'),
        },
      }),
    }),
    BullModule.registerQueue({
      name: 'flow-execution',
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
