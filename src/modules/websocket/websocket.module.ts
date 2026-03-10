import { Global, Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { BullModule } from '@nestjs/bullmq';
import { WebsocketGatewayFactory } from './gateway/websocket-gateway.factory';
import { DynamicWebSocketGateway } from './gateway/dynamic-websocket.gateway';
import { ConnectionQueueService } from './queues/connection-queue.service';
import { EventQueueService } from './queues/event-queue.service';
import { WebsocketCacheService } from '../../infrastructure/cache/services/websocket-cache.service';
import { QueryBuilderModule } from '../../infrastructure/query-builder/query-builder.module';
import { CacheModule } from '../../infrastructure/cache/cache.module';
import { HandlerExecutorModule } from '../../infrastructure/handler-executor/handler-executor.module';
import { WebsocketEmitService } from './services/websocket-emit.service';

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
    BullModule.registerQueue(
      {
        name: 'ws-connection',
        defaultJobOptions: {
          attempts: 0,
          removeOnComplete: { count: 100, age: 3600 },
          removeOnFail: { count: 500, age: 24 * 3600 },
        },
      },
      {
        name: 'ws-event',
        defaultJobOptions: {
          attempts: 0,
          removeOnComplete: { count: 100, age: 3600 },
          removeOnFail: { count: 500, age: 24 * 3600 },
        },
      },
    ),
  ],
  providers: [
    WebsocketGatewayFactory,
    DynamicWebSocketGateway,
    ConnectionQueueService,
    EventQueueService,
    WebsocketEmitService,
  ],
  exports: [
    WebsocketGatewayFactory,
    DynamicWebSocketGateway,
    WebsocketEmitService,
  ],
})
export class WebsocketModule {}
