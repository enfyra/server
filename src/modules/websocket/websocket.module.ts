import { Global, Module } from '@nestjs/common';
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
import { BuiltInSocketRegistry } from './services/built-in-socket.registry';
import { SYSTEM_QUEUES } from '../../shared/utils/constant';

@Global()
@Module({
  imports: [
    QueryBuilderModule,
    CacheModule,
    HandlerExecutorModule,
    BullModule.registerQueue(
      {
        name: SYSTEM_QUEUES.WS_CONNECTION,
        defaultJobOptions: {
          attempts: 0,
          removeOnComplete: { count: 100, age: 3600 },
          removeOnFail: { count: 500, age: 24 * 3600 },
        },
      },
      {
        name: SYSTEM_QUEUES.WS_EVENT,
        defaultJobOptions: {
          attempts: 0,
          removeOnComplete: { count: 100, age: 3600 },
          removeOnFail: { count: 500, age: 24 * 3600 },
        },
      },
    ),
  ],
  providers: [
    BuiltInSocketRegistry,
    WebsocketGatewayFactory,
    DynamicWebSocketGateway,
    ConnectionQueueService,
    EventQueueService,
    WebsocketEmitService,
  ],
  exports: [
    BuiltInSocketRegistry,
    WebsocketGatewayFactory,
    DynamicWebSocketGateway,
    WebsocketEmitService,
  ],
})
export class WebsocketModule {}
