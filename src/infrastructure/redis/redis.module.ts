import { Module } from '@nestjs/common';
import { RedisLockService } from './services/redis-lock.service';
import { RedisPubSubService } from './services/redis-pubsub.service';
import { RouteCacheService } from './services/route-cache.service';

@Module({
  providers: [RedisLockService, RedisPubSubService, RouteCacheService],
  exports: [RedisLockService, RedisPubSubService, RouteCacheService],
})
export class RedisModule {}
