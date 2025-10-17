import { Global, Module, forwardRef } from '@nestjs/common';
import { CacheService } from './services/cache.service';
import { RedisPubSubService } from './services/redis-pubsub.service';
import { RouteCacheService } from './services/route-cache.service';
import { MetadataCacheService } from './services/metadata-cache.service';
import { KnexModule } from '../knex/knex.module';

@Global()
@Module({
  imports: [forwardRef(() => KnexModule)],
  providers: [CacheService, RedisPubSubService, RouteCacheService, MetadataCacheService],
  exports: [CacheService, RedisPubSubService, RouteCacheService, MetadataCacheService],
})
export class CacheModule {}
