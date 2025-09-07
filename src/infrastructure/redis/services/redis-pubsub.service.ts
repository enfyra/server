import {
  Injectable,
  OnModuleInit,
  OnModuleDestroy,
  Inject,
  forwardRef,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { RedisService } from '@liaoliaots/nestjs-redis';
import Redis from 'ioredis';
import { SchemaReloadService } from '../../../modules/schema-management/services/schema-reload.service';
import { SCHEMA_UPDATED_EVENT_KEY } from '../../../shared/utils/constant';

@Injectable()
export class RedisPubSubService implements OnModuleInit, OnModuleDestroy {
  constructor(
    private configService: ConfigService,
    private redisService: RedisService,
    @Inject(forwardRef(() => SchemaReloadService))
    private schemaReloadService: SchemaReloadService,
  ) {}

  private pub: Redis;
  private sub: Redis;

  async onModuleInit() {
    try {
      this.pub = this.redisService.getOrNil();

      if (!this.pub) {
        throw new Error(
          'Redis connection not available - getOrNil() returned null',
        );
      }

      // ✅ Tạo separate connection cho subscription
      this.sub = new Redis(this.configService.get<string>('REDIS_URI'));

      // ✅ Test connections
      await Promise.all([this.pub.ping(), this.sub.ping()]);

      await this.sub.subscribe(SCHEMA_UPDATED_EVENT_KEY);
      this.sub.on(
        'message',
        async (channel, message) =>
          await this.schemaReloadService.subscribe(message),
      );

      console.log('[RedisPubSub] ✅ Service initialized successfully');
    } catch (error) {
      console.error(
        '[RedisPubSub] ❌ Failed to initialize Redis connections:',
        error,
      );
      // ✅ FAIL FAST: Throw error để server không start
      throw new Error(`RedisPubSub initialization failed: ${error.message}`);
    }
  }

  async publish(channel: string, payload: any) {
    try {
      console.log(`[RedisPubSub] 📤 Publishing to channel: ${channel}`);
      console.log(`[RedisPubSub] 📝 Content: ${payload}`);

      const message =
        typeof payload === 'string' ? payload : JSON.stringify(payload);

      await this.pub.publish(channel, message);
      console.log(`[RedisPubSub] ✅ Published successfully to ${channel}`);
    } catch (error) {
      console.error(`[RedisPubSub] ❌ Failed to publish to ${channel}:`, error);
      throw error; // Re-throw để caller biết
    }
  }

  onModuleDestroy() {
    try {
      // ✅ Chỉ disconnect subscription connection
      this.sub?.disconnect();
      console.log('[RedisPubSub] 🔌 Service destroyed successfully');
    } catch (error) {
      console.error('[RedisPubSub] ❌ Error during cleanup:', error);
    }
  }
}
