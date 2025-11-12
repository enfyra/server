import {
  Injectable,
  OnModuleInit,
  OnModuleDestroy,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { RedisService } from '@liaoliaots/nestjs-redis';
import Redis from 'ioredis';

@Injectable()
export class RedisPubSubService implements OnModuleInit, OnModuleDestroy {
  public pub: Redis;
  public sub: Redis;
  private subscribedChannels = new Map<string, (channel: string, message: string) => void>();

  constructor(
    private configService: ConfigService,
    private redisService: RedisService,
  ) {}

  async onModuleInit() {
    try {
      this.pub = this.redisService.getOrNil();

      if (!this.pub) {
        throw new Error(
          'Redis connection not available - getOrNil() returned null',
        );
      }

      this.sub = new Redis(this.configService.get<string>('REDIS_URI'));
      await Promise.all([this.pub.ping(), this.sub.ping()]);

      this.sub.on('message', (channel: string, message: string) => {
        const handler = this.subscribedChannels.get(channel);
        if (handler) {
          handler(channel, message);
        }
      });

      console.log('[RedisPubSub] Master message handler initialized');

    } catch (error) {
      console.error(
        '[RedisPubSub] Failed to initialize Redis connections:',
        error,
      );
      throw new Error(`RedisPubSub initialization failed: ${error.message}`);
    }
  }

  subscribeWithHandler(
    channel: string,
    handler: (channel: string, message: string) => void
  ): boolean {
    if (this.subscribedChannels.has(channel)) {
      return false;
    }

    this.subscribedChannels.set(channel, handler);
    this.sub.subscribe(channel);

    console.log(`[RedisPubSub] Subscribed to channel: ${channel}`);

    return true;
  }

  async publish(channel: string, payload: any) {
    try {

      const message =
        typeof payload === 'string' ? payload : JSON.stringify(payload);

      await this.pub.publish(channel, message);
    } catch (error) {
      console.error(`[RedisPubSub] Failed to publish to ${channel}:`, error);
      throw error;
    }
  }

  onModuleDestroy() {
    try {
      this.sub?.disconnect();
    } catch (error) {
      console.error('[RedisPubSub] Error during cleanup:', error);
    }
  }
}
