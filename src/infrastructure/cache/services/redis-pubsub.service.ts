import { Injectable, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import Redis from 'ioredis';

@Injectable()
export class RedisPubSubService implements OnModuleInit, OnModuleDestroy {
  public pub: Redis;
  public sub: Redis;
  private subscribedChannels = new Map<string, Array<(channel: string, message: string) => void>>();
  private nodeName: string | null = null;
  private redisUri: string;

  constructor(
    private configService: ConfigService,
  ) {
    this.redisUri = this.configService.get<string>('REDIS_URI');
  }

  async onModuleInit() {
    try {
      this.pub = new Redis(this.redisUri);
      this.sub = new Redis(this.redisUri, {
        enableReadyCheck: false,
      });

      this.pub.on('error', (err) => {
        console.error('[RedisPubSub] pub connection error:', err.message);
      });
      this.sub.on('error', (err) => {
        console.error('[RedisPubSub] sub connection error:', err.message);
      });

      await Promise.all([
        this.pub.ping(),
        new Promise<void>((resolve, reject) => {
          const timeout = setTimeout(() => reject(new Error('Redis sub timeout')), 5000);
          this.sub.once('ready', () => { clearTimeout(timeout); resolve(); });
          this.sub.once('error', (err) => { clearTimeout(timeout); reject(err); });
        })
      ]);

      this.sub.on('message', (channel: string, message: string) => {
        const handlers = this.subscribedChannels.get(channel);
        if (handlers) {
          for (const handler of handlers) {
            handler(channel, message);
          }
        }
      });
    } catch (error) {
      console.error('[RedisPubSub] Failed to initialize:', error);
      throw error;
    }
  }

  subscribeWithHandler(
    channel: string,
    handler: (channel: string, message: string) => void
  ): boolean {
    if (!this.sub) {
      setTimeout(() => this.subscribeWithHandler(channel, handler), 100);
      return false;
    }
    const decoratedChannel = this.decorateChannel(channel);
    const existing = this.subscribedChannels.get(decoratedChannel);
    if (existing) {
      existing.push(handler);
      return true;
    }

    this.subscribedChannels.set(decoratedChannel, [handler]);

    this.sub.subscribe(decoratedChannel).then(() => undefined)
      .catch((err) => {
        console.error(`[RedisPubSub] Subscribe error for ${decoratedChannel}:`, err.message);
      });

    return true;
  }

  async publish(channel: string, payload: any) {
    try {
      const decoratedChannel = this.decorateChannel(channel);
      const message =
        typeof payload === 'string' ? payload : JSON.stringify(payload);

      await this.pub.publish(decoratedChannel, message);
    } catch (error) {
      console.error(`[RedisPubSub] Failed to publish to ${channel}:`, error);
      throw error;
    }
  }

  private decorateChannel(channel: string): string {
    const nodeName = this.getNodeName();
    if (!nodeName) {
      return channel;
    }
    return `${channel}:${nodeName}`;
  }

  isChannelForBase(receivedChannel: string, baseChannel: string): boolean {
    if (receivedChannel === baseChannel) {
      return true;
    }
    const nodeName = this.getNodeName();
    if (!nodeName) {
      return false;
    }
    return receivedChannel === `${baseChannel}:${nodeName}`;
  }

  private getNodeName(): string | null {
    if (this.nodeName !== null) {
      return this.nodeName;
    }
    this.nodeName = this.configService.get<string>('NODE_NAME') || null;
    return this.nodeName;
  }

  onModuleDestroy() {
    try {
      this.sub?.disconnect();
      this.pub?.disconnect();
    } catch (error) {
      console.error('[RedisPubSub] Error during cleanup:', error);
    }
  }
}