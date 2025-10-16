import { Injectable, Logger, OnModuleInit, OnApplicationBootstrap } from '@nestjs/common';
import { QueryBuilderService } from '../../query-builder/query-builder.service';
import { RedisPubSubService } from './redis-pubsub.service';
import { CacheService } from './cache.service';
import { InstanceService } from '../../../shared/services/instance.service';
import {
  PACKAGE_CACHE_SYNC_EVENT_KEY,
  PACKAGE_RELOAD_LOCK_KEY,
  REDIS_TTL,
} from '../../../shared/utils/constant';

@Injectable()
export class PackageCacheService implements OnModuleInit, OnApplicationBootstrap {
  private readonly logger = new Logger(PackageCacheService.name);
  private packagesCache: string[] = [];
  private cacheLoaded = false;
  private messageHandler: ((channel: string, message: string) => void) | null = null;

  constructor(
    private readonly queryBuilder: QueryBuilderService,
    private readonly redisPubSubService: RedisPubSubService,
    private readonly cacheService: CacheService,
    private readonly instanceService: InstanceService,
  ) {}

  async onModuleInit() {
    this.subscribe();
  }

  async onApplicationBootstrap() {
    await this.reload();
  }

  /**
   * Subscribe to package sync messages from other instances
   */
  private subscribe() {
    const sub = this.redisPubSubService.sub;
    if (!sub) {
      this.logger.warn('Redis subscription not available for package cache sync');
      return;
    }

    // Only subscribe if not already subscribed
    if (this.messageHandler) {
      return;
    }

    // Create and store handler
    this.messageHandler = (channel: string, message: string) => {
      if (channel === PACKAGE_CACHE_SYNC_EVENT_KEY) {
        try {
          const payload = JSON.parse(message);
          const myInstanceId = this.instanceService.getInstanceId();

          if (payload.instanceId === myInstanceId) {
            return;
          }

          this.logger.log(`📥 Received package cache sync from instance ${payload.instanceId.slice(0, 8)}...`);
          this.packagesCache = payload.packages;
          this.cacheLoaded = true;
          this.logger.log(`✅ Package cache synced: ${payload.packages.length} packages`);
        } catch (error) {
          this.logger.error('Failed to parse package cache sync message:', error);
        }
      }
    };

    // Subscribe via RedisPubSubService (prevents duplicates)
    this.redisPubSubService.subscribeWithHandler(
      PACKAGE_CACHE_SYNC_EVENT_KEY,
      this.messageHandler
    );
  }

  async getPackages(): Promise<string[]> {
    if (!this.cacheLoaded) {
      await this.reload();
    }
    return this.packagesCache;
  }

  /**
   * Reload packages from DB (acquire lock → load → publish → save)
   */
  async reload(): Promise<void> {
    const instanceId = this.instanceService.getInstanceId();

    try {
      const acquired = await this.cacheService.acquire(
        PACKAGE_RELOAD_LOCK_KEY,
        instanceId,
        REDIS_TTL.RELOAD_LOCK_TTL
      );

      if (!acquired) {
        this.logger.log('🔒 Another instance is reloading packages, waiting for broadcast...');
        return;
      }

      this.logger.log(`🔓 Acquired package reload lock (instance ${instanceId.slice(0, 8)})`);

      try {
        const start = Date.now();
        this.logger.log('🔄 Reloading packages cache...');

        // Load from DB
        const packages = await this.loadPackages();
        this.logger.log(`✅ Loaded ${packages.length} packages in ${Date.now() - start}ms`);

        // Broadcast to other instances FIRST
        await this.publish(packages);

        // Then save to local memory cache
        this.packagesCache = packages;
        this.cacheLoaded = true;
      } finally {
        await this.cacheService.release(PACKAGE_RELOAD_LOCK_KEY, instanceId);
        this.logger.log('🔓 Released package reload lock');
      }
    } catch (error) {
      this.logger.error('❌ Failed to reload package cache:', error);
      throw error;
    }
  }

  /**
   * Publish packages to other instances via Redis PubSub
   */
  private async publish(packages: string[]): Promise<void> {
    try {
      const payload = {
        instanceId: this.instanceService.getInstanceId(),
        packages: packages,
        timestamp: Date.now(),
      };

      await this.redisPubSubService.publish(
        PACKAGE_CACHE_SYNC_EVENT_KEY,
        JSON.stringify(payload),
      );

      this.logger.log(`📤 Published package cache to other instances (${packages.length} packages)`);
    } catch (error) {
      this.logger.error('Failed to publish package cache sync:', error);
    }
  }

  private async loadPackages(): Promise<string[]> {
    // IMPORTANT: Use raw Knex query instead of queryBuilder.select()
    // Reason: Metadata may not be loaded yet during bootstrap
    const knex = this.queryBuilder.getKnex();

    // Check if table exists first
    const hasPackageTable = await knex.schema.hasTable('package_definition');
    if (!hasPackageTable) {
      this.logger.warn('⚠️ package_definition table does not exist yet, skipping package cache load');
      return [];
    }

    // Load packages using raw Knex query
    const packages = await knex('package_definition')
      .where('isEnabled', true)
      .where('type', 'Backend')
      .select('name');

    return packages.map((p: any) => p.name);
  }
}
