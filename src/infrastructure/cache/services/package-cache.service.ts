import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { QueryBuilderService } from '../../query-builder/query-builder.service';
import { RedisPubSubService } from './redis-pubsub.service';
import { InstanceService } from '../../../shared/services/instance.service';
import { PACKAGE_CACHE_SYNC_EVENT_KEY } from '../../../shared/utils/constant';

@Injectable()
export class PackageCacheService implements OnModuleInit {
  private readonly logger = new Logger(PackageCacheService.name);
  private packagesCache: string[] = [];
  private cacheLoaded = false;

  constructor(
    private readonly queryBuilder: QueryBuilderService,
    private readonly redisPubSubService: RedisPubSubService,
    private readonly instanceService: InstanceService,
  ) {}

  async onModuleInit() {
    this.subscribeToPackageCacheSync();
  }

  private subscribeToPackageCacheSync() {
    const sub = this.redisPubSubService.sub;
    if (!sub) {
      this.logger.warn('Redis subscription not available for package cache sync');
      return;
    }

    sub.subscribe(PACKAGE_CACHE_SYNC_EVENT_KEY);
    
    sub.on('message', (channel: string, message: string) => {
      if (channel === PACKAGE_CACHE_SYNC_EVENT_KEY) {
        try {
          const payload = JSON.parse(message);
          const myInstanceId = this.instanceService.getInstanceId();
          
          if (payload.instanceId === myInstanceId) {
            this.logger.debug('⏭️  Skipping package cache sync from self');
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
    });
  }

  async getPackages(): Promise<string[]> {
    if (!this.cacheLoaded) {
      await this.reloadPackageCache();
    }
    return this.packagesCache;
  }

  async reloadPackageCache(): Promise<void> {
    const start = Date.now();
    this.logger.log('🔄 Reloading packages cache...');

    const packages = await this.loadPackages();
    this.packagesCache = packages;
    this.cacheLoaded = true;

    this.logger.log(
      `✅ Loaded ${packages.length} packages in ${Date.now() - start}ms`,
    );

    await this.publishPackageCacheSync(packages);
  }

  private async publishPackageCacheSync(packages: string[]): Promise<void> {
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
    const packages = await this.queryBuilder.select({
      table: 'package_definition',
      where: [
        { field: 'isEnabled', operator: '=', value: true },
        { field: 'type', operator: '=', value: 'Backend' },
      ],
      select: ['name'],
    });

    return packages.map((p: any) => p.name);
  }
}
