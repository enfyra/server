import { DatabaseConfigService } from '../../../shared/services/database-config.service';
import { Injectable } from '@nestjs/common';
import { EventEmitter2 } from '@nestjs/event-emitter';
import { QueryBuilderService } from '../../query-builder/query-builder.service';
import { BaseCacheService, CacheConfig } from './base-cache.service';
import {
  CACHE_IDENTIFIERS,
} from '../../../shared/utils/cache-events.constants';

const STORAGE_CONFIG: CacheConfig = {
  cacheIdentifier: CACHE_IDENTIFIERS.STORAGE,
  colorCode: '\x1b[37m',
  cacheName: 'StorageConfigCache',
};

@Injectable()
export class StorageConfigCacheService extends BaseCacheService<
  Map<string | number, any>
> {
  constructor(
    private readonly queryBuilder: QueryBuilderService,
    eventEmitter: EventEmitter2,
  ) {
    super(STORAGE_CONFIG, eventEmitter);
  }

  protected async loadFromDb(): Promise<any[]> {
    const result = await this.queryBuilder.find({
      table: 'storage_config_definition',
      filter: { isEnabled: { _eq: true } },
      fields: ['*'],
    });

    return result.data || [];
  }

  protected transformData(configs: any[]): Map<string | number, any> {
    const configsMap = new Map<string | number, any>();
    const isMongoDb = this.queryBuilder.isMongoDb();

    for (const config of configs) {
      const idField = DatabaseConfigService.getPkField();
      let id = config[idField];

      if (!id) {
        this.logger.warn('Storage config missing ID, skipping');
        continue;
      }

      if (isMongoDb) {
        if (
          typeof id === 'object' &&
          id !== null &&
          typeof id.toString === 'function'
        ) {
          id = id.toString();
        } else {
          id = String(id);
        }
      }

      configsMap.set(id, config);

      if (!isMongoDb) {
        if (typeof id === 'number') {
          configsMap.set(String(id), config);
        } else if (typeof id === 'string' && !isNaN(Number(id))) {
          configsMap.set(Number(id), config);
        }
      }
    }

    return configsMap;
  }

  protected getLogCount(): string {
    return `${this.cache.size} storage configs`;
  }

  async getStorageConfigById(
    id: number | string | null | undefined,
  ): Promise<any | null> {
    await this.ensureLoaded();

    if (!id || id === null || id === undefined) {
      return null;
    }

    let normalizedId: string | number = id;
    const isMongoDb = this.queryBuilder.isMongoDb();

    if (isMongoDb) {
      if (
        typeof id === 'object' &&
        id !== null &&
        typeof (id as any).toString === 'function'
      ) {
        normalizedId = (id as any).toString();
      } else {
        normalizedId = String(id);
      }
    }

    let config = this.cache.get(normalizedId);
    if (config) {
      return config;
    }

    if (!isMongoDb) {
      const numId =
        typeof normalizedId === 'string'
          ? parseInt(normalizedId, 10)
          : normalizedId;
      if (!isNaN(numId as number)) {
        config = this.cache.get(numId);
        if (config) {
          return config;
        }
      }
      if (typeof normalizedId === 'number') {
        config = this.cache.get(String(normalizedId));
        if (config) {
          return config;
        }
      }
    }
    return null;
  }

  async getStorageConfigByType(type: string): Promise<any | null> {
    await this.ensureLoaded();
    const values = Array.from(this.cache.values());
    for (const config of values) {
      if (config.type === type && config.isEnabled === true) {
        return config;
      }
    }
    return null;
  }
}
