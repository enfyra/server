import type { TCacheInvalidationPayload } from '../../../shared/types/cache.types';
import { DatabaseConfigService } from '../../../shared/services';
import { EventEmitter2 } from 'eventemitter2';
import { QueryBuilderService } from '@enfyra/kernel';
import { BaseCacheService, CacheConfig } from './base-cache.service';
import { RedisRuntimeCacheStore } from './redis-runtime-cache-store.service';
import { CACHE_IDENTIFIERS } from '../../../shared/utils/cache-events.constants';

const STORAGE_CONFIG: CacheConfig = {
  cacheIdentifier: CACHE_IDENTIFIERS.STORAGE,
  colorCode: '\x1b[37m',
  cacheName: 'StorageConfigCache',
};

export class StorageConfigCacheBuilder extends BaseCacheService<
  Map<string | number, any>
> {
  private readonly queryBuilderService: QueryBuilderService;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    eventEmitter?: EventEmitter2;
    redisRuntimeCacheStore?: RedisRuntimeCacheStore;
  }) {
    super(STORAGE_CONFIG, deps.eventEmitter, deps.redisRuntimeCacheStore);
    this.queryBuilderService = deps.queryBuilderService;
  }

  override supportsPartialReload(): boolean {
    return true;
  }

  protected override async applyPartialUpdate(
    payload: TCacheInvalidationPayload,
  ): Promise<void> {
    if (payload.table !== 'enfyra_storage_config') {
      await this.reloadFromPartial();
      return;
    }
    const ids = payload.ids ?? [];
    if (!ids.length) return;
    const result = await this.queryBuilderService.find({
      table: 'enfyra_storage_config',
      fields: ['*'],
      filter: {
        _and: [
          { isEnabled: { _eq: true } },
          { [DatabaseConfigService.getPkField()]: { _in: ids } },
        ],
      },
      limit: -1,
    });
    const updates = this.transformData(result.data);
    const changed = new Set(ids.map(String));
    const next = new Map(
      [...this.cache].filter(([id]) => !changed.has(String(id))),
    );
    for (const [id, value] of updates) next.set(id, value);
    this.cache = next;
  }

  protected async loadFromDb(): Promise<any[]> {
    const result = await this.queryBuilderService.find({
      table: 'enfyra_storage_config',
      filter: { isEnabled: { _eq: true } },
      fields: ['*'],
      limit: -1,
    });

    return result.data || [];
  }

  protected transformData(configs: any[]): Map<string | number, any> {
    const configsMap = new Map<string | number, any>();
    const isMongoDb = this.queryBuilderService.isMongoDb();

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

  protected override getLogCount(): string {
    return `${this.cache.size} storage configs`;
  }
}
