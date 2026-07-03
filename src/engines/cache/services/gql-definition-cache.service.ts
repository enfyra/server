import { EventEmitter2 } from 'eventemitter2';
import { BaseCacheService } from './base-cache.service';
import { QueryBuilderService } from '@enfyra/kernel';
import { RedisRuntimeCacheStore } from './redis-runtime-cache-store.service';
import {
  CACHE_IDENTIFIERS,
  isMetadataTable,
} from '../../../shared/utils/cache-events.constants';
import type { TGqlDefinition } from '../types/cache-data.types';

export class GqlDefinitionCacheService extends BaseCacheService<
  Map<string, TGqlDefinition>
> {
  private readonly queryBuilderService: QueryBuilderService;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    eventEmitter: EventEmitter2;
    redisRuntimeCacheStore?: RedisRuntimeCacheStore;
  }) {
    super(
      {
        cacheIdentifier: CACHE_IDENTIFIERS.GRAPHQL,
        colorCode: '\x1b[38;5;183m',
        cacheName: 'GqlDefinitionCache',
      },
      deps.eventEmitter,
      deps.redisRuntimeCacheStore,
    );
    this.queryBuilderService = deps.queryBuilderService;
  }

  protected async loadFromDb(): Promise<any> {
    try {
      const result = await this.queryBuilderService.find({
        table: 'enfyra_graphql',
        fields: ['*', 'table.id', 'table.name'],
        limit: 10000,
      });
      return result?.data ?? [];
    } catch {
      return [];
    }
  }

  protected transformData(rawData: any): Map<string, TGqlDefinition> {
    const map = new Map<string, TGqlDefinition>();
    const rows: any[] = Array.isArray(rawData) ? rawData : [];

    for (const row of rows) {
      const tableName = row?.table?.name;
      if (!tableName) continue;

      map.set(tableName, {
        id: row.id,
        isEnabled: !!row.isEnabled,
        isSystem: !!row.isSystem,
        description: row.description ?? null,
        metadata: row.metadata ?? null,
        tableName,
      });
    }

    return map;
  }

  protected getLogCount(): string {
    return `${this.cache?.size ?? 0} definitions`;
  }

  async getAllEnabledFromCache(): Promise<TGqlDefinition[]> {
    const cache = await this.getCacheAsync();
    return this.filterEnabled(cache);
  }

  private filterEnabled(
    cache: Map<string, TGqlDefinition> | undefined,
  ): TGqlDefinition[] {
    if (!cache) return [];
    return Array.from(cache.values()).filter(
      (d) => d.isEnabled && !isMetadataTable(d.tableName),
    );
  }
}
