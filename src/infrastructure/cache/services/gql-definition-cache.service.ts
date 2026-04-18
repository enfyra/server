import { EventEmitter2 } from 'eventemitter2';
import { BaseCacheService } from './base-cache.service';
import { QueryBuilderService } from '../../query-builder/query-builder.service';
import {
  CACHE_IDENTIFIERS,
  isMetadataTable,
} from '../../../shared/utils/cache-events.constants';

export interface TGqlDefinition {
  id: number;
  isEnabled: boolean;
  isSystem: boolean;
  description: string | null;
  metadata: Record<string, any> | null;
  tableName: string;
}

export class GqlDefinitionCacheService extends BaseCacheService<
  Map<string, TGqlDefinition>
> {
  private readonly queryBuilderService: QueryBuilderService;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    eventEmitter: EventEmitter2;
  }) {
    super(
      {
        cacheIdentifier: CACHE_IDENTIFIERS.GRAPHQL,
        colorCode: '\x1b[38;5;183m',
        cacheName: 'GqlDefinitionCache',
      },
      deps.eventEmitter,
    );
    this.queryBuilderService = deps.queryBuilderService;
  }

  protected async loadFromDb(): Promise<any> {
    try {
      const result = await this.queryBuilderService.find({
        table: 'gql_definition',
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

  async isEnabledForTable(tableName: string): Promise<boolean> {
    await this.ensureLoaded();
    if (isMetadataTable(tableName)) return false;
    const def = this.cache?.get(tableName);
    return !!def?.isEnabled;
  }

  async getForTable(tableName: string): Promise<TGqlDefinition | undefined> {
    await this.ensureLoaded();
    return this.cache?.get(tableName);
  }

  async getAllEnabled(): Promise<TGqlDefinition[]> {
    await this.ensureLoaded();
    if (!this.cache) return [];
    return Array.from(this.cache.values()).filter(
      (d) => d.isEnabled && !isMetadataTable(d.tableName),
    );
  }
}
