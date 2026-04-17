import { Injectable } from '@nestjs/common';
import { EventEmitter2 } from '@nestjs/event-emitter';
import { BaseCacheService } from './base-cache.service';
import { QueryBuilderService } from '../../query-builder/query-builder.service';
import {
  CACHE_IDENTIFIERS,
  isMetadataTable,
} from '../../../shared/utils/cache-events.constants';
import { DatabaseConfigService } from '../../../shared/services/database-config.service';

export interface TGqlDefinition {
  id: number;
  isEnabled: boolean;
  isSystem: boolean;
  description: string | null;
  metadata: Record<string, any> | null;
  tableName: string;
}

@Injectable()
export class GqlDefinitionCacheService extends BaseCacheService<
  Map<string, TGqlDefinition>
> {
  constructor(
    private readonly queryBuilder: QueryBuilderService,
    eventEmitter: EventEmitter2,
  ) {
    super(
      {
        cacheIdentifier: CACHE_IDENTIFIERS.GRAPHQL,
        colorCode: '\x1b[38;5;183m',
        cacheName: 'GqlDefinitionCache',
      },
      eventEmitter,
    );
  }

  protected async loadFromDb(): Promise<any> {
    try {
      const result = await this.queryBuilder.find({
        table: 'gql_definition',
        limit: 10000,
      });
      const rows = result?.data ?? [];

      // If rows exist but have no tableName from relation join, resolve manually
      const needsResolution = rows.length > 0 && !rows[0]?.table?.name;
      if (needsResolution) {
        const isMongoDB = DatabaseConfigService.instanceIsMongoDb();
        const tableIds = rows
          .map((r: any) => r.tableId || r.table)
          .filter(Boolean);
        if (tableIds.length === 0) return rows;

        const pkField = DatabaseConfigService.getPkField();
        const tables = await this.queryBuilder.find({
          table: 'table_definition',
          filter: { [pkField]: { _in: tableIds } },
          fields: [pkField, 'name'],
        });
        const tableMap = new Map(
          (tables.data || []).map((t: any) => [String(t[pkField]), t.name]),
        );

        return rows.map((row: any) => ({
          ...row,
          table: {
            name: tableMap.get(String(row.tableId || row.table)) || null,
          },
        }));
      }

      return rows;
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
        isEnabled: row.isEnabled !== false,
        isSystem: row.isSystem === true,
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
    return def?.isEnabled === true;
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
