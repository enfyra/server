import { EventEmitter2 } from 'eventemitter2';
import { QueryBuilderService } from '@enfyra/kernel';
import {
  CACHE_IDENTIFIERS,
  TCacheInvalidationPayload,
} from '../../../shared/utils/cache-events.constants';
import { BaseCacheService } from './base-cache.service';
import { RedisRuntimeCacheStore } from './redis-runtime-cache-store.service';
import type { MetadataCacheService } from './metadata-cache.service';

export type TColumnRuleType =
  | 'min'
  | 'max'
  | 'minLength'
  | 'maxLength'
  | 'pattern'
  | 'format'
  | 'minItems'
  | 'maxItems'
  | 'custom';

export interface TColumnRule {
  id: string | number;
  ruleType: TColumnRuleType;
  value: any;
  message: string | null;
  isEnabled: boolean;
  columnId: string;
}

function toIdString(v: any): string | null {
  if (v === undefined || v === null) return null;
  return String(v?._id ?? v?.id ?? v);
}

export class ColumnRuleCacheBuilder extends BaseCacheService<
  Map<string, TColumnRule[]>
> {
  private readonly queryBuilderService: QueryBuilderService;
  private readonly metadataCacheService: MetadataCacheService | undefined;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    eventEmitter: EventEmitter2;
    redisRuntimeCacheStore?: RedisRuntimeCacheStore;
    metadataCacheService?: MetadataCacheService;
  }) {
    super(
      {
        cacheIdentifier: CACHE_IDENTIFIERS.COLUMN_RULE,
        colorCode: '\x1b[38;5;183m',
        cacheName: 'ColumnRuleCache',
      },
      deps.eventEmitter,
      deps.redisRuntimeCacheStore,
    );
    this.queryBuilderService = deps.queryBuilderService;
    this.metadataCacheService = deps.metadataCacheService;
  }

  protected async loadFromDb(): Promise<any> {
    const result = await this.queryBuilderService.find({
      table: 'enfyra_column_rule',
      fields: [
        '*',
        this.queryBuilderService.isMongoDb() ? 'column._id' : 'column.id',
      ],
      filter: { isEnabled: { _eq: true } },
      limit: 100000,
    });
    return result?.data ?? [];
  }

  protected transformData(rawData: any): Map<string, TColumnRule[]> {
    const map = new Map<string, TColumnRule[]>();
    const rows: any[] = Array.isArray(rawData) ? rawData : [];

    for (const row of rows) {
      const rule = this.rowToRule(row);
      if (!rule) continue;
      const existing = map.get(rule.columnId);
      if (existing) existing.push(rule);
      else map.set(rule.columnId, [rule]);
    }

    return map;
  }

  override supportsPartialReload(): boolean {
    return true;
  }

  override async partialReload(
    payload: TCacheInvalidationPayload,
    publish = true,
  ): Promise<void> {
    if (
      ![
        'enfyra_column_rule',
        'enfyra_table',
        'enfyra_column',
        'enfyra_relation',
      ].includes(payload.table)
    ) {
      await this.reload(publish);
      return;
    }
    await super.partialReload(payload, publish);
  }

  protected override async applyPartialUpdate(
    payload: TCacheInvalidationPayload,
  ): Promise<void> {
    if (!this.cache) {
      throw new Error('Cache not initialized, cannot partial reload');
    }
    const ids = (payload.ids ?? []).map(String);
    if (ids.length === 0) return;
    const idField = this.queryBuilderService.isMongoDb() ? '_id' : 'id';
    if (payload.table !== 'enfyra_column_rule') {
      if (!this.metadataCacheService) {
        await this.reloadFromPartial();
        return;
      }
      const metadata = await this.metadataCacheService.getMetadata();
      const owners = new Set(ids);
      const names = new Set(payload.affectedTables ?? []);
      const tables = metadata.tablesList.filter(
        (table: any) =>
          owners.has(String(table._id ?? table.id)) || names.has(table.name),
      );
      const columns = tables.flatMap((table: any) =>
        (table.columns ?? []).map(toIdString).filter(Boolean),
      );
      const knownColumns = new Set(
        metadata.tablesList.flatMap((table: any) =>
          (table.columns ?? []).map(toIdString).filter(Boolean),
        ),
      );
      const changedColumns = new Set(columns);
      const rows = columns.length
        ? (
            await this.queryBuilderService.find({
              table: 'enfyra_column_rule',
              fields: ['*', `column.${idField}`],
              filter: { column: { [idField]: { _in: columns } } },
              limit: -1,
            })
          ).data
        : [];
      const next = new Map(
        [...this.cache].filter(
          ([columnId]) =>
            knownColumns.has(columnId) && !changedColumns.has(columnId),
        ),
      );
      for (const [columnId, rules] of this.transformData(rows))
        next.set(
          columnId,
          rules.filter((rule) => rule.isEnabled),
        );
      this.cache = next;
      return;
    }

    const result = await this.queryBuilderService.find({
      table: 'enfyra_column_rule',
      fields: [
        '*',
        this.queryBuilderService.isMongoDb() ? 'column._id' : 'column.id',
      ],
      filter: { [idField]: { _in: ids } },
      limit: ids.length,
    });
    const rows: any[] = result?.data ?? [];

    const fetchedById = new Map<string, any>();
    for (const row of rows) {
      const rid = toIdString(row);
      if (rid) fetchedById.set(rid, row);
    }

    const changed = new Set(ids);
    for (const [columnId, rules] of this.cache) {
      const retained = rules.filter((rule) => !changed.has(String(rule.id)));
      if (!retained.length) this.cache.delete(columnId);
      else if (retained.length !== rules.length)
        this.cache.set(columnId, retained);
    }
    for (const id of ids) {
      const row = fetchedById.get(id);
      if (!row) continue;
      const rule = this.rowToRule(row);
      if (!rule || !rule.isEnabled) continue;
      const bucket = this.cache.get(rule.columnId);
      if (bucket) bucket.push(rule);
      else this.cache.set(rule.columnId, [rule]);
    }
  }

  private rowToRule(row: any): TColumnRule | null {
    const columnId = toIdString(row?.column);
    if (!columnId) return null;
    const id = toIdString(row);
    if (!id) return null;
    return {
      id,
      ruleType: row.ruleType,
      value: row.value,
      message: row.message ?? null,
      isEnabled: row.isEnabled !== false,
      columnId,
    };
  }
}
