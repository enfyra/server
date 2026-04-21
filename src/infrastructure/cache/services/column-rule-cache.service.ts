import { EventEmitter2 } from 'eventemitter2';
import { QueryBuilderService } from '../../query-builder/query-builder.service';
import {
  CACHE_IDENTIFIERS,
  TCacheInvalidationPayload,
} from '../../../shared/utils/cache-events.constants';
import { BaseCacheService } from './base-cache.service';

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

export class ColumnRuleCacheService extends BaseCacheService<
  Map<string, TColumnRule[]>
> {
  private readonly queryBuilderService: QueryBuilderService;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    eventEmitter: EventEmitter2;
  }) {
    super(
      {
        cacheIdentifier: CACHE_IDENTIFIERS.COLUMN_RULE,
        colorCode: '\x1b[38;5;183m',
        cacheName: 'ColumnRuleCache',
      },
      deps.eventEmitter,
    );
    this.queryBuilderService = deps.queryBuilderService;
  }

  protected async loadFromDb(): Promise<any> {
    const result = await this.queryBuilderService.find({
      table: 'column_rule_definition',
      fields: ['*', 'column.id', 'column._id'],
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

  supportsPartialReload(): boolean {
    return true;
  }

  protected async applyPartialUpdate(
    payload: TCacheInvalidationPayload,
  ): Promise<void> {
    if (!this.cache) {
      throw new Error('Cache not initialized, cannot partial reload');
    }
    if (payload.table !== 'column_rule_definition') {
      throw new Error(
        'partial reload by non-column_rule_definition payload unsupported',
      );
    }
    const ids = (payload.ids ?? []).map(String);
    if (ids.length === 0) return;

    const result = await this.queryBuilderService.find({
      table: 'column_rule_definition',
      fields: ['*', 'column.id', 'column._id'],
      filter: { id: { _in: ids } },
      limit: ids.length,
    });
    const rows: any[] = result?.data ?? [];

    const fetchedById = new Map<string, any>();
    for (const row of rows) {
      const rid = toIdString(row);
      if (rid) fetchedById.set(rid, row);
    }

    for (const id of ids) {
      this.removeRuleFromAllBuckets(id);
      const row = fetchedById.get(id);
      if (!row) continue;
      const rule = this.rowToRule(row);
      if (!rule || !rule.isEnabled) continue;
      const bucket = this.cache.get(rule.columnId);
      if (bucket) bucket.push(rule);
      else this.cache.set(rule.columnId, [rule]);
    }
  }

  private removeRuleFromAllBuckets(ruleId: string): void {
    for (const [columnId, rules] of this.cache.entries()) {
      const idx = rules.findIndex((r) => String(r.id) === ruleId);
      if (idx === -1) continue;
      rules.splice(idx, 1);
      if (rules.length === 0) this.cache.delete(columnId);
    }
  }

  private rowToRule(row: any): TColumnRule | null {
    const columnId = toIdString(row?.column);
    if (!columnId) return null;
    return {
      id: row.id,
      ruleType: row.ruleType,
      value: row.value,
      message: row.message ?? null,
      isEnabled: row.isEnabled !== false,
      columnId,
    };
  }

  async getRulesForColumn(columnId: string | number): Promise<TColumnRule[]> {
    await this.ensureLoaded();
    const key = String(columnId);
    return this.cache.get(key) ?? [];
  }

  getRulesForColumnSync(columnId: string | number): TColumnRule[] {
    if (!this.cacheLoaded) return [];
    const key = String(columnId);
    return this.cache.get(key) ?? [];
  }
}
