import { EventEmitter2 } from 'eventemitter2';
import { QueryBuilderService } from '../../query-builder/query-builder.service';
import { CACHE_IDENTIFIERS } from '../../../shared/utils/cache-events.constants';
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
      const columnId = toIdString(row?.column);
      if (!columnId) continue;

      const rule: TColumnRule = {
        id: row.id,
        ruleType: row.ruleType,
        value: row.value,
        message: row.message ?? null,
        isEnabled: row.isEnabled !== false,
        columnId,
      };

      const existing = map.get(columnId);
      if (existing) existing.push(rule);
      else map.set(columnId, [rule]);
    }

    return map;
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
