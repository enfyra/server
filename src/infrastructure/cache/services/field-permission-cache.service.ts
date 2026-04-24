import { EventEmitter2 } from 'eventemitter2';
import { QueryBuilderService } from '../../query-builder/query-builder.service';
import {
  CACHE_IDENTIFIERS,
  TCacheInvalidationPayload,
} from '../../../shared/utils/cache-events.constants';
import { BaseCacheService } from './base-cache.service';

export type TFieldPermissionAction = 'read' | 'create' | 'update';
export type TFieldPermissionEffect = 'allow' | 'deny';

export type TFieldPermissionRule = {
  id: string | number;
  isEnabled: boolean;
  action: TFieldPermissionAction;
  effect: TFieldPermissionEffect;
  tableName: string;
  roleId: string | null;
  allowedUserIds: string[];
  columnName: string | null;
  relationPropertyName: string | null;
  condition: any | null;
};

export type TCompiledFieldPolicy = {
  unconditionalAllowedColumns: Set<string>;
  unconditionalAllowedRelations: Set<string>;
  unconditionalDeniedColumns: Set<string>;
  unconditionalDeniedRelations: Set<string>;
  rules: TFieldPermissionRule[];
};

function toIdString(v: any): string | null {
  if (v === undefined || v === null) return null;
  return String(v?._id ?? v?.id ?? v);
}

export class FieldPermissionCacheService extends BaseCacheService<
  Map<string, TCompiledFieldPolicy>
> {
  private readonly queryBuilderService: QueryBuilderService;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    eventEmitter: EventEmitter2;
  }) {
    super(
      {
        cacheIdentifier: CACHE_IDENTIFIERS.FIELD_PERMISSION,
        colorCode: '\x1b[38;5;215m',
        cacheName: 'FieldPermissionCache',
      },
      deps.eventEmitter,
    );
    this.queryBuilderService = deps.queryBuilderService;
  }

  protected async loadFromDb(): Promise<any> {
    const result = await this.queryBuilderService.find({
      table: 'field_permission_definition',
      fields: [
        '*',
        'role.*',
        'allowedUsers.*',
        'column.*',
        'column.table.*',
        'relation.*',
        'relation.sourceTable.*',
      ],
      filter: { isEnabled: { _eq: true } },
      limit: 100000,
    });
    return result?.data ?? [];
  }

  protected transformData(rawData: any): Map<string, TCompiledFieldPolicy> {
    const map = new Map<string, TCompiledFieldPolicy>();
    const rows: any[] = Array.isArray(rawData) ? rawData : [];
    for (const row of rows) {
      const rule = this.rowToRule(row);
      if (!rule) continue;
      this.upsertRuleIntoMap(map, rule);
    }
    return map;
  }

  supportsPartialReload(): boolean {
    return true;
  }

  async partialReload(
    payload: TCacheInvalidationPayload,
    publish = true,
  ): Promise<void> {
    if (payload.table !== 'field_permission_definition') {
      await this.reload(publish);
      return;
    }
    await super.partialReload(payload, publish);
  }

  protected async applyPartialUpdate(
    payload: TCacheInvalidationPayload,
  ): Promise<void> {
    if (!this.cache) {
      throw new Error('Cache not initialized, cannot partial reload');
    }
    const ids = (payload.ids ?? []).map(String);
    if (ids.length === 0) return;

    const result = await this.queryBuilderService.find({
      table: 'field_permission_definition',
      fields: [
        '*',
        'role.*',
        'allowedUsers.*',
        'column.*',
        'column.table.*',
        'relation.*',
        'relation.sourceTable.*',
      ],
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
      this.upsertRuleIntoMap(this.cache, rule);
    }
  }

  private rowToRule(row: any): TFieldPermissionRule | null {
    const action = (row?.action as TFieldPermissionAction) || 'read';
    const effect = (row?.effect as TFieldPermissionEffect) || 'allow';
    const tableName =
      row?.column?.table?.name || row?.relation?.sourceTable?.name || null;
    if (!tableName) return null;
    const roleId = toIdString(row?.role);
    const allowedUserIds = Array.isArray(row?.allowedUsers)
      ? row.allowedUsers
          .map((u: any) => toIdString(u))
          .filter((x: any): x is string => !!x)
      : [];
    return {
      id: row?.id,
      isEnabled: row?.isEnabled !== false,
      action,
      effect,
      tableName,
      roleId,
      allowedUserIds,
      columnName: row?.column?.name ?? null,
      relationPropertyName: row?.relation?.propertyName ?? null,
      condition: row?.condition ?? null,
    };
  }

  private bucketKeyForRule(rule: TFieldPermissionRule): string {
    const subjectsKey =
      rule.allowedUserIds.length > 0
        ? `u:${rule.allowedUserIds.join(',')}`
        : `r:${rule.roleId ?? 'null'}`;
    return `${subjectsKey}|${rule.tableName}|${rule.action}`;
  }

  private emptyPolicy(): TCompiledFieldPolicy {
    return {
      unconditionalAllowedColumns: new Set<string>(),
      unconditionalAllowedRelations: new Set<string>(),
      unconditionalDeniedColumns: new Set<string>(),
      unconditionalDeniedRelations: new Set<string>(),
      rules: [],
    };
  }

  private upsertRuleIntoMap(
    map: Map<string, TCompiledFieldPolicy>,
    rule: TFieldPermissionRule,
  ): void {
    const key = this.bucketKeyForRule(rule);
    let bucket = map.get(key);
    if (!bucket) {
      bucket = this.emptyPolicy();
      map.set(key, bucket);
    }
    bucket.rules.push(rule);
    this.indexRuleIntoBucket(bucket, rule);
  }

  private indexRuleIntoBucket(
    bucket: TCompiledFieldPolicy,
    rule: TFieldPermissionRule,
  ): void {
    if (rule.condition != null) return;
    if (rule.effect === 'allow') {
      if (rule.columnName)
        bucket.unconditionalAllowedColumns.add(rule.columnName);
      if (rule.relationPropertyName)
        bucket.unconditionalAllowedRelations.add(rule.relationPropertyName);
    } else {
      if (rule.columnName)
        bucket.unconditionalDeniedColumns.add(rule.columnName);
      if (rule.relationPropertyName)
        bucket.unconditionalDeniedRelations.add(rule.relationPropertyName);
    }
  }

  private rebuildBucketIndexes(bucket: TCompiledFieldPolicy): void {
    bucket.unconditionalAllowedColumns.clear();
    bucket.unconditionalAllowedRelations.clear();
    bucket.unconditionalDeniedColumns.clear();
    bucket.unconditionalDeniedRelations.clear();
    for (const r of bucket.rules) this.indexRuleIntoBucket(bucket, r);
  }

  private removeRuleFromAllBuckets(ruleId: string): void {
    for (const [key, bucket] of this.cache.entries()) {
      const idx = bucket.rules.findIndex((r) => String(r.id) === ruleId);
      if (idx === -1) continue;
      bucket.rules.splice(idx, 1);
      if (bucket.rules.length === 0) {
        this.cache.delete(key);
      } else {
        this.rebuildBucketIndexes(bucket);
      }
    }
  }

  protected getLogCount(): string {
    return `${this.cache?.size ?? 0} buckets`;
  }

  async ensureLoaded(): Promise<void> {
    await super.ensureLoaded();
  }

  async getPoliciesFor(
    user: any,
    tableName: string,
    action: TFieldPermissionAction,
  ): Promise<TCompiledFieldPolicy[]> {
    await this.ensureLoaded();
    const cache = this.cache || new Map();
    const policies: TCompiledFieldPolicy[] = [];

    const userId = toIdString(user);
    const roleId = toIdString(user?.role);

    if (userId) {
      for (const [key, policy] of cache.entries()) {
        if (!key.includes(`|${tableName}|${action}`)) continue;
        if (!key.startsWith('u:')) continue;
        const idsPart = key.split('|')[0]?.slice(2) || '';
        const ids = idsPart.split(',').filter(Boolean);
        if (ids.includes(userId)) policies.push(policy);
      }
    }

    const roleKey = `r:${roleId ?? 'null'}|${tableName}|${action}`;
    if (cache.has(roleKey)) policies.push(cache.get(roleKey)!);

    if (roleId != null) {
      const catchAllKey = `r:null|${tableName}|${action}`;
      if (cache.has(catchAllKey)) policies.push(cache.get(catchAllKey)!);
    }

    return policies;
  }
}
