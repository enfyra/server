import { EventEmitter2 } from 'eventemitter2';
import { QueryBuilderService } from '../../../kernel/query';
import {
  CACHE_IDENTIFIERS,
  TCacheInvalidationPayload,
} from '../../../shared/utils/cache-events.constants';
import { BaseCacheService } from './base-cache.service';
import { MetadataCacheService } from './metadata-cache.service';
import { RedisRuntimeCacheStore } from './redis-runtime-cache-store.service';

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

type TFieldPermissionMetadataIndex = {
  columnsById: Map<string, { tableName: string; columnName: string }>;
  relationsById: Map<
    string,
    { tableName: string; relationPropertyName: string }
  >;
};

const FIELD_PERMISSION_CACHE_FIELDS = [
  'id',
  'isEnabled',
  'action',
  'effect',
  'condition',
  'role.id',
  'allowedUsers.id',
  'column.id',
  'relation.id',
];

function toIdString(v: any): string | null {
  if (v === undefined || v === null) return null;
  return String(v?._id ?? v?.id ?? v);
}

function isFalseValue(v: any): boolean {
  return v === false || v === 0 || v === '0';
}

export class FieldPermissionCacheService extends BaseCacheService<
  Map<string, TCompiledFieldPolicy>
> {
  private readonly queryBuilderService: QueryBuilderService;
  private readonly metadataCacheService: MetadataCacheService;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    metadataCacheService: MetadataCacheService;
    eventEmitter: EventEmitter2;
    redisRuntimeCacheStore?: RedisRuntimeCacheStore;
  }) {
    super(
      {
        cacheIdentifier: CACHE_IDENTIFIERS.FIELD_PERMISSION,
        colorCode: '\x1b[38;5;215m',
        cacheName: 'FieldPermissionCache',
      },
      deps.eventEmitter,
      deps.redisRuntimeCacheStore,
    );
    this.queryBuilderService = deps.queryBuilderService;
    this.metadataCacheService = deps.metadataCacheService;
  }

  protected async loadFromDb(): Promise<any> {
    const [result, metadata] = await Promise.all([
      this.queryBuilderService.find({
        table: 'field_permission_definition',
        fields: FIELD_PERMISSION_CACHE_FIELDS,
        filter: { isEnabled: { _eq: true } },
        limit: 100000,
      }),
      this.metadataCacheService.getMetadata(),
    ]);
    return { rows: result?.data ?? [], metadata };
  }

  protected transformData(rawData: any): Map<string, TCompiledFieldPolicy> {
    const map = new Map<string, TCompiledFieldPolicy>();
    const rows: any[] = Array.isArray(rawData) ? rawData : rawData?.rows ?? [];
    const metadataIndex = this.buildMetadataIndex(rawData?.metadata);
    for (const row of rows) {
      const rule = this.rowToRule(row, metadataIndex);
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
      fields: FIELD_PERMISSION_CACHE_FIELDS,
      filter: { id: { _in: ids } },
      limit: ids.length,
    });
    const rows: any[] = result?.data ?? [];

    const fetchedById = new Map<string, any>();
    for (const row of rows) {
      const rid = toIdString(row);
      if (rid) fetchedById.set(rid, row);
    }

    const metadataIndex = this.buildMetadataIndex(
      await this.metadataCacheService.getMetadata(),
    );
    for (const id of ids) {
      this.removeRuleFromAllBuckets(id);
      const row = fetchedById.get(id);
      if (!row) continue;
      const rule = this.rowToRule(row, metadataIndex);
      if (!rule || !rule.isEnabled) continue;
      this.upsertRuleIntoMap(this.cache, rule);
    }
  }

  private buildMetadataIndex(metadata: any): TFieldPermissionMetadataIndex {
    const columnsById = new Map<
      string,
      { tableName: string; columnName: string }
    >();
    const relationsById = new Map<
      string,
      { tableName: string; relationPropertyName: string }
    >();

    for (const table of metadata?.tablesList ?? []) {
      if (!table?.name) continue;

      for (const column of table.columns ?? []) {
        const columnId = toIdString(column);
        if (!columnId || !column?.name) continue;
        columnsById.set(columnId, {
          tableName: table.name,
          columnName: column.name,
        });
      }

      for (const relation of table.relations ?? []) {
        const relationId = toIdString(relation);
        if (!relationId || !relation?.propertyName) continue;
        relationsById.set(relationId, {
          tableName: table.name,
          relationPropertyName: relation.propertyName,
        });
      }
    }

    return { columnsById, relationsById };
  }

  private resolveColumnInfo(
    row: any,
    metadataIndex: TFieldPermissionMetadataIndex,
  ): { tableName: string; columnName: string } | null {
    const columnId = toIdString(row?.column) ?? toIdString(row?.columnId);
    const indexed = columnId ? metadataIndex.columnsById.get(columnId) : null;
    if (indexed) return indexed;

    const tableName = row?.column?.table?.name;
    const columnName = row?.column?.name;
    if (tableName && columnName) return { tableName, columnName };
    return null;
  }

  private resolveRelationInfo(
    row: any,
    metadataIndex: TFieldPermissionMetadataIndex,
  ): { tableName: string; relationPropertyName: string } | null {
    const relationId = toIdString(row?.relation) ?? toIdString(row?.relationId);
    const indexed = relationId
      ? metadataIndex.relationsById.get(relationId)
      : null;
    if (indexed) return indexed;

    const tableName = row?.relation?.sourceTable?.name;
    const relationPropertyName = row?.relation?.propertyName;
    if (tableName && relationPropertyName) {
      return { tableName, relationPropertyName };
    }
    return null;
  }

  private rowToRule(
    row: any,
    metadataIndex: TFieldPermissionMetadataIndex,
  ): TFieldPermissionRule | null {
    const action = (row?.action as TFieldPermissionAction) || 'read';
    const effect = (row?.effect as TFieldPermissionEffect) || 'allow';
    const columnInfo = this.resolveColumnInfo(row, metadataIndex);
    const relationInfo = this.resolveRelationInfo(row, metadataIndex);
    const tableName = columnInfo?.tableName ?? relationInfo?.tableName ?? null;
    if (!tableName) return null;
    const roleId = toIdString(row?.role) ?? toIdString(row?.roleId);
    const allowedUserIds = Array.isArray(row?.allowedUsers)
      ? row.allowedUsers
          .map((u: any) => toIdString(u))
          .filter((x: any): x is string => !!x)
      : [];
    return {
      id: row?.id ?? row?._id,
      isEnabled: !isFalseValue(row?.isEnabled),
      action,
      effect,
      tableName,
      roleId,
      allowedUserIds,
      columnName: columnInfo?.columnName ?? null,
      relationPropertyName: relationInfo?.relationPropertyName ?? null,
      condition: row?.condition ?? null,
    };
  }

  private bucketKeysForRule(rule: TFieldPermissionRule): string[] {
    if (rule.allowedUserIds.length > 0) {
      const keys = new Set<string>();
      for (const userId of rule.allowedUserIds) {
        keys.add(`u:${userId}|${rule.tableName}|${rule.action}`);
      }
      return [...keys];
    }

    return [`r:${rule.roleId ?? 'null'}|${rule.tableName}|${rule.action}`];
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
    for (const key of this.bucketKeysForRule(rule)) {
      let bucket = map.get(key);
      if (!bucket) {
        bucket = this.emptyPolicy();
        map.set(key, bucket);
      }
      bucket.rules.push(rule);
      this.indexRuleIntoBucket(bucket, rule);
    }
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
    const cache = await this.getCacheAsync();
    const policies: TCompiledFieldPolicy[] = [];

    const userId = toIdString(user);
    const roleId = toIdString(user?.role);

    if (userId) {
      const userKey = `u:${userId}|${tableName}|${action}`;
      if (cache.has(userKey)) policies.push(cache.get(userKey)!);
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
