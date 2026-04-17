import { Injectable } from '@nestjs/common';
import { EventEmitter2 } from '@nestjs/event-emitter';
import { QueryBuilderService } from '../../query-builder/query-builder.service';
import { CACHE_IDENTIFIERS } from '../../../shared/utils/cache-events.constants';
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

@Injectable()
export class FieldPermissionCacheService extends BaseCacheService<
  Map<string, TCompiledFieldPolicy>
> {
  constructor(
    private readonly queryBuilder: QueryBuilderService,
    eventEmitter: EventEmitter2,
  ) {
    super(
      {
        cacheIdentifier: CACHE_IDENTIFIERS.FIELD_PERMISSION,
        colorCode: '\x1b[38;5;215m',
        cacheName: 'FieldPermissionCache',
      },
      eventEmitter,
    );
  }

  protected async loadFromDb(): Promise<any> {
    const result = await this.queryBuilder.find({
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
      const action = (row?.action as TFieldPermissionAction) || 'read';
      const effect = (row?.effect as TFieldPermissionEffect) || 'allow';

      const tableName =
        row?.column?.table?.name || row?.relation?.sourceTable?.name || null;
      if (!tableName) continue;

      const roleId = toIdString(row?.role);
      const allowedUserIds = Array.isArray(row?.allowedUsers)
        ? row.allowedUsers
            .map((u: any) => toIdString(u))
            .filter((x: any): x is string => !!x)
        : [];

      const columnName = row?.column?.name ?? null;
      const relationPropertyName = row?.relation?.propertyName ?? null;

      const rule: TFieldPermissionRule = {
        id: row?.id,
        isEnabled: row?.isEnabled !== false,
        action,
        effect,
        tableName,
        roleId,
        allowedUserIds,
        columnName,
        relationPropertyName,
        condition: row?.condition ?? null,
      };

      const subjectsKey = (() => {
        if (allowedUserIds.length > 0) return `u:${allowedUserIds.join(',')}`;
        return `r:${roleId ?? 'null'}`;
      })();

      const key = `${subjectsKey}|${tableName}|${action}`;
      if (!map.has(key)) {
        map.set(key, {
          unconditionalAllowedColumns: new Set<string>(),
          unconditionalAllowedRelations: new Set<string>(),
          unconditionalDeniedColumns: new Set<string>(),
          unconditionalDeniedRelations: new Set<string>(),
          rules: [],
        });
      }

      const bucket = map.get(key)!;
      bucket.rules.push(rule);

      if (rule.condition != null) continue;
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

    return map;
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

    return policies;
  }
}
