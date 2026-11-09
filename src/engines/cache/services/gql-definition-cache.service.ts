import type { TCacheInvalidationPayload } from '../../../shared/types/cache.types';
import { EventEmitter2 } from 'eventemitter2';
import { BaseCacheService } from './base-cache.service';
import { QueryBuilderService } from '@enfyra/kernel';
import { RedisRuntimeCacheStore } from './redis-runtime-cache-store.service';
import {
  CACHE_IDENTIFIERS,
  isMetadataTable,
} from '../../../shared/utils/cache-events.constants';
import type { TGqlDefinition } from '../types/cache-data.types';
import {
  assertGraphqlPermissionScope,
  assertNoPublicPermissionOverlap,
  normalizeGraphqlOperationList,
} from '../../../modules/graphql/utils/graphql-access.util';

const GRAPHQL_DEFINITION_CACHE_FIELDS = [
  'id',
  'isEnabled',
  'isSystem',
  'description',
  'metadata',
  'table.id',
  'table.name',
  'publicOperations.name',
  'permissions.id',
  'permissions.isEnabled',
  'permissions.role.id',
  'permissions.allowedUsers.id',
  'permissions.operations.name',
];

function toIdString(value: any): string | null {
  if (value === undefined || value === null) return null;
  return String(value?._id ?? value?.id ?? value);
}

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

  override supportsPartialReload(): boolean {
    return true;
  }

  protected override async applyPartialUpdate(
    payload: TCacheInvalidationPayload,
  ): Promise<void | false> {
    const ids = payload.ids ?? [];
    if (!ids.length) return;
    if (
      payload.table === 'enfyra_route' ||
      payload.table === 'enfyra_field_permission'
    )
      return false;
    let filter: Record<string, unknown>;
    const changed = new Set(
      payload.table === 'enfyra_graphql' ? ids.map(String) : [],
    );
    const names = new Set(payload.affectedTables ?? []);
    for (const rename of payload.tableRenames ?? []) {
      names.add(rename.oldName);
      names.add(rename.newName);
    }
    if (payload.table === 'enfyra_graphql') filter = { id: { _in: ids } };
    else if (
      ['enfyra_table', 'enfyra_column', 'enfyra_relation'].includes(
        payload.table,
      )
    ) {
      filter = {
        _or: [
          { table: { id: { _in: ids } } },
          ...(names.size ? [{ table: { name: { _in: [...names] } } }] : []),
        ],
      };
      if ([...this.cache.values()].some((definition) => !definition.tableId)) {
        await this.reloadFromPartial();
        return;
      }
      const tableIds = new Set(ids.map(String));
      for (const [name, definition] of this.cache)
        if (
          names.has(name) ||
          (definition.tableId && tableIds.has(definition.tableId))
        )
          changed.add(definition.id);
    } else if (payload.table === 'enfyra_graphql_permission') {
      if (
        [...this.cache.values()].some((definition) => !definition.permissionIds)
      ) {
        await this.reloadFromPartial();
        return;
      }
      const permissionIds = new Set(ids.map(String));
      for (const definition of this.cache.values())
        if (definition.permissionIds?.some((id) => permissionIds.has(id)))
          changed.add(definition.id);
      const result = await this.queryBuilderService.find({
        table: 'enfyra_graphql_permission',
        fields: ['graphql.id'],
        filter: { id: { _in: ids } },
        limit: -1,
      });
      for (const row of result.data) {
        const id = toIdString(row.graphql);
        if (id) changed.add(id);
      }
      if (!changed.size) return;
      filter = { id: { _in: [...changed] } };
    } else {
      await this.reloadFromPartial();
      return;
    }
    const rows = await this.loadDefinitions(filter);
    const updates = this.transformData(rows);
    const next = new Map(
      [...this.cache].filter(
        ([name, definition]) =>
          !changed.has(definition.id) && !updates.has(name) && !names.has(name),
      ),
    );
    for (const [name, definition] of updates) next.set(name, definition);
    this.cache = next;
  }

  private async loadDefinitions(
    filter?: Record<string, unknown>,
  ): Promise<any[]> {
    const result = await this.queryBuilderService.find({
      table: 'enfyra_graphql',
      fields: GRAPHQL_DEFINITION_CACHE_FIELDS,
      ...(filter ? { filter } : {}),
      deep: {
        publicOperations: { limit: -1 },
        permissions: {
          limit: -1,
          deep: { allowedUsers: { limit: -1 }, operations: { limit: -1 } },
        },
      },
      limit: -1,
    });
    return result.data ?? [];
  }

  protected async loadFromDb(): Promise<any> {
    try {
      return await this.loadDefinitions();
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

      const publicOperations = normalizeGraphqlOperationList(
        row.publicOperations,
      );
      const permissions = (
        Array.isArray(row.permissions) ? row.permissions : []
      ).map((permission: any) => {
        assertGraphqlPermissionScope({
          role: permission.role,
          allowedUsers: permission.allowedUsers,
        });
        const operations = normalizeGraphqlOperationList(permission.operations);
        if (operations.length === 0) {
          throw new Error(
            'GraphQL permission must grant at least one operation',
          );
        }
        assertNoPublicPermissionOverlap({
          publicOperations,
          permissionOperations: operations,
        });
        return {
          isEnabled: permission.isEnabled !== false,
          roleId: toIdString(permission.role),
          allowedUserIds: (permission.allowedUsers ?? [])
            .map(toIdString)
            .filter((id: string | null): id is string => id !== null),
          operations,
        };
      });

      map.set(tableName, {
        id: toIdString(row) ?? '',
        ...(toIdString(row.table) ? { tableId: toIdString(row.table)! } : {}),
        permissionIds: (row.permissions ?? [])
          .map(toIdString)
          .filter((id: string | null): id is string => id !== null),
        isEnabled: !!row.isEnabled,
        isSystem: !!row.isSystem,
        description: row.description ?? null,
        metadata: row.metadata ?? null,
        tableName,
        publicOperations,
        permissions,
      });
    }

    return map;
  }

  protected override getLogCount(): string {
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
