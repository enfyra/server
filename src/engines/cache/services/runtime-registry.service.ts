import { EventEmitter2 } from 'eventemitter2';
import { Logger } from '../../../shared/logger';
import { getErrorMessage } from '../../../shared/utils/error.util';
import {
  CACHE_EVENTS,
  CACHE_IDENTIFIERS,
  isMetadataTable,
} from '../../../shared/utils/cache-events.constants';
import {
  DEFAULT_MAX_QUERY_DEPTH,
  DEFAULT_MAX_REQUEST_BODY_SIZE_MB,
  DEFAULT_MAX_UPLOAD_FILE_SIZE_MB,
} from '../../../shared/utils/constant';
import type { EnfyraMetadata } from './metadata-cache.service';
import type {
  GuardCache,
  GuardNode,
  GuardPosition,
} from './guard-cache-builder.service';
import type { OAuthConfig } from './oauth-config-cache-builder.service';
import type { FlowDefinition } from '../../../shared/types/flow.types';
import type {
  TCompiledFieldPolicy,
  TFieldPermissionAction,
} from './field-permission-cache-builder.service';
import type { TColumnRule } from './column-rule-cache-builder.service';
import type {
  WebSocketEvent,
  WebSocketGateway,
} from './websocket-cache-builder.service';
import type {
  FolderNode,
  FolderTreeCache,
  RuntimeCacheIdentifier,
  RuntimeRegistryEntry,
  RuntimeRegistrySnapshot,
  SettingData,
  TGqlDefinition,
} from '../types/runtime-registry.types';

export interface RuntimeCacheViewSource {
  getCacheAsync?: () => Promise<unknown>;
  getMetadata?: () => Promise<unknown>;
  getRawCache?: () => unknown;
}

export class RuntimeRegistryService {
  private readonly logger = new Logger(RuntimeRegistryService.name);
  private readonly eventEmitter?: EventEmitter2;
  private readonly entries = new Map<
    RuntimeCacheIdentifier,
    RuntimeRegistryEntry
  >();
  private readonly publishStates = new Map<
    RuntimeCacheIdentifier,
    RuntimeRegistryEntry
  >();
  private initialized = false;

  constructor(deps: { eventEmitter?: EventEmitter2; lazyRef?: unknown } = {}) {
    this.eventEmitter = deps.eventEmitter;
  }

  init(): void {
    if (this.initialized) return;
    this.initialized = true;
  }

  async publishFromCache(
    identifier: RuntimeCacheIdentifier,
    service: RuntimeCacheViewSource,
  ): Promise<RuntimeRegistrySnapshot> {
    const snapshot = await this.stageSnapshotFromCache(identifier, service);
    this.activateSnapshots([snapshot]);
    return snapshot;
  }

  async stageSnapshotFromCache(
    identifier: RuntimeCacheIdentifier,
    service: RuntimeCacheViewSource,
  ): Promise<RuntimeRegistrySnapshot> {
    const nextVersion = (this.entries.get(identifier)?.version ?? 0) + 1;
    this.publishStates.set(identifier, {
      identifier,
      version: nextVersion,
      status: 'building',
    });

    try {
      const data =
        typeof service.getCacheAsync === 'function'
          ? await service.getCacheAsync()
          : typeof service.getMetadata === 'function'
            ? await service.getMetadata()
            : service.getRawCache?.();
      if (data === undefined) {
        throw new Error(`Cache ${identifier} did not return active data`);
      }
      const snapshotData = this.cloneRuntimeData(data);
      const activatedAt = new Date().toISOString();
      return {
        identifier,
        version: nextVersion,
        activatedAt,
        data: snapshotData,
      };
    } catch (error) {
      const message = getErrorMessage(error);
      this.publishStates.set(identifier, {
        identifier,
        version: nextVersion,
        status: 'failed',
        failedAt: new Date().toISOString(),
        error: message,
      });
      this.logger.error(
        `Failed to publish runtime cache ${identifier}: ${message}`,
      );
      throw error;
    }
  }

  activateSnapshots(snapshots: RuntimeRegistrySnapshot[]): void {
    const activatedAt = new Date().toISOString();
    for (const snapshot of snapshots) {
      const entry: RuntimeRegistryEntry = {
        identifier: snapshot.identifier,
        version: snapshot.version,
        status: 'activated',
        activatedAt,
        data: snapshot.data,
      };
      this.entries.set(snapshot.identifier, entry);
      this.publishStates.set(snapshot.identifier, entry);
    }

    for (const snapshot of snapshots) {
      this.eventEmitter?.emit(CACHE_EVENTS.RUNTIME_CACHE_ACTIVATED, {
        identifier: snapshot.identifier,
        version: snapshot.version,
        activatedAt,
      });
    }
  }

  getSnapshot<T = unknown>(
    identifier: RuntimeCacheIdentifier,
  ): RuntimeRegistrySnapshot<T> | undefined {
    const entry = this.entries.get(identifier);
    if (!entry || entry.status !== 'activated' || entry.data === undefined) {
      return undefined;
    }
    return {
      identifier,
      version: entry.version,
      activatedAt: entry.activatedAt!,
      data: entry.data as T,
    };
  }

  getActiveData<T = unknown>(
    identifier: RuntimeCacheIdentifier,
  ): T | undefined {
    return this.getSnapshot<T>(identifier)?.data;
  }

  requireActiveData<T = unknown>(identifier: RuntimeCacheIdentifier): T {
    const data = this.getActiveData<T>(identifier);
    if (data === undefined) {
      throw new Error(`Runtime cache ${identifier} is not activated`);
    }
    return data;
  }

  getMetadata(): EnfyraMetadata | undefined {
    return this.getActiveData<EnfyraMetadata>(CACHE_IDENTIFIERS.METADATA);
  }

  requireMetadata(): EnfyraMetadata {
    return this.requireActiveData<EnfyraMetadata>(CACHE_IDENTIFIERS.METADATA);
  }

  getTableMetadata(tableName: string): any | null {
    return this.getMetadata()?.tables.get(tableName) ?? null;
  }

  requireTableMetadata(tableName: string): any {
    const table = this.getTableMetadata(tableName);
    if (!table) {
      throw new Error(`Runtime metadata table ${tableName} is not activated`);
    }
    return table;
  }

  getAllTablesMetadata(): any[] {
    return this.getMetadata()?.tablesList ?? [];
  }

  getRoutes(): any[] {
    return (
      this.getActiveData<{ routes: any[] }>(CACHE_IDENTIFIERS.ROUTE)?.routes ??
      []
    );
  }

  requireRoutes(): any[] {
    return this.requireActiveData<{ routes: any[] }>(CACHE_IDENTIFIERS.ROUTE)
      .routes;
  }

  getGuardsForRoute(
    position: GuardPosition,
    routePath: string,
    method: string,
  ): GuardNode[] {
    const cache = this.requireActiveData<GuardCache>(CACHE_IDENTIFIERS.GUARD);
    const globalGuards =
      position === 'pre_auth' ? cache.preAuthGlobal : cache.postAuthGlobal;
    const routeMap =
      position === 'pre_auth' ? cache.preAuthByRoute : cache.postAuthByRoute;
    const routeGuards = routeMap.get(routePath) || [];

    const all = [...globalGuards, ...routeGuards];
    return all.filter((guard) => {
      if (guard.methods.length === 0) return true;
      return guard.methods.includes(method);
    });
  }

  getSettings(): SettingData | undefined {
    return this.getActiveData<SettingData>(CACHE_IDENTIFIERS.SETTING);
  }

  requireSettings(): SettingData {
    return this.requireActiveData<SettingData>(CACHE_IDENTIFIERS.SETTING);
  }

  getMaxQueryDepth(): number {
    return this.getSettings()?.maxQueryDepth ?? DEFAULT_MAX_QUERY_DEPTH;
  }

  getMaxUploadFileSizeBytes(): number {
    return (
      (this.getSettings()?.maxUploadFileSize ??
        DEFAULT_MAX_UPLOAD_FILE_SIZE_MB) *
      1024 *
      1024
    );
  }

  getMaxRequestBodySizeBytes(): number {
    return (
      (this.getSettings()?.maxRequestBodySize ??
        DEFAULT_MAX_REQUEST_BODY_SIZE_MB) *
      1024 *
      1024
    );
  }

  getSetting<T = any>(key: string): T | undefined {
    return this.getSettings()?.[key];
  }

  getStorageConfigById(id: unknown): any | null {
    const cache = this.requireActiveData<Map<string | number, any>>(
      CACHE_IDENTIFIERS.STORAGE,
    );
    if (id === null || id === undefined || id === '') return null;

    const candidates: Array<string | number> = [];
    if (typeof id === 'number' || typeof id === 'string') {
      candidates.push(id);
    }
    if (typeof id === 'number') {
      candidates.push(String(id));
    } else if (typeof id === 'string' && !Number.isNaN(Number(id))) {
      candidates.push(Number(id));
    } else if (
      typeof id === 'object' &&
      id !== null &&
      typeof (id as any).toString === 'function'
    ) {
      candidates.push((id as any).toString());
    }

    for (const candidate of candidates) {
      const config = cache.get(candidate);
      if (config) return config;
    }
    return null;
  }

  getStorageConfigByType(type: string): any | null {
    const cache = this.requireActiveData<Map<string | number, any>>(
      CACHE_IDENTIFIERS.STORAGE,
    );
    for (const config of cache.values()) {
      if (config.type === type && config.isEnabled) return config;
    }
    return null;
  }

  getOauthConfigByProvider(provider: string): OAuthConfig | null {
    const cache = this.requireActiveData<Map<string, OAuthConfig>>(
      CACHE_IDENTIFIERS.OAUTH_CONFIG,
    );
    return cache.get(provider) || null;
  }

  getOauthProviders(): string[] {
    const cache = this.requireActiveData<Map<string, OAuthConfig>>(
      CACHE_IDENTIFIERS.OAUTH_CONFIG,
    );
    return Array.from(cache.keys());
  }

  getFlows(): FlowDefinition[] {
    return this.requireActiveData<FlowDefinition[]>(CACHE_IDENTIFIERS.FLOW);
  }

  getFlowById(id: number | string | undefined | null): FlowDefinition | null {
    if (id === undefined || id === null || id === '') return null;
    const idStr = String(id);
    return (
      this.getFlows().find(
        (flow) =>
          flow.id === id || flow.id === Number(id) || String(flow.id) === idStr,
      ) || null
    );
  }

  getFlowByName(name: string | undefined | null): FlowDefinition | null {
    if (!name) return null;
    return this.getFlows().find((flow) => flow.name === name) || null;
  }

  getFlowsByTriggerType(triggerType: string): FlowDefinition[] {
    return this.getFlows().filter((flow) => flow.triggerType === triggerType);
  }

  getWebsocketGateways(): WebSocketGateway[] {
    return this.requireActiveData<WebSocketGateway[]>(
      CACHE_IDENTIFIERS.WEBSOCKET,
    );
  }

  getWebsocketGatewayByPath(path: string): WebSocketGateway | null {
    return (
      this.getWebsocketGateways().find((gateway) => gateway.path === path) ||
      null
    );
  }

  getWebsocketEventsByGatewayId(gatewayId: number | string): WebSocketEvent[] {
    const gateway = this.getWebsocketGateways().find(
      (candidate) => String(candidate.id) === String(gatewayId),
    );
    return gateway?.events || [];
  }

  getColumnRulesForColumn(columnId: string | number): TColumnRule[] {
    const cache = this.requireActiveData<Map<string, TColumnRule[]>>(
      CACHE_IDENTIFIERS.COLUMN_RULE,
    );
    return cache.get(String(columnId)) ?? [];
  }

  getFieldPermissionPoliciesFor(
    user: any,
    tableName: string,
    action: TFieldPermissionAction,
  ): TCompiledFieldPolicy[] {
    const cache = this.requireActiveData<Map<string, TCompiledFieldPolicy>>(
      CACHE_IDENTIFIERS.FIELD_PERMISSION,
    );
    const policies: TCompiledFieldPolicy[] = [];

    const userId = this.toIdString(user);
    const roleId = this.toIdString(user?.role);

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

  getPackages(): string[] {
    return this.requireActiveData<string[]>(CACHE_IDENTIFIERS.PACKAGE);
  }

  getGraphqlDefinitions(): Map<string, TGqlDefinition> {
    return this.requireActiveData<Map<string, TGqlDefinition>>(
      CACHE_IDENTIFIERS.GRAPHQL,
    );
  }

  getGraphqlDefinitionForTable(tableName: string): TGqlDefinition | undefined {
    return this.getGraphqlDefinitions().get(tableName);
  }

  isGraphqlEnabledForTable(tableName: string): boolean {
    if (isMetadataTable(tableName)) return false;
    return !!this.getGraphqlDefinitionForTable(tableName)?.isEnabled;
  }

  getAllEnabledGraphqlDefinitions(): TGqlDefinition[] {
    return Array.from(this.getGraphqlDefinitions().values()).filter(
      (definition) =>
        definition.isEnabled && !isMetadataTable(definition.tableName),
    );
  }

  getFolderTreeCache(): FolderTreeCache {
    return this.requireActiveData<FolderTreeCache>(
      CACHE_IDENTIFIERS.FOLDER_TREE,
    );
  }

  private toIdString(value: any): string | null {
    if (value === undefined || value === null) return null;
    return String(value?._id ?? value?.id ?? value);
  }

  getFolderTree(): FolderNode[] {
    return this.getFolderTreeCache().tree;
  }

  getFolders(): Map<string, FolderNode> {
    return this.getFolderTreeCache().folders;
  }

  isCircularFolderParent(
    folderId: string | null,
    newParentId: string | null,
  ): boolean {
    if (!folderId) return false;
    if (!newParentId) return false;

    const cache = this.getFolderTreeCache();
    const visited = new Set<string>();
    let currentId: string | null = newParentId;

    while (currentId) {
      if (currentId === folderId) return true;
      if (visited.has(currentId)) break;

      visited.add(currentId);
      const folder = cache.folders.get(currentId);
      currentId = folder?.parentId ?? null;
    }

    return false;
  }

  lookupTableByName(tableName: string): any | null {
    return this.getTableMetadata(tableName);
  }

  lookupTableById(tableId: number | string): any | null {
    return (
      this.getMetadata()?.tablesList.find(
        (table) => table.id === tableId || table.id === Number(tableId),
      ) ?? null
    );
  }

  getEntry(
    identifier: RuntimeCacheIdentifier,
  ): RuntimeRegistryEntry | undefined {
    const entry =
      this.publishStates.get(identifier) ?? this.entries.get(identifier);
    return entry ? { ...entry } : undefined;
  }

  private cloneRuntimeData<T>(data: T): T {
    try {
      return structuredClone(data);
    } catch {
      return this.cloneValue(data, new WeakMap()) as T;
    }
  }

  private cloneValue(value: unknown, seen: WeakMap<object, unknown>): unknown {
    if (value === null || typeof value !== 'object') return value;
    const existing = seen.get(value);
    if (existing) return existing;

    if (value instanceof Date) {
      return new Date(value.getTime());
    }

    if (value instanceof Map) {
      const clone = new Map();
      seen.set(value, clone);
      for (const [key, item] of value.entries()) {
        clone.set(this.cloneValue(key, seen), this.cloneValue(item, seen));
      }
      return clone;
    }

    if (value instanceof Set) {
      const clone = new Set();
      seen.set(value, clone);
      for (const item of value.values()) {
        clone.add(this.cloneValue(item, seen));
      }
      return clone;
    }

    if (Array.isArray(value)) {
      const clone: unknown[] = [];
      seen.set(value, clone);
      for (const item of value) {
        clone.push(this.cloneValue(item, seen));
      }
      return clone;
    }

    const clone: Record<string, unknown> = {};
    seen.set(value, clone);
    for (const [key, item] of Object.entries(value)) {
      clone[key] = this.cloneValue(item, seen);
    }
    return clone;
  }
}
