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
      const entry: RuntimeRegistryEntry = {
        identifier,
        version: nextVersion,
        status: 'activated',
        activatedAt,
        data: snapshotData,
      };
      this.entries.set(identifier, entry);
      this.publishStates.set(identifier, entry);
      this.eventEmitter?.emit(CACHE_EVENTS.RUNTIME_CACHE_ACTIVATED, {
        identifier,
        version: nextVersion,
        activatedAt,
      });
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
