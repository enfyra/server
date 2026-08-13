import { Logger } from '../../../shared/logger';
import { getEffectiveMemoryBytes, type QueryBuilderService } from '@enfyra/kernel';
import type { EventEmitter2 } from 'eventemitter2';
import { CACHE_EVENTS } from '../../../shared/utils/cache-events.constants';
import type { TCacheInvalidationPayload } from '../../../shared/types/cache.types';
import type { RequestWithRouteData } from '../../../shared/types/dynamic-context.types';
import { loadCachedUserWithRoles } from '../../../shared/utils/load-user-with-role.util';
import { FileValidationHelper } from '../utils/file-validation.helper';
import type { AssetFileRecord, AssetPermissionRow } from '../types/file-asset.types';

const ASSET_FILE_CACHE_MAX_ENTRIES = 1_000;
const ASSET_PERMISSION_CACHE_MAX_ENTRIES = 1_000;
const ASSET_CACHE_MEMORY_PRESSURE_RATIO = Math.max(
  0,
  Number(process.env.ASSET_CACHE_MEMORY_PRESSURE_RATIO || 0.8),
);
const ASSET_CACHE_MIN_FREE_MEMORY_MB = Math.max(
  0,
  Number(process.env.ASSET_CACHE_MIN_FREE_MEMORY_MB || 256),
);
const ASSET_CACHE_PRESSURE_CLEAR_INTERVAL_MS = Math.max(
  1_000,
  Number(process.env.ASSET_CACHE_PRESSURE_CLEAR_INTERVAL_MS || 5_000),
);

export class FileAssetAccessService {
  private readonly logger = new Logger(FileAssetAccessService.name);
  private readonly queryBuilderService: QueryBuilderService;
  private readonly eventEmitter?: EventEmitter2;
  private readonly fileCache = new Map<string, any>();
  private readonly permissionsByFileCache = new Map<
    string,
    AssetPermissionRow[]
  >();
  private readonly permissionToFileIndex = new Map<string, string>();
  private readonly effectiveMemoryBytes = getEffectiveMemoryBytes();
  private lastMemoryPressureClearAt = 0;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    eventEmitter?: EventEmitter2;
  }) {
    this.queryBuilderService = deps.queryBuilderService;
    this.eventEmitter = deps.eventEmitter;
    const handleCacheInvalidation = this.handleCacheInvalidation.bind(this);
    this.eventEmitter?.on(CACHE_EVENTS.INVALIDATE, handleCacheInvalidation);
    this.eventEmitter?.on(
      CACHE_EVENTS.SYNC_INVALIDATE,
      handleCacheInvalidation,
    );
  }

  async resolveAuthorizedFile(
    req: RequestWithRouteData,
    fileId: string,
  ): Promise<AssetFileRecord | null> {
    const file = await this.getFileFromCache(fileId);
    if (!file) return null;

    if (!file.isPublic) {
      const currentUser = req.user || req.routeData?.context?.$user;
      const currentUserId = this.normalizeId(
        currentUser?.id ?? currentUser?._id,
      );
      const isRootAdmin = currentUser?.isRootAdmin === true;
      if (
        currentUserId &&
        !isRootAdmin &&
        (!req.user || !Array.isArray(req.user.roles))
      ) {
        req.user = await loadCachedUserWithRoles(
          this.queryBuilderService,
          undefined,
          currentUserId,
        );
      }

      if (!isRootAdmin) {
        const permissions = await this.getPermissionsForFile(fileId);

        for (const perm of permissions) {
          if (perm.roleId && !perm.role) {
            perm.role = await this.queryBuilderService.findOne({
              table: 'enfyra_role',
              where: { id: perm.roleId },
            });
          }
        }

        file.permissions = permissions;
      }
    }

    await FileValidationHelper.checkFilePermissions(file, req);
    return file;
  }

  getFileSize(file: any): number | undefined {
    const size = Number(file?.filesize);
    return Number.isSafeInteger(size) && size >= 0 ? size : undefined;
  }

  private cloneRow<T>(row: T): T {
    if (row === null || row === undefined || typeof row !== 'object')
      return row;
    return JSON.parse(JSON.stringify(row));
  }

  private normalizeId(id: unknown): string | null {
    if (id === null || id === undefined) return null;
    if (typeof id === 'string' && id.trim() === '') return null;
    if (
      typeof id === 'object' &&
      id !== null &&
      typeof (id as any).toString === 'function'
    ) {
      return (id as any).toString();
    }
    return String(id);
  }

  private getFileIdFromPermission(permission: any): string | null {
    return this.normalizeId(
      permission?.file?.id ??
        permission?.file?._id ??
        permission?.file ??
        permission?.fileId,
    );
  }

  private getPermissionId(permission: any): string | null {
    return this.normalizeId(permission?.id ?? permission?._id);
  }

  private getAndPromoteCacheEntry<T>(
    cache: Map<string, T>,
    key: string,
  ): T | null {
    const cached = cache.get(key);
    if (!cached) return null;
    cache.delete(key);
    cache.set(key, cached);
    return cached;
  }

  private async getFileFromCache(fileId: string): Promise<any | null> {
    const key = String(fileId);
    const cached = this.getAndPromoteCacheEntry(this.fileCache, key);
    if (cached) return this.cloneRow(cached);

    const fileResult = await this.queryBuilderService.find({
      table: 'enfyra_file',
      filter: { [this.queryBuilderService.getPkField()]: { _eq: fileId } },
      fields: ['*', 'storageConfig.*'],
    });

    const file = fileResult.data?.[0] ?? null;
    if (!file) return null;

    if (this.canAddAssetCacheEntry()) {
      this.fileCache.set(key, this.cloneRow(file));
      this.trimFileCache();
    }
    return this.cloneRow(file);
  }

  private async getPermissionsForFile(
    fileId: string,
  ): Promise<AssetPermissionRow[]> {
    const key = String(fileId);
    const cached = this.getAndPromoteCacheEntry(
      this.permissionsByFileCache,
      key,
    );
    if (cached) return this.cloneRow(cached);

    const idField = this.queryBuilderService.getPkField();
    const permissionsResult = await this.queryBuilderService.find({
      table: 'enfyra_file_permission',
      filter: {
        _and: [
          { isEnabled: { _eq: true } },
          { file: { [idField]: { _eq: fileId } } },
        ],
      },
      fields: [
        idField,
        'isEnabled',
        `file.${idField}`,
        `role.${idField}`,
        'role.name',
        `allowedUsers.${idField}`,
        'allowedUsers.email',
      ],
      limit: 1000,
    });

    const permissions = (permissionsResult.data || []).filter((perm: any) => {
      const permissionFileId = this.getFileIdFromPermission(perm);
      return String(permissionFileId) === String(fileId);
    });

    for (const perm of permissions) {
      const permissionId = this.getPermissionId(perm);
      if (permissionId) this.permissionToFileIndex.set(permissionId, key);
    }

    if (this.canAddAssetCacheEntry()) {
      this.permissionsByFileCache.set(key, this.cloneRow(permissions));
      this.trimPermissionCache();
    }
    return this.cloneRow(permissions);
  }

  private canAddAssetCacheEntry(): boolean {
    if (ASSET_CACHE_MEMORY_PRESSURE_RATIO <= 0) return true;
    const rss = process.memoryUsage().rss;
    const freeBytes = Math.max(0, this.effectiveMemoryBytes - rss);
    const minFreeBytes = ASSET_CACHE_MIN_FREE_MEMORY_MB * 1024 * 1024;
    const canAdd =
      rss / this.effectiveMemoryBytes < ASSET_CACHE_MEMORY_PRESSURE_RATIO &&
      freeBytes >= minFreeBytes;
    if (!canAdd) this.clearAssetCachesUnderMemoryPressure();
    return canAdd;
  }

  private clearAssetCachesUnderMemoryPressure(): void {
    const now = Date.now();
    if (
      now - this.lastMemoryPressureClearAt <
      ASSET_CACHE_PRESSURE_CLEAR_INTERVAL_MS
    ) {
      return;
    }
    this.lastMemoryPressureClearAt = now;
    this.fileCache.clear();
    this.permissionsByFileCache.clear();
    this.permissionToFileIndex.clear();
  }

  private trimFileCache(): void {
    while (this.fileCache.size > ASSET_FILE_CACHE_MAX_ENTRIES) {
      const oldestKey = this.fileCache.keys().next().value;
      if (oldestKey === undefined) return;
      this.fileCache.delete(oldestKey);
    }
  }

  private trimPermissionCache(): void {
    while (
      this.permissionsByFileCache.size > ASSET_PERMISSION_CACHE_MAX_ENTRIES
    ) {
      const oldestKey = this.permissionsByFileCache.keys().next().value;
      if (oldestKey === undefined) return;
      this.invalidatePermissionsForFile(oldestKey);
    }
  }

  private invalidateFile(fileId: string | number): void {
    const key = String(fileId);
    this.fileCache.delete(key);
    this.invalidatePermissionsForFile(key);
  }

  private invalidatePermissionsForFile(fileId: string | number): void {
    const key = String(fileId);
    const permissions = this.permissionsByFileCache.get(key) || [];
    for (const perm of permissions) {
      const permissionId = this.getPermissionId(perm);
      if (permissionId) this.permissionToFileIndex.delete(permissionId);
    }
    this.permissionsByFileCache.delete(key);
  }

  private async getPermissionFileIds(
    permissionIds: (string | number)[],
  ): Promise<Set<string>> {
    const fileIds = new Set<string>();

    for (const permissionId of permissionIds) {
      const cachedFileId = this.permissionToFileIndex.get(String(permissionId));
      if (cachedFileId) fileIds.add(cachedFileId);
    }

    if (permissionIds.length === 0) return fileIds;

    try {
      const result = await this.queryBuilderService.find({
        table: 'enfyra_file_permission',
        filter: {
          [this.queryBuilderService.getPkField()]: { _in: permissionIds },
        },
        fields: [
          this.queryBuilderService.getPkField(),
          `file.${this.queryBuilderService.getPkField()}`,
        ],
        limit: permissionIds.length,
      });

      for (const perm of result.data || []) {
        const fileId = this.getFileIdFromPermission(perm);
        if (fileId) fileIds.add(fileId);
      }
    } catch (error) {
      this.logger.warn(
        `Failed to resolve file permission cache keys: ${error instanceof Error ? error.message : String(error)}`,
      );
    }

    return fileIds;
  }

  private async handleCacheInvalidation(
    payload: TCacheInvalidationPayload,
  ): Promise<void> {
    if (payload.table === 'enfyra_file') {
      if (payload.scope === 'partial' && payload.ids?.length) {
        for (const id of payload.ids) this.invalidateFile(id);
      } else {
        this.fileCache.clear();
        this.permissionsByFileCache.clear();
        this.permissionToFileIndex.clear();
      }
      return;
    }

    if (payload.table === 'enfyra_file_permission') {
      if (payload.scope === 'partial' && payload.ids?.length) {
        const fileIds = await this.getPermissionFileIds(payload.ids);
        for (const fileId of fileIds) this.invalidatePermissionsForFile(fileId);
      } else {
        this.permissionsByFileCache.clear();
        this.permissionToFileIndex.clear();
      }
      return;
    }

    if (payload.table === 'enfyra_storage_config') {
      this.fileCache.clear();
      return;
    }

    if (payload.table === 'enfyra_role') {
      this.permissionsByFileCache.clear();
      this.permissionToFileIndex.clear();
    }
  }
}
