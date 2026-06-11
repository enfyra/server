import { Response } from 'express';
import * as fs from 'fs';
import * as path from 'path';
import { Logger } from '../../../shared/logger';
import { NotFoundException } from '../../../domain/exceptions';
import { getEffectiveMemoryBytes, QueryBuilderService } from '@enfyra/kernel';
import { FileManagementService } from './file-management.service';
import { StorageFactoryService } from '../storage/storage-factory.service';
import { ImageProcessorHelper } from '../utils/image-processor.helper';
import { StreamHelper } from '../utils/stream.helper';
import { FileValidationHelper } from '../utils/file-validation.helper';
import { ImageFormatHelper } from '../utils/image-format.helper';
import { FileSignatureHelper } from '../utils/file-signature.helper';
import { loadUserWithRole } from '../../../shared/utils/load-user-with-role.util';
import { EventEmitter2 } from 'eventemitter2';
import { CACHE_EVENTS } from '../../../shared/utils/cache-events.constants';
import type { TCacheInvalidationPayload } from '../../../shared/types/cache.types';

type AssetPermissionRow = Record<string, any>;
type HeicConvert = (options: {
  buffer: Buffer;
  format: 'JPEG' | 'PNG';
  quality?: number;
}) => Promise<ArrayBuffer | Uint8Array>;
type LocalFileSignature = {
  extension: string;
  mimetype: string;
} | null;
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
const heicConvert = require('heic-convert') as HeicConvert;

export class FileAssetsService {
  private readonly logger = new Logger(FileAssetsService.name);
  private readonly streamHelper: StreamHelper;
  private readonly queryBuilderService: QueryBuilderService;
  private readonly fileManagementService: FileManagementService;
  private readonly storageFactoryService: StorageFactoryService;
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
    fileManagementService: FileManagementService;
    storageFactoryService: StorageFactoryService;
    eventEmitter?: EventEmitter2;
  }) {
    this.queryBuilderService = deps.queryBuilderService;
    this.fileManagementService = deps.fileManagementService;
    this.storageFactoryService = deps.storageFactoryService;
    this.eventEmitter = deps.eventEmitter;
    this.streamHelper = new StreamHelper();
    ImageProcessorHelper.configureSharp();
    this.eventEmitter?.on(
      CACHE_EVENTS.INVALIDATE,
      this.handleCacheInvalidation.bind(this),
    );
  }

  private resolveLocalAssetPath(location: string): string {
    const basePath = path.resolve(process.cwd(), 'public');
    const relativePath = location.startsWith('/')
      ? location.slice(1)
      : location;
    const filePath = path.resolve(basePath, relativePath);
    if (
      filePath !== basePath &&
      !filePath.startsWith(`${basePath}${path.sep}`)
    ) {
      throw new NotFoundException('Physical file not found');
    }
    return filePath;
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

  private getFileSize(file: any): number | undefined {
    const size = Number(file?.filesize);
    return Number.isSafeInteger(size) && size >= 0 ? size : undefined;
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
      table: 'file_definition',
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
      table: 'file_permission_definition',
      filter: {
        _and: [
          { isEnabled: { _eq: true } },
          { file: { [idField]: { _eq: fileId } } },
        ],
      },
      fields: [
        'id',
        'isEnabled',
        'file.id',
        'role.id',
        'role.name',
        'allowedUsers.id',
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
        table: 'file_permission_definition',
        filter: {
          [this.queryBuilderService.getPkField()]: { _in: permissionIds },
        },
        fields: ['id', 'file.id'],
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
    if (payload.table === 'file_definition') {
      if (payload.scope === 'partial' && payload.ids?.length) {
        for (const id of payload.ids) this.invalidateFile(id);
      } else {
        this.fileCache.clear();
        this.permissionsByFileCache.clear();
        this.permissionToFileIndex.clear();
      }
      return;
    }

    if (payload.table === 'file_permission_definition') {
      if (payload.scope === 'partial' && payload.ids?.length) {
        const fileIds = await this.getPermissionFileIds(payload.ids);
        for (const fileId of fileIds) this.invalidatePermissionsForFile(fileId);
      } else {
        this.permissionsByFileCache.clear();
        this.permissionToFileIndex.clear();
      }
      return;
    }

    if (payload.table === 'storage_config_definition') {
      this.fileCache.clear();
      return;
    }

    if (payload.table === 'role_definition') {
      this.permissionsByFileCache.clear();
      this.permissionToFileIndex.clear();
    }
  }

  private async detectLocalFileSignature(
    location: string,
  ): Promise<LocalFileSignature> {
    try {
      const filePath = this.resolveLocalAssetPath(location);
      const handle = await fs.promises.open(filePath, 'r');
      try {
        const buffer = Buffer.alloc(32);
        const { bytesRead } = await handle.read(buffer, 0, buffer.length, 0);
        return FileSignatureHelper.detect(buffer.subarray(0, bytesRead));
      } finally {
        await handle.close();
      }
    } catch {
      return null;
    }
  }

  private isHeicMimeType(mimetype: string): boolean {
    return mimetype === 'image/heic' || mimetype === 'image/heif';
  }

  private async processHeicInline(
    filePath: string,
    req: any,
    res: Response,
    filename: string,
  ): Promise<void> {
    try {
      const query = req.routeData?.context?.$query || req.query;
      const requestedFormat = (
        query.format as string | undefined
      )?.toLowerCase();
      const outputFormat = requestedFormat || 'jpeg';
      const quality = query.quality
        ? parseInt(query.quality as string, 10)
        : undefined;
      const width = query.width
        ? parseInt(query.width as string, 10)
        : undefined;
      const height = query.height
        ? parseInt(query.height as string, 10)
        : undefined;
      const fit = query.fit as string;
      const gravity = query.gravity as string;

      const validation = ImageProcessorHelper.validateImageParams(
        width,
        height,
        quality,
      );
      if (!validation.valid) {
        return void res.status(400).json({ error: validation.error });
      }

      const fitValidation = ImageProcessorHelper.validateFit(fit);
      if (!fitValidation.valid) {
        return void res.status(400).json({ error: fitValidation.error });
      }

      const gravityValidation = ImageProcessorHelper.validateGravity(gravity);
      if (!gravityValidation.valid) {
        return void res.status(400).json({ error: gravityValidation.error });
      }

      const formatValidation =
        ImageProcessorHelper.validateFormat(outputFormat);
      if (!formatValidation.valid) {
        return void res.status(400).json({ error: formatValidation.error });
      }

      const input = await fs.promises.readFile(filePath);
      const converted = await heicConvert({
        buffer: input,
        format: outputFormat === 'png' ? 'PNG' : 'JPEG',
        quality: quality ? quality / 100 : 0.85,
      });
      const decoded = Buffer.from(
        converted instanceof ArrayBuffer
          ? new Uint8Array(converted)
          : converted,
      );

      let output: Buffer<ArrayBufferLike> = decoded;
      if (
        outputFormat !== 'jpeg' ||
        width ||
        height ||
        fit ||
        gravity ||
        quality
      ) {
        let processor = ImageProcessorHelper.createProcessor(decoded);
        processor = ImageProcessorHelper.applyResize(
          processor,
          width,
          height,
          fit,
          gravity,
        );
        processor = ImageProcessorHelper.setImageFormat(
          processor,
          outputFormat,
          quality,
        );
        output = await processor.toBuffer();
      }

      const outFormat = outputFormat === 'jpg' ? 'jpeg' : outputFormat;
      const outFilename = ImageFormatHelper.updateFilenameWithFormat(
        filename,
        outFormat,
      );

      res.setHeader('Content-Type', ImageFormatHelper.getMimeType(outFormat));
      res.setHeader('Content-Length', output.length);
      res.setHeader('Content-Disposition', `inline; filename="${outFilename}"`);
      res.setHeader('Cache-Control', 'public, max-age=86400');
      res.end(output);
    } catch (error) {
      this.logger.error('HEIC image conversion error:', error);
      if (!res.headersSent)
        res.status(415).json({ error: 'HEIC image conversion failed' });
    }
  }

  async streamFile(req: any, res: Response): Promise<void> {
    const fileId = req.routeData?.params?.id || req.params.id;
    if (!fileId)
      return void res.status(400).json({ error: 'File ID is required' });

    const file = await this.getFileFromCache(fileId);
    if (!file) throw new NotFoundException(`File not found: ${fileId}`);

    if (!file.isPublished) {
      const currentUser = req.user || req.routeData?.context?.$user;
      const currentUserId = this.normalizeId(
        currentUser?.id ?? currentUser?._id,
      );
      const isRootAdmin = currentUser?.isRootAdmin === true;
      if (
        currentUserId &&
        !isRootAdmin &&
        (!req.user || (!req.user.role && !req.user.roleId))
      ) {
        req.user = await loadUserWithRole(
          this.queryBuilderService,
          currentUserId,
        );
      }

      if (!isRootAdmin) {
        const permissions = await this.getPermissionsForFile(fileId);

        for (const perm of permissions) {
          if (perm.roleId && !perm.role) {
            perm.role = await this.queryBuilderService.findOne({
              table: 'role_definition',
              where: { id: perm.roleId },
            });
          }
        }

        file.permissions = permissions;
      }
    }

    await FileValidationHelper.checkFilePermissions(file, req);

    const {
      location,
      storageConfig,
      filename,
      mimetype,
      type: fileType,
    } = file as any;
    const storageType = storageConfig?.type || 'Local Storage';
    const storageConfigId = storageConfig?._id || storageConfig?.id || null;
    const rangeHeader = req.headers?.range as string | undefined;
    const totalSize = this.getFileSize(file);

    if (
      storageType === 'Google Cloud Storage' ||
      storageType === 'Cloudflare R2' ||
      storageType === 'Amazon S3'
    ) {
      if (
        FileValidationHelper.isImageFile(mimetype, fileType) &&
        FileValidationHelper.hasImageQueryParams(req)
      ) {
        return void (await this.processImageWithQuery(
          location,
          req,
          res,
          filename,
          storageConfigId,
        ));
      }

      const query = req.routeData?.context?.$query || req.query;
      const shouldDownload =
        query.download === 'true' || query.download === true;
      const parsedRange = this.streamHelper.parseHttpRange(
        rangeHeader,
        totalSize,
      );
      if (parsedRange.type === 'invalid') {
        return void this.streamHelper.sendRangeNotSatisfiable(res, totalSize);
      }
      const stream = await this.fileManagementService.getStreamFromStorage(
        location,
        storageConfigId,
        parsedRange.type === 'partial'
          ? { range: parsedRange.range }
          : undefined,
      );
      return void (await this.streamHelper.streamCloudFile(
        stream,
        res,
        filename,
        mimetype,
        shouldDownload,
        parsedRange.type === 'partial' ? parsedRange.range : undefined,
        totalSize,
      ));
    }

    if (storageType === 'Local Storage') {
      const query = req.routeData?.context?.$query || req.query;
      const shouldDownload =
        query.download === 'true' || query.download === true;
      const actualSignature = await this.detectLocalFileSignature(location);
      const actualMimeType = actualSignature?.mimetype || mimetype;
      const actualFilename = actualSignature
        ? FileSignatureHelper.replaceExtension(
            filename,
            actualSignature.extension,
          )
        : filename;
      if (this.isHeicMimeType(actualMimeType) && !shouldDownload) {
        return void (await this.processHeicInline(
          this.resolveLocalAssetPath(location),
          req,
          res,
          actualFilename,
        ));
      }

      if (
        FileValidationHelper.isImageFile(mimetype, fileType) &&
        FileValidationHelper.hasImageQueryParams(req)
      ) {
        const filePath = this.resolveLocalAssetPath(location);

        return void (await this.processImageWithQuery(
          filePath,
          req,
          res,
          filename,
          storageConfigId,
        ));
      }

      const storageService =
        this.storageFactoryService.getStorageService('Local Storage');
      let sc: any;

      if (storageConfigId) {
        sc =
          await this.fileManagementService.getStorageConfigById(
            storageConfigId,
          );
      } else {
        sc = {
          type: 'Local Storage',
          name: 'Local',
          isEnabled: true,
        };
      }

      let stream;
      const parsedRange = this.streamHelper.parseHttpRange(
        rangeHeader,
        totalSize,
      );
      if (parsedRange.type === 'invalid') {
        return void this.streamHelper.sendRangeNotSatisfiable(res, totalSize);
      }
      try {
        stream = await storageService.getStream(
          location,
          sc,
          parsedRange.type === 'partial'
            ? { range: parsedRange.range }
            : undefined,
        );
      } catch (error) {
        this.logger.error(`Local file not found: ${location}`, error);
        throw new NotFoundException('Physical file not found');
      }
      return void (await this.streamHelper.streamCloudFile(
        stream,
        res,
        actualFilename,
        actualMimeType,
        shouldDownload,
        parsedRange.type === 'partial' ? parsedRange.range : undefined,
        totalSize,
      ));
    }

    const filePath = this.fileManagementService.getFilePath(
      path.basename(location),
    );

    if (!(await FileValidationHelper.fileExists(filePath))) {
      this.logger.error(`File not found: ${filePath}`);
      throw new NotFoundException('Physical file not found');
    }

    if (
      FileValidationHelper.isImageFile(mimetype, fileType) &&
      FileValidationHelper.hasImageQueryParams(req)
    ) {
      return void (await this.processImageWithQuery(
        filePath,
        req,
        res,
        filename,
      ));
    }

    const query = req.routeData?.context?.$query || req.query;
    const shouldDownload = query.download === 'true' || query.download === true;
    await this.streamHelper.streamRegularFile(
      filePath,
      res,
      filename,
      mimetype,
      shouldDownload,
      rangeHeader,
    );
  }

  private async processImageWithQuery(
    filePath: string,
    req: any,
    res: Response,
    filename: string,
    storageConfigId?: number | string,
  ): Promise<void> {
    try {
      const query = req.routeData?.context?.$query || req.query;
      const format = query.format as string;
      const width = query.width
        ? parseInt(query.width as string, 10)
        : undefined;
      const height = query.height
        ? parseInt(query.height as string, 10)
        : undefined;
      const quality = query.quality
        ? parseInt(query.quality as string, 10)
        : undefined;
      const cache = query.cache
        ? parseInt(query.cache as string, 10)
        : undefined;
      const shouldDownload =
        query.download === 'true' || query.download === true;
      const fit = query.fit as string;
      const gravity = query.gravity as string;
      const rotate = query.rotate
        ? parseInt(query.rotate as string, 10)
        : undefined;
      const flip = query.flip as string;
      const blur = query.blur ? parseFloat(query.blur as string) : undefined;
      const sharpen = query.sharpen
        ? parseFloat(query.sharpen as string)
        : undefined;
      const brightness = query.brightness
        ? parseInt(query.brightness as string, 10)
        : undefined;
      const contrast = query.contrast
        ? parseInt(query.contrast as string, 10)
        : undefined;
      const saturation = query.saturation
        ? parseInt(query.saturation as string, 10)
        : undefined;
      const grayscale = query.grayscale === 'true' || query.grayscale === true;

      const validation = ImageProcessorHelper.validateImageParams(
        width,
        height,
        quality,
      );
      if (!validation.valid) {
        return void res.status(400).json({ error: validation.error });
      }

      const fitValidation = ImageProcessorHelper.validateFit(fit);
      if (!fitValidation.valid) {
        return void res.status(400).json({ error: fitValidation.error });
      }

      const gravityValidation = ImageProcessorHelper.validateGravity(gravity);
      if (!gravityValidation.valid) {
        return void res.status(400).json({ error: gravityValidation.error });
      }

      const transformValidation = ImageProcessorHelper.validateTransformParams(
        rotate,
        flip,
        blur,
        sharpen,
        brightness,
        contrast,
        saturation,
      );
      if (!transformValidation.valid) {
        return void res.status(400).json({ error: transformValidation.error });
      }

      let shouldStream = false;
      if (storageConfigId) {
        const config =
          await this.fileManagementService.getStorageConfigById(
            storageConfigId,
          );
        shouldStream =
          config.type === 'Google Cloud Storage' ||
          config.type === 'Cloudflare R2' ||
          config.type === 'Amazon S3';
      }

      if (shouldStream) {
        const finalFormat =
          format || ImageFormatHelper.getOriginalFormat(filePath);
        const finalMimeType = ImageFormatHelper.getMimeType(finalFormat);
        return void (await this.streamImageFromCloud(
          filePath,
          storageConfigId!,
          req,
          res,
          filename,
          format,
          width,
          height,
          quality,
          cache,
          finalMimeType,
          shouldDownload,
          fit,
          gravity,
          rotate,
          flip,
          blur,
          sharpen,
          brightness,
          contrast,
          saturation,
          grayscale,
        ));
      }

      const fileStream = fs.createReadStream(filePath);

      let imageProcessor = ImageProcessorHelper.createStreamProcessor();

      imageProcessor = ImageProcessorHelper.applyResize(
        imageProcessor,
        width,
        height,
        fit,
        gravity,
      );
      imageProcessor = ImageProcessorHelper.applyTransformations(
        imageProcessor,
        rotate,
        flip,
        blur,
        sharpen,
      );
      imageProcessor = ImageProcessorHelper.applyEffects(
        imageProcessor,
        brightness,
        contrast,
        saturation,
        grayscale,
      );

      let outFilename = filename;

      if (format) {
        const formatValidation = ImageProcessorHelper.validateFormat(format);
        if (!formatValidation.valid) {
          return void res.status(400).json({ error: formatValidation.error });
        }
        const formatLower = format.toLowerCase();
        if (formatLower === 'avif' && quality !== undefined) {
          imageProcessor = ImageProcessorHelper.setImageFormat(
            imageProcessor,
            formatLower,
            undefined,
          );
        } else {
          imageProcessor = ImageProcessorHelper.setImageFormat(
            imageProcessor,
            formatLower,
            quality,
          );
        }
        outFilename = ImageFormatHelper.updateFilenameWithFormat(
          outFilename,
          format,
        );
      } else if (quality) {
        const originalFormat = ImageFormatHelper.getOriginalFormat(filePath);
        if (originalFormat === 'avif') {
          imageProcessor = ImageProcessorHelper.setImageFormat(
            imageProcessor,
            originalFormat,
            undefined,
          );
        } else {
          imageProcessor = ImageProcessorHelper.setImageFormat(
            imageProcessor,
            originalFormat,
            quality,
          );
        }
      }

      const finalFormat =
        format || ImageFormatHelper.getOriginalFormat(filePath);
      const finalMimeType = ImageFormatHelper.getMimeType(finalFormat);

      res.setHeader('Content-Type', finalMimeType);
      res.setHeader(
        'Content-Disposition',
        shouldDownload
          ? `attachment; filename="${outFilename}"`
          : `inline; filename="${outFilename}"`,
      );

      if (cache && cache > 0)
        res.setHeader('Cache-Control', `public, max-age=${cache}`);
      else if (format) res.setHeader('Cache-Control', 'public, max-age=86400');
      else res.setHeader('Cache-Control', 'public, max-age=31536000');

      const sharpStream = fileStream.pipe(imageProcessor);

      sharpStream.on('error', (error: any) => {
        this.logger.error('Sharp processing error:', error);
        if (!res.headersSent)
          res.status(500).json({ error: 'Image processing failed' });
      });

      this.streamHelper.setupImageStream(sharpStream, res, false);

      this.streamHelper.handleStreamError(
        fileStream,
        res,
        'Failed to stream from local storage',
      );
    } catch (error) {
      this.logger.error('Image processing error:', error);
      if (!res.headersSent)
        res.status(500).json({
          error: 'Image processing failed',
          details: error instanceof Error ? error.message : String(error),
        });
    }
  }

  private async streamImageFromCloud(
    filePath: string,
    storageConfigId: number | string,
    req: any,
    res: Response,
    filename: string,
    format?: string,
    width?: number,
    height?: number,
    quality?: number,
    cache?: number,
    mimeType?: string,
    shouldDownload?: boolean,
    fit?: string,
    gravity?: string,
    rotate?: number,
    flip?: string,
    blur?: number,
    sharpen?: number,
    brightness?: number,
    contrast?: number,
    saturation?: number,
    grayscale?: boolean,
  ): Promise<void> {
    try {
      const cloudStream = await this.fileManagementService.getStreamFromStorage(
        filePath,
        storageConfigId,
      );

      let imageProcessor = ImageProcessorHelper.createStreamProcessor();

      imageProcessor = ImageProcessorHelper.applyResize(
        imageProcessor,
        width,
        height,
        fit,
        gravity,
      );
      imageProcessor = ImageProcessorHelper.applyTransformations(
        imageProcessor,
        rotate,
        flip,
        blur,
        sharpen,
      );
      imageProcessor = ImageProcessorHelper.applyEffects(
        imageProcessor,
        brightness,
        contrast,
        saturation,
        grayscale,
      );

      let outFilename = filename;

      if (format) {
        const formatValidation = ImageProcessorHelper.validateFormat(format);
        if (!formatValidation.valid) {
          return void res.status(400).json({ error: formatValidation.error });
        }
        const formatLower = format.toLowerCase();
        if (formatLower === 'avif' && quality !== undefined) {
          imageProcessor = ImageProcessorHelper.setImageFormat(
            imageProcessor,
            formatLower,
            undefined,
          );
        } else {
          imageProcessor = ImageProcessorHelper.setImageFormat(
            imageProcessor,
            formatLower,
            quality,
          );
        }
        outFilename = ImageFormatHelper.updateFilenameWithFormat(
          outFilename,
          format,
        );
      } else if (quality) {
        const originalFormat = ImageFormatHelper.getOriginalFormat(filePath);
        if (originalFormat === 'avif') {
          imageProcessor = ImageProcessorHelper.setImageFormat(
            imageProcessor,
            originalFormat,
            undefined,
          );
        } else {
          imageProcessor = ImageProcessorHelper.setImageFormat(
            imageProcessor,
            originalFormat,
            quality,
          );
        }
      }

      const finalFormat =
        format || ImageFormatHelper.getOriginalFormat(filePath);
      const finalMimeType = ImageFormatHelper.getMimeType(finalFormat);

      res.setHeader('Content-Type', finalMimeType);
      res.setHeader(
        'Content-Disposition',
        shouldDownload
          ? `attachment; filename="${outFilename}"`
          : `inline; filename="${outFilename}"`,
      );

      if (cache && cache > 0)
        res.setHeader('Cache-Control', `public, max-age=${cache}`);
      else if (format) res.setHeader('Cache-Control', 'public, max-age=86400');
      else res.setHeader('Cache-Control', 'public, max-age=31536000');

      const sharpStream = cloudStream.pipe(imageProcessor);

      sharpStream.on('error', (error: any) => {
        this.logger.error('Sharp processing error:', error);
        if (!res.headersSent)
          res.status(500).json({ error: 'Image processing failed' });
      });

      this.streamHelper.setupImageStream(sharpStream, res, false);

      this.streamHelper.handleStreamError(
        cloudStream,
        res,
        'Failed to stream from cloud storage',
      );
    } catch (error) {
      this.logger.error('Stream image from cloud error:', error);
      if (!res.headersSent)
        res.status(500).json({ error: 'Image streaming failed' });
    }
  }
}
