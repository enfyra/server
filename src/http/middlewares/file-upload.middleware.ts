import { Response, NextFunction } from 'express';
import multer from 'multer';
import os from 'os';
import crypto from 'crypto';
import { unlink } from 'node:fs/promises';
import { BadRequestException } from '../../domain/exceptions';
import type { MultipartUploadConfig, UploadedFileFields } from '../../shared/types/multipart-upload.types';
import type { UploadedFileInfo } from '../../shared/types/file-management.types';
import type { RuntimeRegistryService } from '../../engines/cache/services/runtime-registry.service';
import type { DynamicWebSocketGateway } from '../../modules/websocket/gateway/dynamic-websocket.gateway';
import type { FileUploadProgressEvent } from '../../shared/types';

export const UPLOAD_PROGRESS_EVENT = '$system:upload:progress';
const UPLOAD_PROGRESS_EMIT_INTERVAL_MS = 2_000;

export function resolveUploadFileSizeLimitBytes(
  globalLimitBytes: number,
  routeMaxUploadFileSizeMb?: unknown,
): number {
  const routeLimitMb =
    typeof routeMaxUploadFileSizeMb === 'number'
      ? routeMaxUploadFileSizeMb
      : typeof routeMaxUploadFileSizeMb === 'string'
        ? Number(routeMaxUploadFileSizeMb)
        : null;

  if (routeLimitMb && Number.isFinite(routeLimitMb) && routeLimitMb > 0) {
    return Math.floor(routeLimitMb * 1024 * 1024);
  }

  return globalLimitBytes;
}

export function resolveMultipartUploadConfig(routeData: any): MultipartUploadConfig | null {
  const operation = routeData?.routeMethodConfig;
  if (operation?.requestBodyType !== 'multipart') return null;
  const configured = Array.isArray(operation.fileFields) ? operation.fileFields : [];
  const fields: MultipartUploadConfig['fields'] = configured.length ? configured.map((field: any) => ({
    name: String(field.name ?? '').trim(),
    required: field.required === true,
    maxCount: field.maxCount ?? 1,
    maxFileSize: field.maxFileSize,
    allowedMimeTypes: field.allowedMimeTypes,
  })) : [{ name: 'file', maxCount: 1 }];
  if (fields.some(field => !field.name || !Number.isSafeInteger(field.maxCount) || field.maxCount! < 1
    || (field.maxFileSize != null && (!Number.isSafeInteger(field.maxFileSize) || field.maxFileSize < 1))
    || (field.allowedMimeTypes != null && (!Array.isArray(field.allowedMimeTypes)
      || field.allowedMimeTypes.some(type => typeof type !== 'string' || !type.trim()))))
    || new Set(fields.map(field => field.name)).size !== fields.length) {
    throw new BadRequestException('Invalid multipart file-field configuration');
  }
  const maxFiles = operation.maxFiles ?? fields.reduce((total: number, field) => total + field.maxCount!, 0);
  if (!Number.isSafeInteger(maxFiles) || maxFiles < 1
    || (operation.maxUploadFileSize != null && (!Number.isSafeInteger(operation.maxUploadFileSize) || operation.maxUploadFileSize < 1))) {
    throw new BadRequestException('Invalid multipart upload limits');
  }
  return { fields, maxFiles, maxUploadFileSize: operation.maxUploadFileSize };
}

export function validateMultipartFiles(files: Record<string, Express.Multer.File[]>, config: MultipartUploadConfig): void {
  for (const field of config.fields) {
    const uploaded = files[field.name] ?? [];
    if (field.required && uploaded.length === 0) {
      throw new BadRequestException(`Missing required multipart file field: ${field.name}`);
    }
    if (field.maxFileSize != null && uploaded.some(file => file.size > field.maxFileSize! * 1024 * 1024)) {
      throw new BadRequestException(`Multipart file field ${field.name} exceeds its file size limit`);
    }
    if (field.allowedMimeTypes?.length && uploaded.some(file => !field.allowedMimeTypes!.includes(file.mimetype))) {
      throw new BadRequestException(`Multipart file field ${field.name} has an unsupported MIME type`);
    }
  }
}

function toUploadedFileInfo(file: Express.Multer.File): UploadedFileInfo {
  return {
    originalname: file.originalname,
    mimetype: file.mimetype,
    encoding: file.encoding || 'utf8',
    path: file.path,
    size: file.size,
    fieldname: file.fieldname,
  };
}

async function cleanupRejectedFiles(files: Express.Multer.File[]): Promise<void> {
  await Promise.all(files.map(file => file.path ? unlink(file.path).catch(() => {}) : Promise.resolve()));
}

const diskStorage = multer.diskStorage({
  destination: (_req, _file, cb) => cb(null, os.tmpdir()),
  filename: (_req, _file, cb) =>
    cb(null, `enfyra-upload-${crypto.randomUUID()}`),
});

export function fileUploadMiddleware(
  runtimeRegistryService: RuntimeRegistryService,
  dynamicWebSocketGateway?: DynamicWebSocketGateway,
) {
  return async (req: any, res: Response, next: NextFunction) => {
    const isPostOrPatch = ['POST', 'PATCH', 'PUT'].includes(req.method);
    const isMultipartContent = req.headers['content-type']?.includes(
      'multipart/form-data',
    );
    if (!isPostOrPatch || !isMultipartContent) {
      return next();
    }
    let multipartConfig: MultipartUploadConfig | null;
    try {
      multipartConfig = resolveMultipartUploadConfig(req.routeData);
    } catch (error) {
      return next(error);
    }
    const isBuiltInFileRoute = req.routeData?.path === '/enfyra_file' || req.routeData?.path === '/enfyra_file/:id';
    if (!multipartConfig && !isBuiltInFileRoute) {
      return next(new BadRequestException('Multipart is not enabled for this route method'));
    }
    if (isBuiltInFileRoute) multipartConfig = null;
    setupUploadProgress(req, dynamicWebSocketGateway);
    const fileSizeLimitBytes = Math.min(
      runtimeRegistryService.getMaxUploadFileSizeBytes(),
      resolveUploadFileSizeLimitBytes(
        runtimeRegistryService.getMaxUploadFileSizeBytes(),
        req.routeData?.maxUploadFileSize,
      ),
      multipartConfig?.maxUploadFileSize
        ? multipartConfig.maxUploadFileSize * 1024 * 1024
        : Number.POSITIVE_INFINITY,
    );
    req.uploadFileSizeLimitBytes = fileSizeLimitBytes;
    const upload = multer({
      storage: diskStorage,
      limits: {
        fileSize: fileSizeLimitBytes,
        files: multipartConfig?.maxFiles ?? 1,
      },
    });
    const parse = multipartConfig && !isBuiltInFileRoute
      ? upload.fields(multipartConfig.fields.map(field => ({ name: field.name, maxCount: field.maxCount ?? 1 })))
      : upload.single('file');
    parse(req, res, async (error: any) => {
      const grouped = (req.files ?? {}) as Record<string, Express.Multer.File[]>;
      const parsedFiles = Object.values(grouped).flat();
      if (error || multipartConfig) {
        try {
          if (error) {
            if (error.code === 'LIMIT_UNEXPECTED_FILE') {
              throw new BadRequestException(`Multipart file field is not configured or exceeds its count: ${error.field ?? 'unknown'}`);
            }
            throw error;
          }
          validateMultipartFiles(grouped, multipartConfig!);
        } catch (failure) {
          await cleanupRejectedFiles(parsedFiles);
          emitUploadProgress(req, dynamicWebSocketGateway, {
            phase: 'failed', loaded: req.uploadProgressLoaded || 0,
            total: req.uploadProgressTotal || 0, percent: 0,
          });
          return next(failure);
        }
      }
      const receivedFiles = [...parsedFiles, ...(req.file ? [req.file] : [])];
      if (receivedFiles.length) {
        res.once('close', () => { void cleanupRejectedFiles(receivedFiles); });
      }
      for (const file of receivedFiles) {
        if (!file.originalname) continue;
        try {
          let fixedName = file.originalname;
          if (detectEncodingCorruption(fixedName)) {
            const utf8Fixed = Buffer.from(fixedName, 'latin1').toString('utf8');
            if (isValidVietnameseString(utf8Fixed)) fixedName = utf8Fixed;
          }
          file.originalname = fixCharacterCorruptions(fixedName);
        } catch (error) {
          console.warn('Failed to fix filename encoding:', error);
        }
      }
      if (req.routeData?.context) {
        const processedBody: any = { ...req.body };
        for (const field of multipartConfig?.fields ?? [{ name: 'file' }]) {
          delete processedBody[field.name];
        }
        if (
          processedBody.folder &&
          processedBody.folder !== null &&
          processedBody.folder !== 'null'
        ) {
          processedBody.folder =
            typeof processedBody.folder === 'object'
              ? processedBody.folder
              : { id: processedBody.folder };
        }
        if (processedBody.storageConfig) {
          processedBody.storageConfig =
            typeof processedBody.storageConfig === 'object'
              ? processedBody.storageConfig
              : { id: processedBody.storageConfig };
        }
        if (processedBody.role) {
          if (typeof processedBody.role === 'string') {
            if (
              processedBody.role.startsWith('{') ||
              processedBody.role.startsWith('[')
            ) {
              try {
                processedBody.role = JSON.parse(processedBody.role);
              } catch (e) {
                const roleId = parseInt(processedBody.role, 10);
                if (!isNaN(roleId)) {
                  processedBody.role = { id: roleId };
                }
              }
            } else {
              const roleId = parseInt(processedBody.role, 10);
              if (!isNaN(roleId)) {
                processedBody.role = { id: roleId };
              }
            }
          }
          if (typeof processedBody.role === 'object' && processedBody.role.id) {
            processedBody.role = { id: processedBody.role.id };
          }
        }
        req.body = processedBody;
        req.routeData.context.$body = {
          ...req.routeData.context.$body,
          ...processedBody,
        };
        if (multipartConfig) {
          const uploadedFiles: UploadedFileFields = {};
          for (const field of multipartConfig.fields) {
            const files = grouped[field.name] ?? [];
            if (files.length) uploadedFiles[field.name] = field.maxCount === 1
              ? toUploadedFileInfo(files[0]!)
              : files.map(toUploadedFileInfo);
          }
          req.routeData.context.$uploadFile = uploadedFiles;
        }
        if (req.file) req.routeData.context.$uploadedFile = toUploadedFileInfo(req.file);
      }
      if (parsedFiles.length || req.file) {
        emitUploadProgress(req, dynamicWebSocketGateway, {
          phase: 'completed',
          loaded: req.uploadProgressTotal || parsedFiles.reduce((size, file) => size + file.size, req.file?.size ?? 0),
          total: req.uploadProgressTotal || parsedFiles.reduce((size, file) => size + file.size, req.file?.size ?? 0),
          percent: 100,
          fileName: (req.file ?? parsedFiles[0])?.originalname,
        });
      }
      next();
    });
  };
}

function normalizeUploadId(value: unknown): string | null {
  const uploadId = Array.isArray(value) ? value[0] : value;
  if (typeof uploadId !== 'string') return null;
  const trimmed = uploadId.trim();
  if (!trimmed || trimmed.length > 128) return null;
  return trimmed;
}

function setupUploadProgress(
  req: any,
  dynamicWebSocketGateway?: DynamicWebSocketGateway,
) {
  const uploadId = normalizeUploadId(req.headers['x-enfyra-upload-id']);
  if (!uploadId || !(req.user?.id ?? req.user?._id)) return;

  req.uploadProgressId = uploadId;
  req.uploadProgressTotal = Number(req.headers['content-length']) || 0;
  req.uploadProgressLoaded = 0;
  req.uploadProgressLastEmit = 0;

  emitUploadProgress(req, dynamicWebSocketGateway, {
    phase: 'receiving',
    loaded: 0,
    total: req.uploadProgressTotal || 0,
    percent: 0,
  });

  req.on('data', (chunk: Buffer) => {
    req.uploadProgressLoaded += chunk.length;
    const now = Date.now();
    if (now - req.uploadProgressLastEmit < UPLOAD_PROGRESS_EMIT_INTERVAL_MS) {
      return;
    }
    req.uploadProgressLastEmit = now;
    const total = req.uploadProgressTotal || 0;
    emitUploadProgress(req, dynamicWebSocketGateway, {
      phase: 'receiving',
      loaded: req.uploadProgressLoaded,
      total,
      percent: total
        ? Math.min(99, Math.floor((req.uploadProgressLoaded / total) * 100))
        : 0,
    });
  });

  req.on('end', () => {
    emitUploadProgress(req, dynamicWebSocketGateway, {
      phase: 'receiving',
      loaded: req.uploadProgressLoaded,
      total: req.uploadProgressTotal || 0,
      percent: 100,
    });
  });
}

export function emitUploadProgress(
  req: any,
  dynamicWebSocketGateway: DynamicWebSocketGateway | undefined,
  event: Omit<FileUploadProgressEvent, 'uploadId'>,
) {
  const uploadId = req.uploadProgressId;
  const userId = req.user?.id ?? req.user?._id;
  if (!uploadId || !userId || !dynamicWebSocketGateway) return;

  try {
    dynamicWebSocketGateway.emitToUser(userId, UPLOAD_PROGRESS_EVENT, {
      ...event,
      uploadId,
      percent: Math.min(100, Math.max(0, Math.round(event.percent))),
      route: req.routeData?.path || req.path,
      method: req.method,
    });
  } catch {}
}

function detectEncodingCorruption(str: string): boolean {
  const corruptionPatterns = [/áº/, /Ã/, /[^\x00-\x7F]/];
  return corruptionPatterns.some((pattern) => pattern.test(str));
}

function isValidVietnameseString(str: string): boolean {
  const vietnameseRanges = [
    /[àáảãạăằắẳẵặâầấẩẫậ]/,
    /[èéẻẽẹêềếểễệ]/,
    /[ìíỉĩị]/,
    /[òóỏõọôồốổỗộơờớởỡợ]/,
    /[ùúủũụưừứửữự]/,
    /[ỳýỷỹỵ]/,
    /[đĐ]/,
  ];
  return vietnameseRanges.some((range) => range.test(str));
}

function fixCharacterCorruptions(str: string): string {
  const corruptionPatterns = [
    { pattern: /áº/g, replacement: 'ă' },
    { pattern: /Ã/g, replacement: 'à' },
    { pattern: /kÃ½/g, replacement: 'ký' },
    { pattern: /tá»±/g, replacement: 'tự' },
    { pattern: /Äáº·c/g, replacement: 'đặc' },
    { pattern: /biá»t/g, replacement: 'biệt' },
  ];
  let fixedStr = str;
  corruptionPatterns.forEach(({ pattern, replacement }) => {
    if (pattern.test(fixedStr)) {
      fixedStr = fixedStr.replace(pattern, replacement);
    }
  });
  return fixedStr;
}
