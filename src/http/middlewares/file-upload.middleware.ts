import { Response, NextFunction } from 'express';
import multer from 'multer';
import os from 'os';
import crypto from 'crypto';
import type { RuntimeRegistryService } from '../../engines/cache/services/runtime-registry.service';
import type { DynamicWebSocketGateway } from '../../modules/websocket/gateway/dynamic-websocket.gateway';
import type { FileUploadProgressEvent } from '../../shared/types';

const FILE_UPLOAD_PROGRESS_EVENT = '$system:file-upload:progress';

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
    const isPostOrPatch = ['POST', 'PATCH'].includes(req.method);
    const isMultipartContent = req.headers['content-type']?.includes(
      'multipart/form-data',
    );
    if (!isPostOrPatch || !isMultipartContent) {
      return next();
    }
    setupUploadProgress(req, dynamicWebSocketGateway);
    const upload = multer({
      storage: diskStorage,
      limits: {
        fileSize: resolveUploadFileSizeLimitBytes(
          runtimeRegistryService.getMaxUploadFileSizeBytes(),
          req.routeData?.maxUploadFileSize,
        ),
      },
    });
    upload.single('file')(req, res, (error: any) => {
      if (error) {
        emitUploadProgress(req, dynamicWebSocketGateway, {
          phase: 'failed',
          loaded: req.uploadProgressLoaded || 0,
          total: req.uploadProgressTotal || 0,
          percent: 0,
        });
        return next(error);
      }
      if (req.file && req.file.originalname) {
        try {
          let fixedName = req.file.originalname;
          if (detectEncodingCorruption(fixedName)) {
            const utf8Fixed = Buffer.from(fixedName, 'latin1').toString('utf8');
            if (isValidVietnameseString(utf8Fixed)) {
              fixedName = utf8Fixed;
            }
          }
          fixedName = fixCharacterCorruptions(fixedName);
          req.file.originalname = fixedName;
        } catch (error) {
          console.warn('Failed to fix filename encoding:', error);
        }
      }
      if (req.routeData?.context) {
        const processedBody: any = { ...req.body };
        delete processedBody.file;
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
        req.routeData.context.$body = {
          ...req.routeData.context.$body,
          ...processedBody,
        };
        if (req.file) {
          req.routeData.context.$uploadedFile = {
            originalname: req.file.originalname,
            mimetype: req.file.mimetype,
            encoding: req.file.encoding || 'utf8',
            path: req.file.path,
            size: req.file.size,
            fieldname: req.file.fieldname,
          };
        }
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
  if (!uploadId || !req.user?.id) return;

  req.uploadProgressId = uploadId;
  req.uploadProgressTotal = Number(req.headers['content-length']) || 0;
  req.uploadProgressLoaded = 0;
  req.uploadProgressLastEmit = 0;

  req.on('data', (chunk: Buffer) => {
    req.uploadProgressLoaded += chunk.length;
    const now = Date.now();
    if (now - req.uploadProgressLastEmit < 100) return;
    req.uploadProgressLastEmit = now;
    const total = req.uploadProgressTotal || 0;
    emitUploadProgress(req, dynamicWebSocketGateway, {
      phase: 'receiving',
      loaded: req.uploadProgressLoaded,
      total,
      percent: total
        ? Math.min(80, Math.floor((req.uploadProgressLoaded / total) * 80))
        : 0,
    });
  });

  req.on('end', () => {
    emitUploadProgress(req, dynamicWebSocketGateway, {
      phase: 'receiving',
      loaded: req.uploadProgressLoaded,
      total: req.uploadProgressTotal || 0,
      percent: 80,
    });
  });
}

export function emitUploadProgress(
  req: any,
  dynamicWebSocketGateway: DynamicWebSocketGateway | undefined,
  event: Omit<FileUploadProgressEvent, 'uploadId'> & { uploadId?: string },
) {
  const uploadId = event.uploadId || req.uploadProgressId;
  const userId = req.user?.id;
  if (!uploadId || !userId || !dynamicWebSocketGateway) return;

  try {
    dynamicWebSocketGateway.emitToUser(userId, FILE_UPLOAD_PROGRESS_EVENT, {
      ...event,
      uploadId,
      percent: Math.min(100, Math.max(0, Math.round(event.percent))),
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
