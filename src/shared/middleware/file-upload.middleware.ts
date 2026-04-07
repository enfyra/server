import {
  Injectable,
  NestMiddleware,
  BadRequestException,
} from '@nestjs/common';
import { Response, NextFunction } from 'express';
import * as multer from 'multer';
import { RequestWithRouteData } from '../types';
@Injectable()
export class FileUploadMiddleware implements NestMiddleware {
  private upload = multer({
    storage: multer.memoryStorage(),
    limits: {
      fileSize: 10 * 1024 * 1024,
    },
    fileFilter: (req, file, cb) => {
      cb(null, true);
    },
    preservePath: true,
    encoding: 'utf8',
  });
  use(req: RequestWithRouteData, res: Response, next: NextFunction) {
    const isPostOrPatch = ['POST', 'PATCH'].includes(req.method);
    const isMultipartContent = req.headers['content-type']?.includes(
      'multipart/form-data',
    );
    if (!isPostOrPatch || !isMultipartContent) {
      return next();
    }
    this.upload.single('file')(req, res, (error: any) => {
      if (error) {
        return next(error);
      }
      if (req.file && req.file.originalname) {
        try {
          let fixedName = req.file.originalname;
          if (this.detectEncodingCorruption(fixedName)) {
            const utf8Fixed = Buffer.from(fixedName, 'latin1').toString('utf8');
            if (this.isValidVietnameseString(utf8Fixed)) {
              fixedName = utf8Fixed;
            }
          }
          fixedName = this.fixCharacterCorruptions(fixedName);
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
            if (processedBody.role.startsWith('{') || processedBody.role.startsWith('[')) {
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
            buffer: req.file.buffer,
            size: req.file.size,
            fieldname: req.file.fieldname,
          };
        }
      }
      next();
    });
  }
  private detectEncodingCorruption(str: string): boolean {
    const corruptionPatterns = [/áº/, /Ã/, /[^\x00-\x7F]/];
    return corruptionPatterns.some((pattern) => pattern.test(str));
  }
  private isValidVietnameseString(str: string): boolean {
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
  private fixCharacterCorruptions(str: string): string {
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
}