import * as path from 'path';

export class ImageFormatHelper {
  static getOriginalFormat(filePath: string): string {
    const ext = path.extname(filePath).toLowerCase().slice(1);
    return ext === 'jpg' ? 'jpeg' : ext;
  }

  static getMimeType(format: string): string {
    const types = {
      jpeg: 'image/jpeg',
      jpg: 'image/jpeg',
      png: 'image/png',
      webp: 'image/webp',
      avif: 'image/avif',
      gif: 'image/gif',
    };
    return types[format] || 'image/jpeg';
  }
}

