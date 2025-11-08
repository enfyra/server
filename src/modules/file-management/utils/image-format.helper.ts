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

  static updateFilenameWithFormat(filename: string, format: string): string {
    if (!format) return filename;
    
    const ext = path.extname(filename).toLowerCase();
    const nameWithoutExt = path.basename(filename, ext);
    
    // Map format to extension
    const formatExtMap: { [key: string]: string } = {
      jpeg: '.jpg',
      jpg: '.jpg',
      png: '.png',
      webp: '.webp',
      avif: '.avif',
      gif: '.gif',
    };
    
    const newExt = formatExtMap[format.toLowerCase()] || ext;
    return `${nameWithoutExt}${newExt}`;
  }
}

