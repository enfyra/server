import * as path from 'path';

type DetectedFileSignature = {
  extension: string;
  mimetype: string;
};

const SIGNATURES: DetectedFileSignature[] = [
  { extension: 'png', mimetype: 'image/png' },
  { extension: 'jpg', mimetype: 'image/jpeg' },
  { extension: 'gif', mimetype: 'image/gif' },
  { extension: 'webp', mimetype: 'image/webp' },
  { extension: 'avif', mimetype: 'image/avif' },
  { extension: 'heic', mimetype: 'image/heic' },
  { extension: 'heif', mimetype: 'image/heif' },
  { extension: 'pdf', mimetype: 'application/pdf' },
  { extension: 'mp4', mimetype: 'video/mp4' },
  { extension: 'mov', mimetype: 'video/quicktime' },
  { extension: 'webm', mimetype: 'video/webm' },
];

export class FileSignatureHelper {
  static detect(buffer: Buffer): DetectedFileSignature | null {
    if (
      buffer.length >= 8 &&
      buffer
        .subarray(0, 8)
        .equals(Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]))
    ) {
      return SIGNATURES[0];
    }

    if (
      buffer.length >= 3 &&
      buffer[0] === 0xff &&
      buffer[1] === 0xd8 &&
      buffer[2] === 0xff
    ) {
      return SIGNATURES[1];
    }

    const asciiPrefix = buffer
      .subarray(0, Math.min(buffer.length, 32))
      .toString('ascii');
    if (asciiPrefix.startsWith('GIF87a') || asciiPrefix.startsWith('GIF89a')) {
      return SIGNATURES[2];
    }

    if (
      buffer.length >= 12 &&
      buffer.subarray(0, 4).toString('ascii') === 'RIFF' &&
      buffer.subarray(8, 12).toString('ascii') === 'WEBP'
    ) {
      return SIGNATURES[3];
    }

    const brand = this.readIsoBrand(buffer);
    if (brand) {
      if (brand === 'avif' || brand === 'avis') return SIGNATURES[4];
      if (
        brand === 'heic' ||
        brand === 'heix' ||
        brand === 'hevc' ||
        brand === 'hevx'
      )
        return SIGNATURES[5];
      if (brand === 'mif1' || brand === 'msf1') return SIGNATURES[6];
      if (brand === 'qt  ') return SIGNATURES[9];
      if (
        brand === 'isom' ||
        brand === 'iso2' ||
        brand === 'mp41' ||
        brand === 'mp42' ||
        brand === 'm4v '
      )
        return SIGNATURES[8];
    }

    if (asciiPrefix.startsWith('%PDF-')) return SIGNATURES[7];

    if (
      buffer.length >= 4 &&
      buffer[0] === 0x1a &&
      buffer[1] === 0x45 &&
      buffer[2] === 0xdf &&
      buffer[3] === 0xa3
    ) {
      return SIGNATURES[10];
    }

    return null;
  }

  static normalizeUploadMetadata(
    filename: string,
    mimetype: string,
    buffer: Buffer,
  ): { filename: string; mimetype: string } {
    const detected = this.detect(buffer);
    if (!detected) return { filename, mimetype };
    return {
      filename: this.replaceExtension(filename, detected.extension),
      mimetype: detected.mimetype,
    };
  }

  static replaceExtension(filename: string, extension: string): string {
    const ext = path.extname(filename);
    const base = ext ? filename.slice(0, -ext.length) : filename;
    return `${base}.${extension}`;
  }

  private static readIsoBrand(buffer: Buffer): string | null {
    if (buffer.length < 12) return null;
    if (buffer.subarray(4, 8).toString('ascii') !== 'ftyp') return null;
    return buffer.subarray(8, 12).toString('ascii').toLowerCase();
  }
}
