import * as sharp from 'sharp';
import * as path from 'path';

export class ImageProcessorHelper {
  static createProcessor(
    input: Buffer | string,
    options?: {
      failOnError?: boolean;
      density?: number;
      limitInputPixels?: number;
    },
  ): sharp.Sharp {
    return sharp(input, {
      failOnError: options?.failOnError ?? false,
      density: options?.density ?? 72,
      limitInputPixels: options?.limitInputPixels ?? 268402689,
    })
      .rotate()
      .withMetadata();
  }

  static createStreamProcessor(options?: {
    failOnError?: boolean;
    density?: number;
    limitInputPixels?: number;
  }): sharp.Sharp {
    return sharp({
      failOnError: options?.failOnError ?? false,
      density: options?.density ?? 72,
      limitInputPixels: options?.limitInputPixels ?? 268402689,
    })
      .rotate()
      .withMetadata();
  }

  static applyResize(
    processor: sharp.Sharp,
    width?: number,
    height?: number,
  ): sharp.Sharp {
    if (!width && !height) return processor;

    return processor.resize(width, height, {
      fit: 'inside',
      withoutEnlargement: true,
      fastShrinkOnLoad: true,
    });
  }

  static setImageFormat(
    processor: sharp.Sharp,
    format: string,
    quality = 80,
  ): sharp.Sharp {
    const formatMap = {
      jpeg: () =>
        processor.jpeg({
          quality,
          progressive: true,
          mozjpeg: true,
          trellisQuantisation: true,
          overshootDeringing: true,
          optimizeScans: true,
        }),
      jpg: () =>
        processor.jpeg({
          quality,
          progressive: true,
          mozjpeg: true,
          trellisQuantisation: true,
          overshootDeringing: true,
          optimizeScans: true,
        }),
      png: () =>
        processor.png({ quality, compressionLevel: 9, progressive: true }),
      webp: () =>
        processor.webp({ quality, effort: 2, smartSubsample: true }),
      avif: () => processor.avif({ quality, effort: 2 }),
      gif: () => processor.gif(),
    };
    return formatMap[format]?.() || processor;
  }

  static validateImageParams(
    width?: number,
    height?: number,
    quality?: number,
  ): { valid: boolean; error?: string } {
    if (width && (width < 1 || width > 4000))
      return { valid: false, error: 'Width 1-4000' };
    if (height && (height < 1 || height > 4000))
      return { valid: false, error: 'Height 1-4000' };
    if (quality && (quality < 1 || quality > 100))
      return { valid: false, error: 'Quality 1-100' };
    return { valid: true };
  }

  static validateFormat(format: string): { valid: boolean; error?: string } {
    const supportedFormats = ['jpeg', 'jpg', 'png', 'webp', 'avif', 'gif'];
    if (!supportedFormats.includes(format.toLowerCase())) {
      return {
        valid: false,
        error: `Unsupported format: ${supportedFormats.join(', ')}`,
      };
    }
    return { valid: true };
  }

  static configureSharp(): void {
    sharp.cache({ memory: 100, files: 50 });
    sharp.concurrency(4);
    sharp.simd(true);
  }
}

