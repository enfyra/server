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
    fit?: string,
    gravity?: string,
  ): sharp.Sharp {
    if (!width && !height) return processor;

    const fitMap: { [key: string]: 'cover' | 'contain' | 'fill' | 'inside' | 'outside' } = {
      cover: 'cover',
      contain: 'contain',
      fill: 'fill',
      inside: 'inside',
      outside: 'outside',
    };

    const gravityMap: { [key: string]: sharp.Gravity } = {
      center: 'center',
      north: 'north',
      south: 'south',
      east: 'east',
      west: 'west',
      northeast: 'northeast',
      northwest: 'northwest',
      southeast: 'southeast',
      southwest: 'southwest',
      face: 'attention',
      faces: 'attention',
      auto: 'attention',
    };

    const resizeOptions: sharp.ResizeOptions = {
      fit: fitMap[fit?.toLowerCase() || ''] || 'inside',
      withoutEnlargement: true,
      fastShrinkOnLoad: true,
    };

    if (gravity && gravityMap[gravity.toLowerCase()]) {
      resizeOptions.position = gravityMap[gravity.toLowerCase()];
    }

    return processor.resize(width, height, resizeOptions);
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
          progressive: false,
          mozjpeg: true,
        }),
      jpg: () =>
        processor.jpeg({
          quality,
          progressive: false,
          mozjpeg: true,
        }),
      png: () =>
        processor.png({ quality, compressionLevel: 6, progressive: false }),
      webp: () =>
        processor.webp({ quality, effort: 1 }),
      avif: () => {
        if (quality === undefined) {
          return processor.avif({ effort: 1 });
        }
        return processor.avif({ quality, effort: 1 });
      },
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

  static applyTransformations(
    processor: sharp.Sharp,
    rotate?: number,
    flip?: string,
    blur?: number,
    sharpen?: number,
  ): sharp.Sharp {
    if (rotate !== undefined && rotate !== 0) {
      processor = processor.rotate(rotate);
    }

    if (flip) {
      const flipLower = flip.toLowerCase();
      if (flipLower === 'horizontal' || flipLower === 'h') {
        processor = processor.flip();
      } else if (flipLower === 'vertical' || flipLower === 'v') {
        processor = processor.flop();
      }
    }

    if (blur !== undefined && blur > 0) {
      processor = processor.blur(blur);
    }

    if (sharpen !== undefined && sharpen > 0) {
      processor = processor.sharpen(sharpen);
    }

    return processor;
  }

  static applyEffects(
    processor: sharp.Sharp,
    brightness?: number,
    contrast?: number,
    saturation?: number,
    grayscale?: boolean,
  ): sharp.Sharp {
    const modulateOptions: { brightness?: number; saturation?: number } = {};

    if (brightness !== undefined) {
      modulateOptions.brightness = brightness / 100 + 1; // Convert -100 to 100 to 0 to 2
    }

    if (saturation !== undefined) {
      modulateOptions.saturation = saturation / 100 + 1; // Convert -100 to 100 to 0 to 2
    }

    if (Object.keys(modulateOptions).length > 0) {
      processor = processor.modulate(modulateOptions);
    }

    // Note: Sharp doesn't have direct contrast support
    // Contrast can be simulated with brightness adjustment, but it's not accurate
    // For now, we'll skip contrast implementation
    // if (contrast !== undefined) {
    //   // Contrast simulation would require more complex processing
    // }

    if (grayscale === true) {
      processor = processor.greyscale();
    }

    return processor;
  }

  static validateTransformParams(
    rotate?: number,
    flip?: string,
    blur?: number,
    sharpen?: number,
    brightness?: number,
    contrast?: number,
    saturation?: number,
  ): { valid: boolean; error?: string } {
    if (rotate !== undefined && (rotate < -360 || rotate > 360))
      return { valid: false, error: 'Rotate -360 to 360' };
    if (flip && !['horizontal', 'vertical', 'h', 'v'].includes(flip.toLowerCase()))
      return { valid: false, error: 'Flip: horizontal, vertical, h, v' };
    if (blur !== undefined && (blur < 0 || blur > 100))
      return { valid: false, error: 'Blur 0-100' };
    if (sharpen !== undefined && (sharpen < 0 || sharpen > 100))
      return { valid: false, error: 'Sharpen 0-100' };
    if (brightness !== undefined && (brightness < -100 || brightness > 100))
      return { valid: false, error: 'Brightness -100 to 100' };
    if (contrast !== undefined && (contrast < -100 || contrast > 100))
      return { valid: false, error: 'Contrast -100 to 100' };
    if (saturation !== undefined && (saturation < -100 || saturation > 100))
      return { valid: false, error: 'Saturation -100 to 100' };
    return { valid: true };
  }

  static validateFit(fit?: string): { valid: boolean; error?: string } {
    if (fit && !['cover', 'contain', 'fill', 'inside', 'outside'].includes(fit.toLowerCase()))
      return { valid: false, error: 'Fit: cover, contain, fill, inside, outside' };
    return { valid: true };
  }

  static validateGravity(gravity?: string): { valid: boolean; error?: string } {
    const validGravities = [
      'center', 'north', 'south', 'east', 'west',
      'northeast', 'northwest', 'southeast', 'southwest',
      'face', 'faces', 'auto',
    ];
    if (gravity && !validGravities.includes(gravity.toLowerCase()))
      return { valid: false, error: `Gravity: ${validGravities.join(', ')}` };
    return { valid: true };
  }

  static configureSharp(): void {
    sharp.cache(false);
    sharp.concurrency(8);
    sharp.simd(true);
  }
}

