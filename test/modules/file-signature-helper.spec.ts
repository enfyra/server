import { describe, expect, it } from 'vitest';
import { FileSignatureHelper } from '../../src/modules/file-management/utils/file-signature.helper';

describe('FileSignatureHelper', () => {
  it('detects HEIC files from ISO brand bytes', () => {
    const buffer = Buffer.concat([
      Buffer.from([0x00, 0x00, 0x00, 0x18]),
      Buffer.from('ftypheic', 'ascii'),
      Buffer.alloc(16),
    ]);

    expect(FileSignatureHelper.detect(buffer)).toEqual({
      extension: 'heic',
      mimetype: 'image/heic',
    });
  });

  it('normalizes uploaded metadata when the declared extension is wrong', () => {
    const buffer = Buffer.concat([
      Buffer.from([0x00, 0x00, 0x00, 0x18]),
      Buffer.from('ftypheic', 'ascii'),
      Buffer.alloc(16),
    ]);

    expect(
      FileSignatureHelper.normalizeUploadMetadata(
        'IMG20260516162757.png',
        'image/png',
        buffer,
      ),
    ).toEqual({
      filename: 'IMG20260516162757.heic',
      mimetype: 'image/heic',
    });
  });

  it('detects MP4 files from ISO brand bytes', () => {
    const buffer = Buffer.concat([
      Buffer.from([0x00, 0x00, 0x00, 0x18]),
      Buffer.from('ftypmp42', 'ascii'),
      Buffer.alloc(16),
    ]);

    expect(FileSignatureHelper.detect(buffer)).toEqual({
      extension: 'mp4',
      mimetype: 'video/mp4',
    });
  });

  it('detects WebM files from EBML bytes', () => {
    const buffer = Buffer.concat([
      Buffer.from([0x1a, 0x45, 0xdf, 0xa3]),
      Buffer.alloc(16),
    ]);

    expect(FileSignatureHelper.detect(buffer)).toEqual({
      extension: 'webm',
      mimetype: 'video/webm',
    });
  });
});
