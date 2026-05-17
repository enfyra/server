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
});
