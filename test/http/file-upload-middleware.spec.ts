import { describe, expect, it } from 'vitest';
import { resolveUploadFileSizeLimitBytes } from '../../src/http/middlewares/file-upload.middleware';

describe('file upload middleware limit resolution', () => {
  it('uses the global upload limit when the route has no override', () => {
    expect(resolveUploadFileSizeLimitBytes(10 * 1024 * 1024, null)).toBe(
      10 * 1024 * 1024,
    );
  });

  it('uses a positive route upload limit override in MB', () => {
    expect(resolveUploadFileSizeLimitBytes(10 * 1024 * 1024, 128)).toBe(
      128 * 1024 * 1024,
    );
  });

  it('ignores invalid route upload limit overrides', () => {
    expect(resolveUploadFileSizeLimitBytes(10 * 1024 * 1024, 0)).toBe(
      10 * 1024 * 1024,
    );
    expect(resolveUploadFileSizeLimitBytes(10 * 1024 * 1024, 'abc')).toBe(
      10 * 1024 * 1024,
    );
  });
});
