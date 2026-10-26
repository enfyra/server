import { describe, expect, it } from 'vitest';
import { getStorageErrorMessage, isStorageObjectNotFound } from '../../src/modules/file-management/utils/storage-error.util';

describe('storage error boundary', () => {
  it('retains message, provider code and name precedence', () => {
    expect(getStorageErrorMessage(new Error('upload failed'))).toBe('upload failed');
    expect(getStorageErrorMessage({ code: 403, name: 'Forbidden' }, true)).toBe('403');
    expect(getStorageErrorMessage({ code: 403, name: 'Forbidden' })).toBe('Forbidden');
    expect(getStorageErrorMessage({ message: '', name: 'ProviderError' })).toBe('ProviderError');
  });

  it.each([null, undefined, false, 42, 'failed', {}])('handles non-error rejection %j without dereferencing it', (error) => {
    expect(getStorageErrorMessage(error)).toBe('Unknown error');
    expect(isStorageObjectNotFound(error)).toBe(false);
  });

  it('recognizes provider missing objects without treating malformed metadata as missing', () => {
    expect(isStorageObjectNotFound({ name: 'NotFound' })).toBe(true);
    expect(isStorageObjectNotFound({ $metadata: { httpStatusCode: 404 } })).toBe(true);
    expect(isStorageObjectNotFound({ $metadata: { httpStatusCode: 403 } })).toBe(false);
    expect(isStorageObjectNotFound({ $metadata: null })).toBe(false);
    expect(isStorageObjectNotFound({ $metadata: { httpStatusCode: '404' } })).toBe(false);
  });
});
