export function getStorageErrorMessage(error: unknown, includeCode = false): string {
  if (error === null || typeof error !== 'object') return 'Unknown error';
  const message = 'message' in error ? error.message : undefined;
  const code = includeCode && 'code' in error ? error.code : undefined;
  const name = 'name' in error ? error.name : undefined;
  return String(message || code || name || 'Unknown error');
}

export function isStorageObjectNotFound(error: unknown): boolean {
  if (error === null || typeof error !== 'object') return false;
  if ('name' in error && error.name === 'NotFound') return true;
  if (!('$metadata' in error)) return false;
  const metadata = error.$metadata;
  return metadata !== null && typeof metadata === 'object' && 'httpStatusCode' in metadata && metadata.httpStatusCode === 404;
}
