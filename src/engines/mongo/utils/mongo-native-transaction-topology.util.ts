export function mongoTopologySupportsNativeTransactions(
  hello: unknown,
): boolean {
  if (!hello || typeof hello !== 'object') {
    return false;
  }
  const h = hello as Record<string, unknown>;
  if (typeof h.setName === 'string' && h.setName.length > 0) {
    return true;
  }
  if (h.msg === 'isdbgrid') {
    return true;
  }
  return false;
}
