const MIN_INT64 = -(1n << 63n);
const MAX_INT64 = (1n << 63n) - 1n;

export function isExactInt64(value: unknown): value is number | string {
  if (typeof value === 'number') return Number.isSafeInteger(value);
  if (typeof value !== 'string' || !/^-?\d+$/.test(value)) return false;
  const integer = BigInt(value);
  return integer >= MIN_INT64 && integer <= MAX_INT64;
}
