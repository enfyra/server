import { Double, Int32, Long, ObjectId } from 'mongodb';

export function normalizeCloneValue(value: any): any {
  if (value === undefined || value === null) return null;
  if (value instanceof Date) return value.toISOString();
  if (value instanceof ObjectId) return value.toHexString();
  if (value instanceof Long) return value.toString();
  if (value instanceof Int32 || value instanceof Double) return value.valueOf();
  if (typeof value === 'bigint') return value.toString();
  if (Array.isArray(value)) return value.map(normalizeCloneValue);
  if (typeof value === 'object') {
    return Object.fromEntries(
      Object.keys(value)
        .sort()
        .map((key) => [key, normalizeCloneValue(value[key])]),
    );
  }
  return value;
}

export function normalizeCloneSqlTimestamp(value: string | Date): string {
  const text = value instanceof Date ? value.toISOString() : value;
  const match = text.match(
    /^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})(?:\.(\d{1,6}))?(?:Z|\+00(?::?00)?)?$/,
  );
  if (!match)
    throw new Error('Clone temporal comparison requires UTC timestamp input');
  return `${match[1]}T${match[2]}.${(match[3] ?? '').padEnd(6, '0')}Z`;
}
