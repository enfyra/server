export function toExactDataMigrationWhere(filter: unknown): Record<string, any> | null {
  if (!filter || typeof filter !== 'object' || Array.isArray(filter)) return null;
  const entries = Object.entries(filter);
  if (entries.length === 0) return null;
  const where: Record<string, any> = {};
  for (const [field, value] of entries) {
    if (!field || field.startsWith('_')) return null;
    let scalar = value;
    if (value && typeof value === 'object' && !Array.isArray(value)) {
      if (Object.keys(value).length !== 1 || !Object.prototype.hasOwnProperty.call(value, '_eq')) return null;
      scalar = value._eq;
    }
    if (scalar !== null && !['string', 'number', 'boolean'].includes(typeof scalar)) return null;
    if (typeof scalar === 'number' && !Number.isFinite(scalar)) return null;
    where[field] = scalar;
  }
  return where;
}
