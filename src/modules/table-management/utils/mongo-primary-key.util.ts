export const MONGO_PRIMARY_KEY_NAME = '_id';
export const MONGO_PRIMARY_KEY_TYPE = 'ObjectId';

export function isMongoPrimaryKeyColumn(column: { name?: string; isPrimary?: boolean }) {
  return column.isPrimary === true && (column.name === '_id' || column.name === 'id');
}

export function isMongoPrimaryKeyType(type: unknown) {
  return String(type) === MONGO_PRIMARY_KEY_TYPE;
}

export function normalizeMongoPrimaryKeyColumn<T extends { name?: string; type?: string; isPrimary?: boolean }>(
  column: T,
): T {
  if (!isMongoPrimaryKeyColumn(column)) return column;
  return {
    ...column,
    name: MONGO_PRIMARY_KEY_NAME,
    type: MONGO_PRIMARY_KEY_TYPE,
  };
}
