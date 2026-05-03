export function getRelationTargetTableId(relation: any): number | string | null {
  const value =
    relation?.targetTable && typeof relation.targetTable === 'object'
      ? relation.targetTable.id
      : relation?.targetTable;

  if (value === undefined || value === null || value === '') return null;

  if (typeof value === 'string' && /^\d+$/.test(value)) {
    return Number(value);
  }

  return value;
}

export function relationTargetTableMapKey(id: number | string | null): string {
  return String(id);
}
