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

export function getRelationMappedByProperty(relation: any): string | null {
  const value = relation?.mappedBy;
  if (typeof value === 'string') {
    const trimmed = value.trim();
    return trimmed.length > 0 ? trimmed : null;
  }
  if (value && typeof value === 'object') {
    const propertyName = value.propertyName;
    if (typeof propertyName === 'string') {
      const trimmed = propertyName.trim();
      return trimmed.length > 0 ? trimmed : null;
    }
  }
  return null;
}
