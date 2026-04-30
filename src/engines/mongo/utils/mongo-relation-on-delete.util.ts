export type TRelationOnDeleteAction = 'CASCADE' | 'SET NULL' | 'RESTRICT';

export function normalizeRelationOnDelete(
  relation: {
    onDelete?: string | null;
  } | null,
): TRelationOnDeleteAction {
  const raw = String(relation?.onDelete ?? 'SET NULL')
    .toUpperCase()
    .trim()
    .replace(/_/g, ' ');
  if (raw === 'NO ACTION') {
    return 'RESTRICT';
  }
  if (raw === 'CASCADE' || raw === 'SET NULL' || raw === 'RESTRICT') {
    return raw === 'SET NULL' ? 'SET NULL' : raw;
  }
  return 'SET NULL';
}
