export function getDeletedIds<T extends { id?: any }>(
  oldItems: T[],
  newItems: T[],
): any[] {
  const oldIds = oldItems.map((item) => item.id).filter(Boolean);
  const newIds = newItems.map((item) => item.id).filter(Boolean);
  return oldIds.filter((id) => !newIds.includes(id));
}
