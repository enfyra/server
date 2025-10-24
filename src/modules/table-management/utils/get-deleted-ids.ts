export function getDeletedIds<T extends { id?: any; _id?: any }>(
  oldItems: T[],
  newItems: T[],
): any[] {
  const oldIds = oldItems.map((item) => (item._id || item.id)).filter(Boolean);
  const newIds = newItems.map((item) => (item._id || item.id)).filter(Boolean);

  return oldIds.filter((oldId) => {
    const oldIdStr = oldId.toString();
    return !newIds.some((newId) => newId.toString() === oldIdStr);
  });
}
