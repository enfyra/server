export function parseSortInput(sort?: string | string[]) {
  if (!sort) return [];
  const arr = Array.isArray(sort) ? sort : [sort];
  return arr.map((s) => {
    if (typeof s === 'string' && s.startsWith('-')) {
      return { field: s.substring(1), direction: 'DESC' as const };
    }
    return { field: s, direction: 'ASC' as const };
  });
}
