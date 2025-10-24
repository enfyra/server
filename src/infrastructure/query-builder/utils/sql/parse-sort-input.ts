export function parseSortInput(sort?: string | string[]) {
  if (!sort) return [];
  const arr = Array.isArray(sort)
    ? sort
    : sort.split(',').map(s => s.trim());
  return arr.map((s) => {
    const trimmed = typeof s === 'string' ? s.trim() : s;
    if (typeof trimmed === 'string' && trimmed.startsWith('-')) {
      return { field: trimmed.substring(1), direction: 'DESC' as const };
    }
    return { field: trimmed as string, direction: 'ASC' as const };
  });
}
