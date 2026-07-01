export function normalizeJsonFieldValue(value: any): any {
  if (value === undefined) return undefined;
  if (value === null) return null;

  let current = value;
  for (let i = 0; i < 64; i += 1) {
    if (typeof current !== 'string') return current;
    const trimmed = current.trim();
    if (!trimmed) return current;
    try {
      const parsed = JSON.parse(trimmed);
      if (parsed === current) return parsed;
      current = parsed;
    } catch {
      return current;
    }
  }

  return current;
}

export function stringifyJsonFieldValue(value: any): string | null {
  if (value === undefined || value === null) return null;
  return JSON.stringify(normalizeJsonFieldValue(value));
}
