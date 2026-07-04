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

function parsePostgresArrayLiteral(value: string): string[] | null {
  const trimmed = value.trim();
  if (!trimmed.startsWith('{') || !trimmed.endsWith('}')) return null;

  const inner = trimmed.slice(1, -1);
  if (!inner) return [];

  const result: string[] = [];
  let current = '';
  let inQuotes = false;
  let wasQuoted = false;
  let escaping = false;

  const pushCurrent = () => {
    const raw = wasQuoted ? current : current.trim();
    result.push(!wasQuoted && raw === 'NULL' ? '' : raw);
    current = '';
    wasQuoted = false;
  };

  for (let i = 0; i < inner.length; i += 1) {
    const char = inner[i];
    if (escaping) {
      current += char;
      escaping = false;
      continue;
    }
    if (char === '\\') {
      escaping = true;
      continue;
    }
    if (char === '"') {
      inQuotes = !inQuotes;
      wasQuoted = true;
      continue;
    }
    if (char === ',' && !inQuotes) {
      pushCurrent();
      continue;
    }
    current += char;
  }

  if (inQuotes || escaping) return null;
  pushCurrent();
  return result;
}

export function normalizeColumnOptionsValue(value: any): any {
  const normalized = normalizeJsonFieldValue(value);
  if (typeof normalized !== 'string') return normalized;
  return parsePostgresArrayLiteral(normalized) ?? normalized;
}

export function stringifyJsonFieldValue(value: any): string | null {
  if (value === undefined || value === null) return null;
  return JSON.stringify(normalizeJsonFieldValue(value));
}
