export function parseBooleanFields(obj: any, booleanFields?: Set<string>): any {
  if (!obj || typeof obj !== 'object' || Buffer.isBuffer(obj) || obj instanceof Date) {
    return obj;
  }

  const parsed = Array.isArray(obj) ? [...obj] : { ...obj };
  const booleanFieldSet = booleanFields || new Set<string>();

  for (const key in parsed) {
    const value = parsed[key];

    if (key === 'createdAt' || key === 'updatedAt') {
      parsed[key] = value;
      continue;
    }

    if (booleanFieldSet.has(key) && (value === 0 || value === 1)) {
      parsed[key] = value === 1;
    }
    else if (value && typeof value === 'object') {
      if (Array.isArray(value)) {
        parsed[key] = value.map((item: any) => parseBooleanFields(item, booleanFieldSet));
      } else {
        parsed[key] = parseBooleanFields(value, booleanFieldSet);
      }
    }
  }

  return parsed;
}

