function toIdString(v: any): string | null {
  if (v === undefined || v === null) return null;
  return String(v?._id ?? v?.id ?? v);
}

function resolveMacro(v: any, user: any): any {
  if (typeof v !== 'string') return v;
  if (v === '@USER.id') return toIdString(user);
  return v;
}

function getFieldValue(record: any, field: string): any {
  if (!record || typeof record !== 'object') return undefined;
  if (field === 'id') return record.id ?? record._id;
  if (field === '_id') return record._id ?? record.id;
  return record[field];
}

function matchEq(expected: any, actual: any): boolean {
  if (expected == null && actual == null) return true;
  return String(expected) === String(actual);
}

export function matchFieldPermissionCondition(
  condition: any,
  record: any,
  user: any,
): boolean {
  if (!condition || typeof condition !== 'object' || Array.isArray(condition)) {
    return false;
  }

  if (Array.isArray(condition._and)) {
    return condition._and.every((c: any) =>
      matchFieldPermissionCondition(c, record, user),
    );
  }

  if (Array.isArray(condition._or)) {
    return condition._or.some((c: any) =>
      matchFieldPermissionCondition(c, record, user),
    );
  }

  for (const key of Object.keys(condition)) {
    if (key === '_and' || key === '_or') continue;
    const node = condition[key];
    if (!node || typeof node !== 'object') return false;
    if ('_eq' in node) {
      const expected = resolveMacro(node._eq, user);
      const actual = getFieldValue(record, key);
      if (!matchEq(expected, actual)) return false;
      continue;
    }
    return false;
  }

  return true;
}
