const FIELD_OPERATORS = new Set([
  '_eq',
  '_neq',
  '_gt',
  '_gte',
  '_lt',
  '_lte',
  '_in',
  '_not_in',
  '_nin',
  '_is_null',
  '_is_not_null',
]);

function isPlainObject(v: any): boolean {
  return v != null && typeof v === 'object' && !Array.isArray(v);
}

function isOperatorNode(v: any): boolean {
  if (!isPlainObject(v)) return false;
  for (const k of Object.keys(v)) {
    if (FIELD_OPERATORS.has(k)) return true;
  }
  return false;
}

function toIdString(v: any): string | null {
  if (v === undefined || v === null) return null;
  if (typeof v === 'object') return String(v._id ?? v.id ?? '');
  return String(v);
}

function resolveMacro(v: any, user: any): any {
  if (typeof v !== 'string' || !v.startsWith('@USER')) return v;
  if (v === '@USER') return toIdString(user);
  if (v === '@USER.id' || v === '@USER._id') return toIdString(user);
  const path = v.slice('@USER.'.length);
  return resolvePathOnObject(user, path);
}

function resolveMacroInArray(arr: any, user: any): any {
  if (!Array.isArray(arr)) return arr;
  return arr.map((v) => resolveMacro(v, user));
}

function resolvePathOnObject(root: any, path: string): any {
  if (root == null) return undefined;
  const parts = path.split('.');
  let cur: any = root;
  for (const part of parts) {
    if (cur == null) return undefined;
    if (typeof cur !== 'object') return undefined;
    if (part === 'id') {
      cur = cur.id ?? cur._id;
      continue;
    }
    if (part === '_id') {
      cur = cur._id ?? cur.id;
      continue;
    }
    cur = cur[part];
  }
  return cur;
}

function getFieldValue(record: any, field: string): any {
  return resolvePathOnObject(record, field);
}

function toComparable(v: any): { kind: 'num' | 'str' | 'null'; value: any } {
  if (v === null || v === undefined) return { kind: 'null', value: null };
  if (typeof v === 'number' && !Number.isNaN(v))
    return { kind: 'num', value: v };
  if (v instanceof Date) return { kind: 'num', value: v.getTime() };
  if (typeof v === 'boolean') return { kind: 'num', value: v ? 1 : 0 };
  if (typeof v === 'string') {
    const asNum = Number(v);
    if (v.trim() !== '' && Number.isFinite(asNum))
      return { kind: 'num', value: asNum };
    const asDate = Date.parse(v);
    if (Number.isFinite(asDate)) return { kind: 'num', value: asDate };
    return { kind: 'str', value: v };
  }
  if (typeof v === 'object') return { kind: 'str', value: toIdString(v) ?? '' };
  return { kind: 'str', value: String(v) };
}

function compareNumericOrdered(
  op: 'gt' | 'gte' | 'lt' | 'lte',
  actual: any,
  expected: any,
): boolean {
  if (actual == null || expected == null) return false;
  const a = toComparable(actual);
  const b = toComparable(expected);
  if (a.kind === 'null' || b.kind === 'null') return false;
  if (a.kind !== b.kind) return false;
  switch (op) {
    case 'gt':
      return a.value > b.value;
    case 'gte':
      return a.value >= b.value;
    case 'lt':
      return a.value < b.value;
    case 'lte':
      return a.value <= b.value;
  }
}

function equalsLoose(a: any, b: any): boolean {
  if (a == null && b == null) return true;
  if (a == null || b == null) return false;
  const sa = toIdString(a);
  const sb = toIdString(b);
  return sa === sb;
}

function evalOperatorNode(
  node: Record<string, any>,
  actual: any,
  user: any,
): boolean {
  for (const op of Object.keys(node)) {
    if (!FIELD_OPERATORS.has(op)) return false;
    const rawExpected = node[op];
    switch (op) {
      case '_eq': {
        const expected = resolveMacro(rawExpected, user);
        if (expected === undefined) return false;
        if (!equalsLoose(actual, expected)) return false;
        break;
      }
      case '_neq': {
        const expected = resolveMacro(rawExpected, user);
        if (expected === undefined) return false;
        if (equalsLoose(actual, expected)) return false;
        break;
      }
      case '_gt':
      case '_gte':
      case '_lt':
      case '_lte': {
        const expected = resolveMacro(rawExpected, user);
        if (expected === undefined) return false;
        const short = op.slice(1) as 'gt' | 'gte' | 'lt' | 'lte';
        if (!compareNumericOrdered(short, actual, expected)) return false;
        break;
      }
      case '_in': {
        const expected = Array.isArray(rawExpected)
          ? resolveMacroInArray(rawExpected, user)
          : resolveMacro(rawExpected, user);
        if (!Array.isArray(expected)) return false;
        if (!expected.some((e) => equalsLoose(actual, e))) return false;
        break;
      }
      case '_not_in':
      case '_nin': {
        const expected = Array.isArray(rawExpected)
          ? resolveMacroInArray(rawExpected, user)
          : resolveMacro(rawExpected, user);
        if (!Array.isArray(expected)) return false;
        if (expected.some((e) => equalsLoose(actual, e))) return false;
        break;
      }
      case '_is_null': {
        const want = rawExpected === true || rawExpected === 'true';
        const isNull = actual === null || actual === undefined;
        if (want !== isNull) return false;
        break;
      }
      case '_is_not_null': {
        const want = rawExpected === true || rawExpected === 'true';
        const isNotNull = actual !== null && actual !== undefined;
        if (want !== isNotNull) return false;
        break;
      }
      default:
        return false;
    }
  }
  return true;
}

export function matchFieldPermissionCondition(
  condition: any,
  record: any,
  user: any,
): boolean {
  if (!isPlainObject(condition)) return false;

  for (const key of Object.keys(condition)) {
    const node = condition[key];

    if (key === '_and') {
      if (!Array.isArray(node) || node.length === 0) return false;
      if (!node.every((c) => matchFieldPermissionCondition(c, record, user)))
        return false;
      continue;
    }
    if (key === '_or') {
      if (!Array.isArray(node) || node.length === 0) return false;
      if (!node.some((c) => matchFieldPermissionCondition(c, record, user)))
        return false;
      continue;
    }
    if (key === '_not') {
      if (!isPlainObject(node)) return false;
      if (matchFieldPermissionCondition(node, record, user)) return false;
      continue;
    }

    if (key.startsWith('_')) return false;

    const actual = getFieldValue(record, key);

    if (isOperatorNode(node)) {
      if (!evalOperatorNode(node, actual, user)) return false;
      continue;
    }

    if (!isPlainObject(node)) return false;

    if (actual == null) return false;
    if (Array.isArray(actual)) return false;
    if (typeof actual !== 'object') return false;

    if (!matchFieldPermissionCondition(node, actual, user)) return false;
  }

  return true;
}
