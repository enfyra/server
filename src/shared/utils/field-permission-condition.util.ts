import { types } from 'node:util';

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

function isPlainObject(v: unknown): v is Record<string, unknown> {
  return v != null && typeof v === 'object' && !Array.isArray(v);
}

function isUnknownArray(v: unknown): v is unknown[] {
  return Array.isArray(v);
}

function isOperatorNode(v: unknown): v is Record<string, unknown> {
  if (!isPlainObject(v)) return false;
  for (const k of Object.keys(v)) {
    if (FIELD_OPERATORS.has(k)) return true;
  }
  return false;
}

function readObjectProperty(value: object, key: string): unknown {
  return Reflect.get(value, key);
}

function toIdString(v: unknown): string | null {
  if (v === undefined || v === null) return null;
  if (typeof v === 'string') return v;
  if (typeof v === 'number') return String(v);
  if (typeof v === 'boolean') return v ? 'true' : 'false';
  if (typeof v === 'object')
    return String(readObjectProperty(v, '_id') ?? readObjectProperty(v, 'id') ?? '');
  return String(v);
}

function resolveMacro(v: unknown, user: unknown): unknown {
  if (typeof v !== 'string' || !v.startsWith('@USER')) return v;
  if (v === '@USER') return toIdString(user);
  if (v === '@USER.id' || v === '@USER._id') return toIdString(user);
  const path = v.slice('@USER.'.length);
  return resolvePathOnObject(user, path);
}

function resolveMacroInArray(arr: unknown, user: unknown): unknown {
  if (!isUnknownArray(arr)) return arr;
  return arr.map((v) => resolveMacro(v, user));
}

function resolvePathOnObject(root: unknown, path: string): unknown {
  if (root == null) return undefined;
  return resolvePathParts(root, path.split('.'));
}

function resolvePathParts(root: unknown, parts: string[]): unknown {
  let cur: unknown = root;
  for (const part of parts) {
    if (cur == null) return undefined;
    if (typeof cur !== 'object') return undefined;
    if (part === 'id') {
      cur = readObjectProperty(cur, 'id') ?? readObjectProperty(cur, '_id');
      continue;
    }
    if (part === '_id') {
      cur = readObjectProperty(cur, '_id') ?? readObjectProperty(cur, 'id');
      continue;
    }
    cur = readObjectProperty(cur, part);
  }
  return cur;
}

function getFieldValue(record: unknown, field: string): unknown {
  return resolvePathOnObject(record, field);
}

type Comparable =
  | { kind: 'num'; value: number }
  | { kind: 'str'; value: string }
  | { kind: 'null'; value: null };

type OrderedOperator = '_gt' | '_gte' | '_lt' | '_lte';

function toComparable(v: unknown): Comparable {
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

function compareOrderedValues<T extends number | string>(
  op: OrderedOperator,
  actual: T,
  expected: T,
): boolean {
  switch (op) {
    case '_gt':
      return actual > expected;
    case '_gte':
      return actual >= expected;
    case '_lt':
      return actual < expected;
    case '_lte':
      return actual <= expected;
  }
}

function compareNumericOrdered(
  op: OrderedOperator,
  actual: unknown,
  expected: unknown,
): boolean {
  if (actual == null || expected == null) return false;
  const a = toComparable(actual);
  const b = toComparable(expected);
  if (a.kind === 'num' && b.kind === 'num')
    return compareOrderedValues(op, a.value, b.value);
  if (a.kind === 'str' && b.kind === 'str')
    return compareOrderedValues(op, a.value, b.value);
  return false;
}

function equalsLoose(a: unknown, b: unknown): boolean {
  if (a == null && b == null) return true;
  if (a == null || b == null) return false;
  const sa = toIdString(a);
  const sb = toIdString(b);
  return sa === sb;
}

function matchesOperator(
  op: string,
  rawExpected: unknown,
  actual: unknown,
  user: unknown,
): boolean {
  switch (op) {
    case '_eq':
    case '_neq': {
      const expected = resolveMacro(rawExpected, user);
      if (expected === undefined) return false;
      const equal = equalsLoose(actual, expected);
      return op === '_eq' ? equal : !equal;
    }
    case '_gt':
    case '_gte':
    case '_lt':
    case '_lte': {
      const expected = resolveMacro(rawExpected, user);
      if (expected === undefined) return false;
      return compareNumericOrdered(op, actual, expected);
    }
    case '_in':
    case '_not_in':
    case '_nin': {
      const expected = isUnknownArray(rawExpected)
        ? resolveMacroInArray(rawExpected, user)
        : resolveMacro(rawExpected, user);
      if (!isUnknownArray(expected)) return false;
      const includes = expected.some((value) => equalsLoose(actual, value));
      return op === '_in' ? includes : !includes;
    }
    case '_is_null': {
      const want = rawExpected === true || rawExpected === 'true';
      return want === (actual === null || actual === undefined);
    }
    case '_is_not_null': {
      const want = rawExpected === true || rawExpected === 'true';
      return want === (actual !== null && actual !== undefined);
    }
    default:
      return false;
  }
}

function evalOperatorNode(
  node: Record<string, unknown>,
  actual: unknown,
  user: unknown,
): boolean {
  for (const op of Object.keys(node)) {
    if (!FIELD_OPERATORS.has(op)) return false;
    if (!matchesOperator(op, node[op], actual, user)) return false;
  }
  return true;
}

type ConditionMatcher = (record: unknown, user: unknown) => boolean;

const preparedConditions = new WeakMap<object, ConditionMatcher>();
const neverMatches: ConditionMatcher = () => false;

function collectConditionObjects(condition: object): object[] | null {
  const objects: object[] = [];
  const visiting = new Set<object>();
  const visited = new Set<object>();
  const pending: Array<{ value: unknown; exit: boolean }> = [
    { value: condition, exit: false },
  ];
  while (pending.length > 0) {
    const entry = pending.pop();
    if (entry === undefined) break;
    const { value, exit } = entry;
    if (
      value == null ||
      ['string', 'number', 'boolean', 'undefined'].includes(typeof value)
    )
      continue;
    if (typeof value !== 'object') return null;
    if (exit) {
      visiting.delete(value);
      visited.add(value);
      continue;
    }
    if (visiting.has(value)) return null;
    if (visited.has(value)) continue;
    if (types.isProxy(value)) return null;
    const prototype: unknown = Object.getPrototypeOf(value);
    if (
      Array.isArray(value)
        ? prototype !== Array.prototype
        : prototype !== Object.prototype && prototype !== null
    )
      return null;
    visiting.add(value);
    objects.push(value);
    pending.push({ value, exit: true });
    for (const key of Reflect.ownKeys(value)) {
      if (typeof key !== 'string') return null;
      const descriptor = Object.getOwnPropertyDescriptor(value, key);
      if (!descriptor || !('value' in descriptor)) return null;
      if (!descriptor.enumerable) {
        if (Array.isArray(value) && key === 'length') continue;
        return null;
      }
      const child: unknown = descriptor.value;
      pending.push({ value: child, exit: false });
    }
  }
  return objects;
}

function compileConditionEntry(key: string, node: unknown): ConditionMatcher {
  if (key === '_and' || key === '_or') {
    if (!isUnknownArray(node) || node.length === 0) return neverMatches;
    const children = node.map(compileCondition);
    return key === '_and'
      ? (record, user) => children.every((match) => match(record, user))
      : (record, user) => children.some((match) => match(record, user));
  }
  if (key === '_not') {
    if (!isPlainObject(node)) return neverMatches;
    const match = compileCondition(node);
    return (record, user) => !match(record, user);
  }
  if (key.startsWith('_')) return neverMatches;
  const path = key.split('.');
  if (isOperatorNode(node)) {
    const operators = Object.keys(node);
    const expected = operators.map((op) => node[op]);
    return (record, user) => {
      const actual = resolvePathParts(record, path);
      for (const [index, operator] of operators.entries()) {
        if (!matchesOperator(operator, expected[index], actual, user))
          return false;
      }
      return true;
    };
  }
  if (!isPlainObject(node)) {
    return (record) => {
      resolvePathParts(record, path);
      return false;
    };
  }
  const match = compileCondition(node);
  return (record, user) => {
    const actual = resolvePathParts(record, path);
    return (
      actual != null &&
      typeof actual === 'object' &&
      !Array.isArray(actual) &&
      match(actual, user)
    );
  };
}

function compileCondition(condition: unknown): ConditionMatcher {
  if (!isPlainObject(condition)) return neverMatches;
  const matchers = Object.keys(condition).map((key) =>
    compileConditionEntry(key, condition[key]),
  );
  return (record, user) => {
    for (const match of matchers) {
      if (!match(record, user)) return false;
    }
    return true;
  };
}

export function prepareFieldPermissionCondition(condition: unknown): void {
  if (!isPlainObject(condition) || preparedConditions.has(condition)) return;
  const objects = collectConditionObjects(condition);
  if (!objects) return;
  const match = compileCondition(condition);
  for (const object of objects) Object.freeze(object);
  preparedConditions.set(condition, match);
}

export function getFieldPermissionConditionFields(
  condition: unknown,
): string[] {
  if (!isPlainObject(condition)) return [];
  const fields = new Set<string>();
  for (const [key, node] of Object.entries(condition)) {
    if (key === '_and' || key === '_or') {
      if (isUnknownArray(node)) {
        for (const child of node) {
          for (const field of getFieldPermissionConditionFields(child))
            fields.add(field);
        }
      }
    } else if (key === '_not') {
      for (const field of getFieldPermissionConditionFields(node))
        fields.add(field);
    } else if (!key.startsWith('_')) {
      const children = isOperatorNode(node)
        ? []
        : getFieldPermissionConditionFields(node);
      if (children.length === 0) fields.add(key);
      for (const field of children) fields.add(`${key}.${field}`);
    }
  }
  return [...fields];
}

export function matchFieldPermissionCondition(
  condition: unknown,
  record: unknown,
  user: unknown,
): boolean {
  if (!isPlainObject(condition)) return false;
  const prepared = preparedConditions.get(condition);
  if (prepared) return prepared(record, user);

  for (const key of Object.keys(condition)) {
    const node = condition[key];

    if (key === '_and') {
      if (!isUnknownArray(node) || node.length === 0) return false;
      if (!node.every((c) => matchFieldPermissionCondition(c, record, user)))
        return false;
      continue;
    }
    if (key === '_or') {
      if (!isUnknownArray(node) || node.length === 0) return false;
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
