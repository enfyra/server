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
  if (typeof v === 'string') return v;
  if (typeof v === 'number') return String(v);
  if (typeof v === 'boolean') return v ? 'true' : 'false';
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
  return resolvePathParts(root, path.split('.'));
}

function resolvePathParts(root: any, parts: string[]): any {
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

function matchesOperator(
  op: string,
  rawExpected: any,
  actual: any,
  user: any,
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
      const short = op.slice(1) as 'gt' | 'gte' | 'lt' | 'lte';
      return compareNumericOrdered(short, actual, expected);
    }
    case '_in':
    case '_not_in':
    case '_nin': {
      const expected = Array.isArray(rawExpected)
        ? resolveMacroInArray(rawExpected, user)
        : resolveMacro(rawExpected, user);
      if (!Array.isArray(expected)) return false;
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
  node: Record<string, any>,
  actual: any,
  user: any,
): boolean {
  for (const op of Object.keys(node)) {
    if (!FIELD_OPERATORS.has(op)) return false;
    if (!matchesOperator(op, node[op], actual, user)) return false;
  }
  return true;
}

type ConditionMatcher = (record: any, user: any) => boolean;

const preparedConditions = new WeakMap<object, ConditionMatcher>();
const neverMatches: ConditionMatcher = () => false;

function collectConditionObjects(condition: object): object[] | null {
  const objects: object[] = [];
  const visiting = new Set<object>();
  const visited = new Set<object>();
  const pending = [{ value: condition as unknown, exit: false }];
  while (pending.length > 0) {
    const { value, exit } = pending.pop()!;
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
    const prototype = Object.getPrototypeOf(value);
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
      const descriptor = Object.getOwnPropertyDescriptor(value, key)!;
      if (!('value' in descriptor)) return null;
      if (!descriptor.enumerable) {
        if (Array.isArray(value) && key === 'length') continue;
        return null;
      }
      pending.push({ value: descriptor.value, exit: false });
    }
  }
  return objects;
}

function compileConditionEntry(key: string, node: any): ConditionMatcher {
  if (key === '_and' || key === '_or') {
    if (!Array.isArray(node) || node.length === 0) return neverMatches;
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
      for (let index = 0; index < operators.length; index++) {
        if (!matchesOperator(operators[index], expected[index], actual, user))
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

function compileCondition(condition: any): ConditionMatcher {
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
  if (!isPlainObject(condition) || preparedConditions.has(condition as object))
    return;
  const objects = collectConditionObjects(condition as object);
  if (!objects) return;
  const match = compileCondition(condition);
  for (const object of objects) Object.freeze(object);
  preparedConditions.set(condition as object, match);
}

export function getFieldPermissionConditionFields(
  condition: unknown,
): string[] {
  if (!isPlainObject(condition)) return [];
  const fields = new Set<string>();
  for (const [key, node] of Object.entries(
    condition as Record<string, unknown>,
  )) {
    if (key === '_and' || key === '_or') {
      if (Array.isArray(node)) {
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
  condition: any,
  record: any,
  user: any,
): boolean {
  if (!isPlainObject(condition)) return false;
  const prepared = preparedConditions.get(condition);
  if (prepared) return prepared(record, user);

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
