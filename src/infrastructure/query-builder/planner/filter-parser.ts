import { JoinRegistry } from './join-registry';
import {
  FilterNode,
  ComparisonNode,
  FieldRef,
  FIELD_OPERATORS,
  dslOpToCompareOp,
  ComparisonOp,
} from './types/filter-ast';
import { throwUnsupportedFieldOperator } from '../utils/shared/filter-sanitizer.util';

export interface FilterParseContext {
  tableName: string;
  metadata: any;
  registry: JoinRegistry;
  parentJoinId: string | null;
  currentJoinId: string | null;
  currentTable: string;
}

export interface FilterParseResult {
  node: FilterNode | null;
  hasRelationFilters: boolean;
}

export function parseFilter(
  raw: any,
  tableName: string,
  metadata: any,
  registry: JoinRegistry,
): FilterParseResult {
  if (!raw || typeof raw !== 'object') {
    return { node: null, hasRelationFilters: false };
  }
  const ctx: FilterParseContext = {
    tableName,
    metadata,
    registry,
    parentJoinId: null,
    currentJoinId: null,
    currentTable: tableName,
  };
  return parseObject(raw, ctx);
}

function parseObject(obj: any, ctx: FilterParseContext): FilterParseResult {
  if (!obj || typeof obj !== 'object') {
    return { node: null, hasRelationFilters: false };
  }

  const children: FilterNode[] = [];
  let hasRelationFilters = false;

  for (const [key, value] of Object.entries(obj)) {
    const result = parseEntry(key, value, ctx);
    if (result.hasRelationFilters) hasRelationFilters = true;
    if (result.node) children.push(result.node);
  }

  if (children.length === 0) return { node: null, hasRelationFilters };
  if (children.length === 1) return { node: children[0], hasRelationFilters };
  return { node: { kind: 'and', children }, hasRelationFilters };
}

function parseEntry(
  key: string,
  value: any,
  ctx: FilterParseContext,
): FilterParseResult {
  if (key === '_and') {
    return parseLogicalArray('and', value, ctx);
  }
  if (key === '_or') {
    return parseLogicalArray('or', value, ctx);
  }
  if (key === '_not') {
    const inner = parseObject(value, ctx);
    if (!inner.node)
      return { node: null, hasRelationFilters: inner.hasRelationFilters };
    return {
      node: { kind: 'not', child: inner.node },
      hasRelationFilters: inner.hasRelationFilters,
    };
  }

  const tableMeta = ctx.metadata?.tables?.get(ctx.currentTable);
  const hasMongoId = tableMeta?.columns?.some(
    (c: any) => c.name === '_id' && c.isPrimary,
  );
  const hasSqlId = tableMeta?.columns?.some((c: any) => c.name === 'id');
  const pkAlias = hasMongoId && !hasSqlId ? '_id' : 'id';
  const normalizedKey = key === 'id' ? pkAlias : key;

  const relation = tableMeta?.relations?.find(
    (r: any) => r.propertyName === normalizedKey,
  );

  if (relation) {
    return parseRelationEntry(normalizedKey, value, relation, ctx);
  }

  if (tableMeta) {
    const column = tableMeta.columns?.find(
      (c: any) => c.name === normalizedKey,
    );
    if (!column) {
      if (key.startsWith('_')) {
        if (key !== '_id') {
          throwUnsupportedFieldOperator(key, key, ctx.currentTable);
        }
      }
      console.warn(
        `[filter-parser] Unknown field "${key}" on table "${ctx.currentTable}" — skipped (not a column or relation in metadata).`,
      );
      return { node: null, hasRelationFilters: false };
    }
  }

  return parseFieldEntry(normalizedKey, value, ctx);
}

function parseLogicalArray(
  kind: 'and' | 'or',
  value: any,
  ctx: FilterParseContext,
): FilterParseResult {
  if (!Array.isArray(value)) return { node: null, hasRelationFilters: false };
  const children: FilterNode[] = [];
  let hasRelationFilters = false;
  for (const item of value) {
    const result = parseObject(item, ctx);
    if (result.hasRelationFilters) hasRelationFilters = true;
    if (result.node) children.push(result.node);
  }
  if (children.length === 0) return { node: null, hasRelationFilters };
  if (children.length === 1) return { node: children[0], hasRelationFilters };
  return { node: { kind, children }, hasRelationFilters };
}

function parseRelationEntry(
  relationName: string,
  value: any,
  relation: any,
  ctx: FilterParseContext,
): FilterParseResult {
  if (typeof value !== 'object' || value === null) {
    return { node: null, hasRelationFilters: true };
  }

  const keys = Object.keys(value);
  const isExplicitNullCheck =
    keys.length === 1 &&
    (keys[0] === '_is_null' ||
      keys[0] === '_is_not_null' ||
      keys[0] === '_eq' ||
      keys[0] === '_neq' ||
      keys[0] === '_in' ||
      keys[0] === '_not_in' ||
      keys[0] === '_nin');

  const joinId = ctx.registry.registerWithParent(
    ctx.currentTable,
    relationName,
    ctx.metadata,
    'left',
    ctx.currentJoinId,
    'filter',
  );

  if (isExplicitNullCheck && keys[0] === '_is_null') {
    if (!joinId) return { node: null, hasRelationFilters: true };
    return {
      node: {
        kind: 'relation_exists',
        joinId,
        negate: value._is_null === true,
      },
      hasRelationFilters: true,
    };
  }
  if (isExplicitNullCheck && keys[0] === '_is_not_null') {
    if (!joinId) return { node: null, hasRelationFilters: true };
    return {
      node: {
        kind: 'relation_exists',
        joinId,
        negate: value._is_not_null !== true,
      },
      hasRelationFilters: true,
    };
  }

  if (
    isExplicitNullCheck &&
    (keys[0] === '_eq' || keys[0] === '_neq') &&
    value[keys[0]] === null
  ) {
    if (!joinId) return { node: null, hasRelationFilters: true };
    return {
      node: { kind: 'relation_exists', joinId, negate: keys[0] === '_eq' },
      hasRelationFilters: true,
    };
  }

  if (!joinId) return { node: null, hasRelationFilters: true };

  const targetTable = relation.targetTableName || relation.targetTable;
  const nestedCtx: FilterParseContext = {
    ...ctx,
    parentJoinId: ctx.currentJoinId,
    currentJoinId: joinId,
    currentTable: targetTable,
  };

  const idImplicitOps = new Set([
    '_eq',
    '_neq',
    '_in',
    '_not_in',
    '_nin',
    '_gt',
    '_gte',
    '_lt',
    '_lte',
  ]);
  const allKeysAreIdImplicit =
    keys.length > 0 && keys.every((k) => idImplicitOps.has(k));
  const effectiveValue = allKeysAreIdImplicit ? { id: value } : value;

  const inner = parseObject(effectiveValue, nestedCtx);
  return { node: inner.node, hasRelationFilters: true };
}

function parseFieldEntry(
  fieldName: string,
  value: any,
  ctx: FilterParseContext,
): FilterParseResult {
  const fieldRef: FieldRef = {
    joinId: ctx.currentJoinId,
    fieldName,
    isUuid: detectUuid(fieldName, ctx),
  };

  if (value === null) {
    return {
      node: {
        kind: 'compare',
        field: fieldRef,
        op: 'is_null',
        value: true,
      },
      hasRelationFilters: false,
    };
  }

  if (typeof value !== 'object') {
    return {
      node: {
        kind: 'compare',
        field: fieldRef,
        op: 'eq',
        value,
      },
      hasRelationFilters: false,
    };
  }

  for (const k of Object.keys(value)) {
    if (k.startsWith('_') && !FIELD_OPERATORS.has(k)) {
      throwUnsupportedFieldOperator(k, fieldName, ctx.tableName);
    }
  }

  const opEntries = Object.entries(value).filter(([k]) =>
    FIELD_OPERATORS.has(k),
  );

  if (opEntries.length === 0) {
    return {
      node: {
        kind: 'compare',
        field: fieldRef,
        op: 'eq',
        value,
      },
      hasRelationFilters: false,
    };
  }

  const compareNodes: FilterNode[] = [];
  for (const [opKey, opValue] of opEntries) {
    const node = buildComparisonNode(fieldRef, opKey, opValue);
    if (node) compareNodes.push(node);
  }

  if (compareNodes.length === 0)
    return { node: null, hasRelationFilters: false };
  if (compareNodes.length === 1)
    return { node: compareNodes[0], hasRelationFilters: false };
  return {
    node: { kind: 'and', children: compareNodes },
    hasRelationFilters: false,
  };
}

function buildComparisonNode(
  field: FieldRef,
  dslOp: string,
  value: any,
): ComparisonNode | null {
  if (dslOp === '_is_null') {
    return { kind: 'compare', field, op: 'is_null', value: value === true };
  }
  if (dslOp === '_is_not_null') {
    return { kind: 'compare', field, op: 'is_null', value: value !== true };
  }
  const op = dslOpToCompareOp(dslOp);
  if (!op) return null;
  return { kind: 'compare', field, op, value };
}

function detectUuid(fieldName: string, ctx: FilterParseContext): boolean {
  const tableMeta = ctx.metadata?.tables?.get(ctx.currentTable);
  if (!tableMeta) return false;
  const cleanName = fieldName.includes('.')
    ? fieldName.split('.').pop()!
    : fieldName;
  const col = tableMeta.columns?.find((c: any) => c.name === cleanName);
  if (!col) return false;
  const t = (col.type || '').toLowerCase();
  return t === 'uuid' || t === 'uuidv4' || t.includes('uuid');
}
