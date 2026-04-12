import {
  FilterNode,
  ComparisonNode,
  ComparisonOp,
} from '../../planner/types/filter-ast';
import { getMongoFoldTextSearchJs } from '../../../../shared/utils/mongo-fold-text-search';
import { convertValueByType } from './type-converter';

const MONGO_FOLD_PENDING = '__mongoFoldTextTriples';

type FoldTriple = {
  ref: string;
  needle: string;
  mode: 'contains' | 'starts' | 'ends';
};

export interface MongoRenderContext {
  metadata: any;
  rootTable: string;
}

export function renderFilterToMongo(
  node: FilterNode | null,
  ctx: MongoRenderContext,
): any {
  if (!node) return {};
  const result: any = {};
  applyNode(node, result, ctx);
  flushFoldTextSearchExpr(result);
  return result;
}

function applyNode(
  node: FilterNode,
  container: any,
  ctx: MongoRenderContext,
): void {
  switch (node.kind) {
    case 'true':
      return;
    case 'false':
      mergeAnd(container, { _id: { $exists: false, $type: 'null' } });
      return;
    case 'and': {
      const subs = node.children.map((c) => {
        const sub: any = {};
        applyNode(c, sub, ctx);
        flushFoldTextSearchExpr(sub);
        return sub;
      });
      const filtered = subs.filter((s) => Object.keys(s).length > 0);
      if (filtered.length === 0) return;
      if (filtered.length === 1) {
        Object.assign(container, filtered[0]);
        return;
      }
      mergeAnd(container, ...filtered);
      return;
    }
    case 'or': {
      const subs = node.children.map((c) => {
        const sub: any = {};
        applyNode(c, sub, ctx);
        flushFoldTextSearchExpr(sub);
        return sub;
      });
      const filtered = subs.filter((s) => Object.keys(s).length > 0);
      if (filtered.length === 0) return;
      const existing = container.$or as any[] | undefined;
      container.$or = existing ? [...existing, ...filtered] : filtered;
      return;
    }
    case 'not': {
      const sub: any = {};
      applyNode(node.child, sub, ctx);
      flushFoldTextSearchExpr(sub);
      const existing = container.$nor as any[] | undefined;
      container.$nor = existing ? [...existing, sub] : [sub];
      return;
    }
    case 'compare':
      applyComparison(node, container, ctx);
      return;
    case 'relation_exists': {
      return;
    }
  }
}

function applyComparison(
  node: ComparisonNode,
  container: any,
  ctx: MongoRenderContext,
): void {
  let fieldName = node.field.fieldName;
  if (fieldName === 'id') fieldName = '_id';

  if (node.field.joinId !== null) {
    return;
  }

  const v = (val: any) =>
    convertValueByType(ctx.metadata, ctx.rootTable, fieldName, val);

  switch (node.op) {
    case 'eq':
      assignField(container, fieldName, v(node.value));
      return;
    case 'neq': {
      const conv = v(node.value);
      if (isNullableColumn(ctx, fieldName)) {
        mergeAnd(container, { [fieldName]: { $ne: null } }, { [fieldName]: { $ne: conv } });
      } else {
        assignField(container, fieldName, { $ne: conv });
      }
      return;
    }
    case 'gt':
      assignField(container, fieldName, { $gt: v(node.value) });
      return;
    case 'gte':
      assignField(container, fieldName, { $gte: v(node.value) });
      return;
    case 'lt':
      assignField(container, fieldName, { $lt: v(node.value) });
      return;
    case 'lte':
      assignField(container, fieldName, { $lte: v(node.value) });
      return;
    case 'in': {
      const arr = normalizeArray(node.value).map(v);
      assignField(container, fieldName, { $in: arr });
      return;
    }
    case 'not_in': {
      const arr = normalizeArray(node.value).map(v);
      if (isNullableColumn(ctx, fieldName)) {
        mergeAnd(
          container,
          { [fieldName]: { $ne: null } },
          { [fieldName]: { $nin: arr } },
        );
      } else {
        assignField(container, fieldName, { $nin: arr });
      }
      return;
    }
    case 'between': {
      const arr = Array.isArray(node.value)
        ? node.value
        : typeof node.value === 'string'
          ? node.value.split(',').map((s: string) => s.trim())
          : null;
      if (Array.isArray(arr) && arr.length === 2) {
        assignField(container, fieldName, { $gte: v(arr[0]), $lte: v(arr[1]) });
      }
      return;
    }
    case 'is_null':
      assignField(
        container,
        fieldName,
        node.value === true ? { $eq: null } : { $ne: null },
      );
      return;
    case 'contains':
      appendFoldTextSearchExpr(container, fieldName, String(node.value), 'contains');
      return;
    case 'starts_with':
      appendFoldTextSearchExpr(container, fieldName, String(node.value), 'starts');
      return;
    case 'ends_with':
      appendFoldTextSearchExpr(container, fieldName, String(node.value), 'ends');
      return;
  }
}

function assignField(container: any, field: string, value: any): void {
  if (container[field] === undefined) {
    container[field] = value;
  } else {
    mergeAnd(container, { [field]: container[field] }, { [field]: value });
    delete container[field];
  }
}

function mergeAnd(container: any, ...clauses: any[]): void {
  if (clauses.length === 0) return;
  const existing = container.$and as any[] | undefined;
  container.$and = existing ? [...existing, ...clauses] : clauses;
}

function isNullableColumn(ctx: MongoRenderContext, field: string): boolean {
  const tableMeta = ctx.metadata?.tables?.get?.(ctx.rootTable);
  const col = tableMeta?.columns?.find((c: any) => c.name === field);
  if (!col) return true;
  return col.isNullable !== false;
}

function normalizeArray(value: any): any[] {
  if (Array.isArray(value)) return value;
  if (typeof value === 'string' && value.includes(',')) {
    return value.split(',').map((v) => v.trim());
  }
  return [value];
}

function mongoFieldRefForFoldExpr(fieldName: string): string {
  return fieldName === '_id' ? '$_id' : `$${fieldName}`;
}

function appendFoldTextSearchExpr(
  container: Record<string, unknown>,
  fieldName: string,
  needle: string,
  mode: 'contains' | 'starts' | 'ends',
): void {
  const ref = mongoFieldRefForFoldExpr(fieldName);
  const list =
    (container[MONGO_FOLD_PENDING] as FoldTriple[] | undefined) ?? [];
  list.push({ ref, needle, mode });
  container[MONGO_FOLD_PENDING] = list;
}

function flushFoldTextSearchExpr(container: Record<string, unknown>): void {
  const list = container[MONGO_FOLD_PENDING] as FoldTriple[] | undefined;
  delete container[MONGO_FOLD_PENDING];
  if (!list?.length) return;
  const args: unknown[] = [];
  for (const t of list) {
    args.push(t.ref, t.needle, t.mode);
  }
  const expr = {
    $eq: [
      {
        $function: {
          body: getMongoFoldTextSearchJs(),
          args,
          lang: 'js',
        },
      },
      true,
    ],
  };
  const prev = container.$expr;
  if (prev) {
    container.$expr = { $and: [prev, expr] };
  } else {
    container.$expr = expr;
  }
}
