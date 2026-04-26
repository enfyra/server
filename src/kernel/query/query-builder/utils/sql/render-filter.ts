import { Knex } from 'knex';
import {
  FilterNode,
  ComparisonNode,
  FieldRef,
} from '../../../query-dsl/types/filter-ast';

export interface SqlRenderContext {
  dbType: 'postgres' | 'mysql' | 'sqlite';
  rootTable: string;
}

export function renderFilterToKnex(
  query: Knex.QueryBuilder,
  node: FilterNode | null,
  ctx: SqlRenderContext,
): void {
  if (!node) return;
  applyNode(query, node, ctx, 'and');
}

function applyNode(
  query: Knex.QueryBuilder,
  node: FilterNode,
  ctx: SqlRenderContext,
  combine: 'and' | 'or',
): void {
  switch (node.kind) {
    case 'true':
      return;
    case 'false':
      if (combine === 'or') query.orWhereRaw('1 = 0');
      else query.whereRaw('1 = 0');
      return;
    case 'and': {
      const fn = function (this: Knex.QueryBuilder) {
        for (const c of node.children) {
          this.where(function () {
            applyNode(this as any, c, ctx, 'and');
          });
        }
      };
      if (combine === 'or') query.orWhere(fn);
      else query.where(fn);
      return;
    }
    case 'or': {
      const fn = function (this: Knex.QueryBuilder) {
        for (const c of node.children) {
          this.orWhere(function () {
            applyNode(this as any, c, ctx, 'and');
          });
        }
      };
      if (combine === 'or') query.orWhere(fn);
      else query.where(fn);
      return;
    }
    case 'not': {
      const fn = function (this: Knex.QueryBuilder) {
        this.whereNot(function () {
          applyNode(this as any, node.child, ctx, 'and');
        });
      };
      if (combine === 'or') query.orWhere(fn);
      else query.where(fn);
      return;
    }
    case 'compare':
      applyComparison(query, node, ctx, combine);
      return;
    case 'relation_exists':
      console.warn(
        `[sql-render-filter] relation_exists node reached renderer directly (rootTable=${ctx.rootTable}, joinId=${node.joinId}) — expected to be handled by applyRelationFilters. Filter silently skipped.`,
      );
      return;
  }
}

function applyComparison(
  query: Knex.QueryBuilder,
  node: ComparisonNode,
  ctx: SqlRenderContext,
  combine: 'and' | 'or',
): void {
  if (node.field.joinId !== null) {
    console.warn(
      `[sql-render-filter] compare node has joinId=${node.field.joinId} (field=${node.field.fieldName}) but reached renderer — expected join path to handle it. Filter silently skipped.`,
    );
    return;
  }
  const fullField = resolveFieldName(node.field, ctx);
  const isUuid = !!node.field.isUuid;
  const isPg = ctx.dbType === 'postgres';
  const where = combine === 'or' ? 'orWhere' : 'where';
  const whereRaw = combine === 'or' ? 'orWhereRaw' : 'whereRaw';
  const whereIn = combine === 'or' ? 'orWhereIn' : 'whereIn';
  const whereNotIn = combine === 'or' ? 'orWhereNotIn' : 'whereNotIn';
  const whereBetween = combine === 'or' ? 'orWhereBetween' : 'whereBetween';
  const whereNull = combine === 'or' ? 'orWhereNull' : 'whereNull';
  const whereNotNull = combine === 'or' ? 'orWhereNotNull' : 'whereNotNull';

  switch (node.op) {
    case 'eq':
      if (isUuid && isPg && typeof node.value === 'string') {
        (query as any)[whereRaw](`${fullField} = ?::uuid`, [node.value]);
      } else {
        (query as any)[where](fullField, '=', node.value);
      }
      return;
    case 'neq':
      if (isUuid && isPg && typeof node.value === 'string') {
        (query as any)[whereRaw](`${fullField} != ?::uuid`, [node.value]);
      } else {
        (query as any)[where](fullField, '!=', node.value);
      }
      return;
    case 'gt':
      if (isUuid && isPg && typeof node.value === 'string') {
        (query as any)[whereRaw](`${fullField} > ?::uuid`, [node.value]);
      } else {
        (query as any)[where](fullField, '>', node.value);
      }
      return;
    case 'gte':
      if (isUuid && isPg && typeof node.value === 'string') {
        (query as any)[whereRaw](`${fullField} >= ?::uuid`, [node.value]);
      } else {
        (query as any)[where](fullField, '>=', node.value);
      }
      return;
    case 'lt':
      if (isUuid && isPg && typeof node.value === 'string') {
        (query as any)[whereRaw](`${fullField} < ?::uuid`, [node.value]);
      } else {
        (query as any)[where](fullField, '<', node.value);
      }
      return;
    case 'lte':
      if (isUuid && isPg && typeof node.value === 'string') {
        (query as any)[whereRaw](`${fullField} <= ?::uuid`, [node.value]);
      } else {
        (query as any)[where](fullField, '<=', node.value);
      }
      return;
    case 'in': {
      const arr = normalizeArray(node.value);
      if (isUuid && isPg && arr.every((v: any) => typeof v === 'string')) {
        (query as any)[whereRaw](`${fullField} = ANY(?::uuid[])`, [arr]);
      } else {
        (query as any)[whereIn](fullField, arr);
      }
      return;
    }
    case 'not_in': {
      const arr = normalizeArray(node.value);
      if (isUuid && isPg && arr.every((v: any) => typeof v === 'string')) {
        (query as any)[whereRaw](`${fullField} != ALL(?::uuid[])`, [arr]);
      } else {
        (query as any)[whereNotIn](fullField, arr);
      }
      return;
    }
    case 'contains':
      applyTextSearch(query, fullField, node.value, 'contains', ctx, combine);
      return;
    case 'starts_with':
      applyTextSearch(
        query,
        fullField,
        node.value,
        'starts_with',
        ctx,
        combine,
      );
      return;
    case 'ends_with':
      applyTextSearch(query, fullField, node.value, 'ends_with', ctx, combine);
      return;
    case 'between':
      if (Array.isArray(node.value) && node.value.length === 2) {
        (query as any)[whereBetween](fullField, [node.value[0], node.value[1]]);
      }
      return;
    case 'is_null':
      if (node.value === true) (query as any)[whereNull](fullField);
      else (query as any)[whereNotNull](fullField);
      return;
  }
}

function applyTextSearch(
  query: Knex.QueryBuilder,
  fullField: string,
  value: any,
  mode: 'contains' | 'starts_with' | 'ends_with',
  ctx: SqlRenderContext,
  combine: 'and' | 'or',
): void {
  const whereRaw = combine === 'or' ? 'orWhereRaw' : 'whereRaw';

  if (ctx.dbType === 'postgres') {
    const tpl =
      mode === 'contains'
        ? `lower(unaccent(${fullField})) ILIKE '%' || lower(unaccent(?)) || '%'`
        : mode === 'starts_with'
          ? `lower(unaccent(${fullField})) ILIKE lower(unaccent(?)) || '%'`
          : `lower(unaccent(${fullField})) ILIKE '%' || lower(unaccent(?))`;
    (query as any)[whereRaw](tpl, [value]);
    return;
  }

  if (ctx.dbType === 'sqlite') {
    const tpl =
      mode === 'contains'
        ? `lower(${fullField}) LIKE '%' || lower(?) || '%'`
        : mode === 'starts_with'
          ? `lower(${fullField}) LIKE lower(?) || '%'`
          : `lower(${fullField}) LIKE '%' || lower(?)`;
    (query as any)[whereRaw](tpl, [value]);
    return;
  }

  const tpl =
    mode === 'contains'
      ? `lower(unaccent(${fullField})) COLLATE utf8mb4_general_ci LIKE CONCAT('%', lower(unaccent(?)) COLLATE utf8mb4_general_ci, '%')`
      : mode === 'starts_with'
        ? `lower(unaccent(${fullField})) COLLATE utf8mb4_general_ci LIKE CONCAT(lower(unaccent(?)) COLLATE utf8mb4_general_ci, '%')`
        : `lower(unaccent(${fullField})) COLLATE utf8mb4_general_ci LIKE CONCAT('%', lower(unaccent(?)) COLLATE utf8mb4_general_ci)`;
  (query as any)[whereRaw](tpl, [value]);
}

function resolveFieldName(field: FieldRef, ctx: SqlRenderContext): string {
  if (field.fieldName.includes('.')) return field.fieldName;
  return `${ctx.rootTable}.${field.fieldName}`;
}

function normalizeArray(value: any): any[] {
  if (Array.isArray(value)) return value;
  if (typeof value === 'string' && value.includes(',')) {
    return value.split(',').map((v) => v.trim());
  }
  return [value];
}
