import { z } from 'zod';
import { TColumnRule } from '../../engines/cache';

export const ZOD_META_MAX_DEPTH = 10;

const AUTO_MANAGED_COLUMNS = new Set(['id', '_id', 'createdAt', 'updatedAt']);

/**
 * Virtual (non-column) fields that specific tables accept in POST/PATCH body.
 * These are handled by custom handler logic on the server (not persisted as
 * columns on the target table). Kept explicit to preserve strict validation
 * for everything else.
 */
const TABLE_VIRTUAL_FIELDS: Record<string, string[]> = {
  table_definition: ['graphqlEnabled'],
  field_permission_definition: ['config'],
};

export interface BuildZodOpts {
  tableMeta: any;
  mode: 'create' | 'update';
  rulesForColumn: (columnId: string | number) => TColumnRule[];
  getTableMetadata: (tableName: string) => any | null;
  visited?: Set<string>;
  depth?: number;
  skipChildRelationName?: string | null;
  strict?: boolean;
}

function connectByIdSchema(): z.ZodType {
  return z.union([
    z.number(),
    z.string(),
    z.object({ id: z.union([z.number(), z.string()]) }).passthrough(),
    z.object({ _id: z.union([z.number(), z.string()]) }).passthrough(),
  ]);
}

function looseSingleRelationSchema(): z.ZodType {
  return z.union([z.number(), z.string(), z.object({}).passthrough()]);
}

function buildColumnZod(
  col: any,
  mode: 'create' | 'update',
  rulesForColumn: (columnId: string | number) => TColumnRule[],
): z.ZodType | null {
  if (col.isPrimary) return null;
  if (col.isGenerated && col.isNullable === false) return z.any().optional();
  if (AUTO_MANAGED_COLUMNS.has(col.name)) return null;
  if (mode === 'update' && col.isUpdatable === false) return null;

  const columnId = col.id ?? col._id;
  const rules = (columnId != null ? rulesForColumn(columnId) : []).filter(
    (r) => r.isEnabled !== false,
  );

  let s: z.ZodType;
  switch (col.type) {
    case 'int':
    case 'bigint':
      s = z.number().int();
      break;
    case 'float':
    case 'decimal':
      s = z.number();
      break;
    case 'boolean':
      s = z.boolean();
      break;
    case 'varchar': {
      let base = z.string();
      const maxLen =
        typeof col.options === 'object' &&
        col.options !== null &&
        typeof col.options.length === 'number'
          ? col.options.length
          : null;
      if (maxLen !== null) base = base.max(maxLen);
      s = base;
      break;
    }
    case 'text':
    case 'richtext':
    case 'code':
      s = z.string();
      break;
    case 'date':
    case 'datetime':
    case 'timestamp':
      s = z.union([z.string(), z.date()]);
      break;
    case 'uuid':
      s = z.string();
      break;
    case 'enum': {
      const opts = Array.isArray(col.options) ? col.options : [];
      if (opts.length > 0) s = z.enum(opts as [string, ...string[]]);
      else s = z.string();
      break;
    }
    case 'array-select':
      s = z.array(z.string());
      break;
    case 'simple-json':
      s = z.any();
      break;
    default:
      s = z.any();
      break;
  }

  for (const rule of rules) {
    switch (rule.ruleType) {
      case 'min':
      case 'max': {
        const v = rule.value?.v;
        if (typeof v === 'number' && s instanceof z.ZodNumber) {
          s = rule.ruleType === 'min' ? s.min(v) : s.max(v);
        }
        break;
      }
      case 'minLength':
      case 'maxLength': {
        const v = rule.value?.v;
        if (typeof v === 'number' && s instanceof z.ZodString) {
          s = rule.ruleType === 'minLength' ? s.min(v) : s.max(v);
        }
        break;
      }
      case 'pattern': {
        const pat = rule.value?.v;
        const flags = rule.value?.flags;
        if (typeof pat === 'string' && s instanceof z.ZodString) {
          try {
            s = s.regex(new RegExp(pat, flags));
          } catch {}
        }
        break;
      }
      case 'format': {
        const fmt = rule.value?.v;
        if (s instanceof z.ZodString) {
          switch (fmt) {
            case 'email':
              s = s.email();
              break;
            case 'url':
              s = s.url();
              break;
            case 'uuid':
              s = s.uuid();
              break;
            case 'datetime':
              s = s.datetime();
              break;
          }
        }
        break;
      }
      case 'minItems':
      case 'maxItems': {
        const v = rule.value?.v;
        if (typeof v === 'number' && s instanceof z.ZodArray) {
          s = rule.ruleType === 'minItems' ? s.min(v) : s.max(v);
        }
        break;
      }
    }
  }

  const isNullable = col.isNullable !== false;
  if (isNullable) s = s.nullable();
  const hasDefault =
    col.defaultValue !== undefined && col.defaultValue !== null;
  const makeOptional = mode === 'update' || isNullable || hasDefault;
  if (makeOptional) s = s.optional();

  return s;
}

function buildRelationZod(
  rel: any,
  mode: 'create' | 'update',
  ctx: Required<
    Pick<
      BuildZodOpts,
      'rulesForColumn' | 'getTableMetadata' | 'visited' | 'depth'
    >
  >,
): z.ZodType | null {
  // NOTE: `isUpdatable=false` on a relation semantically means the LINK can't
  // be swapped out, but nested CRUD through the relation may still be allowed
  // (e.g. table_definition.columns is not-updatable as a link but server PATCH
  // handles nested column changes). Don't filter here; server enforces.

  const targetName = rel.targetTableName || rel.targetTable;
  const isInverse = rel.mappedBy || rel.isInverse;
  const targetMeta = targetName ? ctx.getTableMetadata(targetName) : null;

  const canCascade =
    !!targetMeta &&
    targetMeta.validateBody === true &&
    !ctx.visited.has(targetName) &&
    ctx.depth < ZOD_META_MAX_DEPTH;

  switch (rel.type) {
    case 'one-to-one': {
      if (isInverse) return z.any().nullable().optional();
      if (canCascade) {
        const nested = buildZodFromMetadata({
          tableMeta: targetMeta,
          mode: 'create',
          rulesForColumn: ctx.rulesForColumn,
          getTableMetadata: ctx.getTableMetadata,
          visited: new Set([...ctx.visited, targetName]),
          depth: ctx.depth + 1,
          skipChildRelationName: rel.mappedBy ?? null,
          strict: false,
        });
        return z.union([connectByIdSchema(), nested]).nullable().optional();
      }
      return looseSingleRelationSchema().nullable().optional();
    }
    case 'many-to-one': {
      if (canCascade) {
        const nested = buildZodFromMetadata({
          tableMeta: targetMeta,
          mode: 'create',
          rulesForColumn: ctx.rulesForColumn,
          getTableMetadata: ctx.getTableMetadata,
          visited: new Set([...ctx.visited, targetName]),
          depth: ctx.depth + 1,
          skipChildRelationName: rel.mappedBy ?? null,
          strict: false,
        });
        return z.union([connectByIdSchema(), nested]).nullable().optional();
      }
      return looseSingleRelationSchema().nullable().optional();
    }
    case 'many-to-many':
      return z.array(connectByIdSchema()).optional();
    case 'one-to-many': {
      if (canCascade) {
        const nested = buildZodFromMetadata({
          tableMeta: targetMeta,
          mode: 'create',
          rulesForColumn: ctx.rulesForColumn,
          getTableMetadata: ctx.getTableMetadata,
          visited: new Set([...ctx.visited, targetName]),
          depth: ctx.depth + 1,
          skipChildRelationName: rel.mappedBy ?? null,
          strict: false,
        });
        return z.array(z.union([connectByIdSchema(), nested])).optional();
      }
      return z.array(z.unknown()).optional();
    }
    default:
      return null;
  }
}

export function buildZodFromMetadata(opts: BuildZodOpts): z.ZodObject<any> {
  const { tableMeta, mode, rulesForColumn, getTableMetadata } = opts;
  const visited = opts.visited ?? new Set<string>();
  const depth = opts.depth ?? 0;
  const skipChildRelationName = opts.skipChildRelationName ?? null;

  if (tableMeta?.name) visited.add(tableMeta.name);

  // Admin UIs often echo auto-managed fields back in PATCH payloads. Accept
  // them silently (server ignores these; they're not user-writable).
  const autoManagedSchema: Record<string, z.ZodType> = {};
  for (const col of tableMeta?.columns || []) {
    if (AUTO_MANAGED_COLUMNS.has(col.name)) {
      autoManagedSchema[col.name] = z.any().optional();
    }
  }

  // Whitelisted virtual fields per system table (handled by custom handlers).
  const virtualFields = TABLE_VIRTUAL_FIELDS[tableMeta?.name] || [];
  for (const f of virtualFields) {
    autoManagedSchema[f] = z.any().optional();
  }

  // Collect FK columns to skip:
  // 1. Back-reference FK when cascading (server auto-sets)
  // 2. Every FK of owning relations — the relation propertyName is the public
  //    interface (user sends `column: {id}`, not `columnId`). relation-
  //    transformer derives FK from relation payload.
  const skipFkColumns = new Set<string>();
  if (skipChildRelationName) {
    const backRel = (tableMeta?.relations || []).find(
      (r: any) => r.propertyName === skipChildRelationName,
    );
    if (backRel?.foreignKeyColumn) skipFkColumns.add(backRel.foreignKeyColumn);
  }
  for (const rel of tableMeta?.relations || []) {
    if (rel?.foreignKeyColumn) skipFkColumns.add(rel.foreignKeyColumn);
  }

  const shape: Record<string, z.ZodType> = { ...autoManagedSchema };

  for (const col of tableMeta?.columns || []) {
    if (skipFkColumns.has(col.name)) continue;
    const s = buildColumnZod(col, mode, rulesForColumn);
    if (s) shape[col.name] = s;
  }

  for (const rel of tableMeta?.relations || []) {
    if (skipChildRelationName && rel.propertyName === skipChildRelationName)
      continue;
    const s = buildRelationZod(rel, mode, {
      rulesForColumn,
      getTableMetadata,
      visited,
      depth,
    });
    if (s) shape[rel.propertyName] = s;
  }

  const isStrict = opts.strict !== false;
  return isStrict ? z.object(shape).strict() : z.object(shape).passthrough();
}
