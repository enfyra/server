import { z } from 'zod';
import { TColumnRule } from '../../infrastructure/cache/services/column-rule-cache.service';

export const ZOD_META_MAX_DEPTH = 10;

const AUTO_MANAGED_COLUMNS = new Set(['id', '_id', 'createdAt', 'updatedAt']);

export interface BuildZodOpts {
  tableMeta: any;
  mode: 'create' | 'update';
  rulesForColumn: (columnId: string | number) => TColumnRule[];
  getTableMetadata: (tableName: string) => any | null;
  visited?: Set<string>;
  depth?: number;
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
  return z.union([
    z.number(),
    z.string(),
    z.object({}).passthrough(),
  ]);
}

function buildColumnZod(
  col: any,
  mode: 'create' | 'update',
  rulesForColumn: (columnId: string | number) => TColumnRule[],
): z.ZodType | null {
  if (col.isGenerated || col.isPrimary) return null;
  if (AUTO_MANAGED_COLUMNS.has(col.name)) return null;
  if (mode === 'update' && col.isUpdatable === false) return null;

  const rules = (rulesForColumn(col.id) || []).filter((r) => r.isEnabled !== false);

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
        typeof col.options === 'object' && col.options !== null && typeof col.options.length === 'number'
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

  let requiredOverride = false;
  for (const rule of rules) {
    switch (rule.ruleType) {
      case 'required':
        requiredOverride = true;
        break;
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
  const hasDefault = col.defaultValue !== undefined && col.defaultValue !== null;
  const makeOptional =
    mode === 'update' || (!requiredOverride && (isNullable || hasDefault));
  if (makeOptional) s = s.optional();

  return s;
}

function buildRelationZod(
  rel: any,
  mode: 'create' | 'update',
  ctx: Required<
    Pick<BuildZodOpts, 'rulesForColumn' | 'getTableMetadata' | 'visited' | 'depth'>
  >,
): z.ZodType | null {
  if (mode === 'update' && rel.isUpdatable === false) return null;

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

  if (tableMeta?.name) visited.add(tableMeta.name);

  const shape: Record<string, z.ZodType> = {};

  for (const col of tableMeta?.columns || []) {
    const s = buildColumnZod(col, mode, rulesForColumn);
    if (s) shape[col.name] = s;
  }

  for (const rel of tableMeta?.relations || []) {
    const s = buildRelationZod(rel, mode, {
      rulesForColumn,
      getTableMetadata,
      visited,
      depth,
    });
    if (s) shape[rel.propertyName] = s;
  }

  return z.object(shape).strict();
}
