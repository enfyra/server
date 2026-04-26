import { MetadataCacheService } from '../../engine/cache';
import { FieldPermissionCacheService } from '../../engine/cache';
import { decideFieldPermission } from './field-permission.util';

type TRequestedShape = {
  includeAll?: boolean;
  columns?: Set<string>;
  relations?: Set<string>;
};

function parseRequestedFields(fields: any): {
  includeAll: boolean;
  set: Set<string>;
} {
  if (!fields) return { includeAll: false, set: new Set() };
  const arr = Array.isArray(fields)
    ? fields
    : String(fields)
        .split(',')
        .map((s) => s.trim())
        .filter(Boolean);
  const set = new Set<string>();
  let includeAll = false;
  for (const f of arr) {
    if (f === '*') includeAll = true;
    set.add(String(f).split('.')[0]);
  }
  return { includeAll, set };
}

export async function sanitizeFieldPermissionsResult(params: {
  value: any;
  tableName: string;
  user: any;
  action: 'read';
  metadataCacheService: MetadataCacheService;
  fieldPermissionCacheService: FieldPermissionCacheService;
  requested?: TRequestedShape;
}): Promise<any> {
  const {
    value,
    tableName,
    user,
    action,
    metadataCacheService,
    fieldPermissionCacheService,
    requested,
  } = params;

  const meta = metadataCacheService.getDirectMetadata();
  const tableMeta = meta?.tables?.get?.(tableName) || null;

  const walk = async (
    node: any,
    currentTable: string,
    currentRequested: TRequestedShape | undefined,
  ): Promise<any> => {
    if (node == null) return node;
    if (Array.isArray(node)) {
      return Promise.all(
        node.map((item) => walk(item, currentTable, currentRequested)),
      );
    }
    if (
      typeof node !== 'object' ||
      node instanceof Date ||
      Buffer.isBuffer(node)
    ) {
      return node;
    }

    const direct = meta?.tables?.get?.(currentTable) || tableMeta;
    if (!direct) return node;

    const out: any = { ...node };
    const includeAll = currentRequested?.includeAll === true;

    for (const col of direct.columns || []) {
      const colName = col?.name;
      if (!colName) continue;
      const wasRequested =
        includeAll ||
        (currentRequested?.columns?.has(colName) ?? false) ||
        colName in out;
      if (!wasRequested) continue;

      const published = col?.isPublished !== false;
      const decision = published
        ? { allowed: true }
        : await decideFieldPermission(
            fieldPermissionCacheService,
            {
              user,
              tableName: currentTable,
              action,
              subjectType: 'column',
              subjectName: colName,
              record: out,
            },
            { defaultAllowed: false },
          );

      if (!decision.allowed) {
        delete out[colName];
      }
    }

    for (const rel of direct.relations || []) {
      const relName = rel?.propertyName;
      if (!relName) continue;

      const wasRequested =
        includeAll ||
        (currentRequested?.relations?.has(relName) ?? false) ||
        relName in out;
      if (!wasRequested) continue;

      const published = rel?.isPublished !== false;
      const decision = published
        ? { allowed: true }
        : await decideFieldPermission(
            fieldPermissionCacheService,
            {
              user,
              tableName: currentTable,
              action,
              subjectType: 'relation',
              subjectName: relName,
              record: out,
            },
            { defaultAllowed: false },
          );

      if (!decision.allowed) {
        delete out[relName];
        continue;
      }

      const targetTable = rel.targetTableName || rel.targetTable;
      if (!targetTable) continue;

      const childRequested: TRequestedShape | undefined = undefined;
      if (relName in out) {
        out[relName] = await walk(out[relName], targetTable, childRequested);
      }
    }

    return out;
  };

  return await walk(value, tableName, requested);
}

export function buildRequestedShapeFromQuery(opts: {
  fields: any;
  deep: any;
}): TRequestedShape {
  const { fields, deep } = opts;
  const parsed = parseRequestedFields(fields);
  const relations = new Set<string>();
  if (deep && typeof deep === 'object') {
    for (const k of Object.keys(deep)) relations.add(k);
  }
  return {
    includeAll: parsed.includeAll,
    columns: parsed.set,
    relations,
  };
}
