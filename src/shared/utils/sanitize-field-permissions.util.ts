import {
  decideFieldPermission,
  type TFieldPermissionPolicyReader,
} from './field-permission.util';
import type { TRequestedReadShape } from '../types/field-permission-read.types';

function parseRequestedFields(fields: any): {
  includeAll: boolean;
  set: Set<string>;
} {
  if (!fields || (Array.isArray(fields) && fields.length === 0)) {
    return { includeAll: true, set: new Set() };
  }
  const arr = Array.isArray(fields)
    ? fields
    : String(fields).split(',').map((s) => s.trim()).filter(Boolean);
  const set = new Set<string>();
  let includeAll = false;
  for (const f of arr) {
    if (f === '*') includeAll = true;
    const field = String(f);
    const dotIndex = field.indexOf('.');
    set.add(dotIndex === -1 ? field : field.slice(0, dotIndex));
  }
  return { includeAll, set };
}

export async function sanitizeFieldPermissionsResult(params: {
  value: any;
  tableName: string;
  user: any;
  action: 'read';
  fieldPermissionPolicyReader: TFieldPermissionPolicyReader;
  metadata: any;
  requested?: TRequestedReadShape;
}): Promise<any> {
  const { value, tableName, user, action, fieldPermissionPolicyReader, metadata, requested } = params;
  const walk = async (
    node: any,
    currentTable: string,
    currentRequested: TRequestedReadShape | undefined,
  ): Promise<any> => {
    if (node == null) return node;
    if (Array.isArray(node)) {
      return Promise.all(node.map((item) => walk(item, currentTable, currentRequested)));
    }
    if (typeof node !== 'object' || node instanceof Date || Buffer.isBuffer(node)) return node;
    const direct = metadata?.tables?.get?.(currentTable);
    if (!direct) return node;

    const out: any = { ...node };
    const includeAll = !currentRequested || currentRequested.includeAll === true;
    if (!includeAll) {
      for (const key of Object.keys(out)) {
        if (!currentRequested.columns?.has(key) && !currentRequested.relations?.has(key)) delete out[key];
      }
    }

    for (const col of direct.columns || []) {
      const colName = col?.name;
      if (!colName || !(colName in out) || col.isPrimary === true) continue;
      if (user?.isRootAdmin === true && col.isPublished !== false) continue;
      const decision = await decideFieldPermission(fieldPermissionPolicyReader, {
        user,
        tableName: currentTable,
        action,
        subjectType: 'column',
        subjectName: colName,
        record: node,
      }, { defaultAllowed: col.isPublished !== false });
      if (!decision.allowed) delete out[colName];
    }

    for (const rel of direct.relations || []) {
      const relName = rel?.propertyName;
      if (!relName || !(relName in out)) continue;
      if (!(user?.isRootAdmin === true && rel.isPublished !== false)) {
        const decision = await decideFieldPermission(fieldPermissionPolicyReader, {
          user,
          tableName: currentTable,
          action,
          subjectType: 'relation',
          subjectName: relName,
          record: node,
        }, { defaultAllowed: rel.isPublished !== false });
        if (!decision.allowed) {
          delete out[relName];
          continue;
        }
      }
      const targetTable = rel.targetTableName || rel.targetTable;
      if (targetTable) out[relName] = await walk(out[relName], targetTable, currentRequested?.deep?.[relName]);
    }
    return out;
  };
  return walk(value, tableName, requested);
}

export function buildRequestedShapeFromQuery(opts: {
  fields: any;
  deep: any;
  tableName?: string;
  metadata?: any;
}): TRequestedReadShape {
  const parsed = parseRequestedFields(opts.fields);
  const relations = new Set<string>();
  const deep: Record<string, TRequestedReadShape> = {};
  const table = opts.metadata?.tables?.get?.(opts.tableName);
  for (const relation of table?.relations ?? []) {
    const name = relation.propertyName;
    if (!parsed.includeAll && !parsed.set.has(name)) continue;
    relations.add(name);
    const target = opts.metadata.tables.get(relation.targetTableName ?? relation.targetTable);
    deep[name] = { columns: new Set((target?.columns ?? []).filter((column: any) => column.isPrimary === true).map((column: any) => column.name)) };
  }
  if (opts.deep && typeof opts.deep === 'object') {
    for (const [name, entry] of Object.entries(opts.deep)) {
      relations.add(name);
      const relation = table?.relations?.find((item: any) => item.propertyName === name);
      deep[name] = buildRequestedShapeFromQuery({ fields: (entry as any)?.fields, deep: (entry as any)?.deep, tableName: relation?.targetTableName ?? relation?.targetTable, metadata: opts.metadata });
    }
  }
  return { includeAll: parsed.includeAll, columns: parsed.set, relations, deep };
}
