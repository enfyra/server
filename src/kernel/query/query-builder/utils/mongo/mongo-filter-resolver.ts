import { Db } from 'mongodb';
import {
  separateFilters,
  hasAnyRelations,
} from '../shared/filter-separator.util';
import { renderRawFilterToMongo } from './render-filter';
import { resolveMongoJunctionInfo } from '../../../../../engines/mongo';

export async function resolveMongoFilter(
  filter: any,
  tableName: string,
  metadata: any,
  db: Db,
): Promise<any> {
  if (!filter || typeof filter !== 'object') return {};

  const tableMeta = metadata?.tables?.get(tableName);
  if (!tableMeta) {
    return renderRawFilterToMongo(metadata, filter, tableName);
  }

  const relationNames = new Set<string>(
    (tableMeta.relations || []).map((r: any) => r.propertyName),
  );

  if (!hasAnyRelations(filter, relationNames)) {
    return renderRawFilterToMongo(metadata, filter, tableName);
  }

  if (filter._and && Array.isArray(filter._and)) {
    const resolved = await Promise.all(
      filter._and.map((c: any) =>
        resolveMongoFilter(c, tableName, metadata, db),
      ),
    );
    const nonEmpty = resolved.filter(
      (r: any) => r && Object.keys(r).length > 0,
    );
    if (nonEmpty.length === 0) return {};
    if (nonEmpty.length === 1) return nonEmpty[0];
    return { $and: nonEmpty };
  }

  if (filter._or && Array.isArray(filter._or)) {
    const resolved = await Promise.all(
      filter._or.map((c: any) =>
        resolveMongoFilter(c, tableName, metadata, db),
      ),
    );
    const nonEmpty = resolved.filter(
      (r: any) => r && Object.keys(r).length > 0,
    );
    if (nonEmpty.length === 0) return {};
    return { $or: nonEmpty };
  }

  if (filter._not) {
    const resolved = await resolveMongoFilter(
      filter._not,
      tableName,
      metadata,
      db,
    );
    if (!resolved || Object.keys(resolved).length === 0) return {};
    return { $nor: [resolved] };
  }

  const { fieldFilters, relationFilters } = separateFilters(filter, tableMeta);

  const parts: any[] = [];

  if (fieldFilters && Object.keys(fieldFilters).length > 0) {
    const rendered = renderRawFilterToMongo(metadata, fieldFilters, tableName);
    if (rendered && Object.keys(rendered).length > 0) {
      parts.push(rendered);
    }
  }

  if (relationFilters && Object.keys(relationFilters).length > 0) {
    for (const [relName, relFilter] of Object.entries(relationFilters)) {
      const relResult = await resolveSingleRelation(
        relName,
        relFilter,
        tableName,
        tableMeta,
        metadata,
        db,
      );
      if (relResult && Object.keys(relResult).length > 0) {
        parts.push(relResult);
      }
    }
  }

  if (parts.length === 0) return {};
  if (parts.length === 1) return parts[0];
  return { $and: parts };
}

async function resolveSingleRelation(
  relName: string,
  relFilter: any,
  tableName: string,
  tableMeta: any,
  metadata: any,
  db: Db,
): Promise<any> {
  const relation = (tableMeta.relations || []).find(
    (r: any) => r.propertyName === relName,
  );
  if (!relation) return {};

  const targetTable = relation.targetTableName || relation.targetTable;
  const targetMeta = metadata?.tables?.get(targetTable);
  if (!targetTable || !targetMeta) return {};

  if (relation.type === 'many-to-one' || relation.type === 'one-to-one') {
    return resolveM2oOrO2o(
      relFilter,
      relation,
      relName,
      targetTable,
      targetMeta,
      metadata,
      db,
    );
  }

  if (relation.type === 'one-to-many') {
    return resolveO2m(
      relFilter,
      relation,
      tableName,
      targetTable,
      targetMeta,
      metadata,
      db,
    );
  }

  if (relation.type === 'many-to-many') {
    return resolveM2m(
      relFilter,
      relation,
      tableName,
      targetTable,
      targetMeta,
      metadata,
      db,
    );
  }

  return {};
}

async function resolveM2oOrO2o(
  relFilter: any,
  relation: any,
  relName: string,
  targetTable: string,
  targetMeta: any,
  metadata: any,
  db: Db,
): Promise<any> {
  const fkField = relation.foreignKeyColumn || `${relName}Id`;
  const inner = unwrapIdLayer(relFilter);

  const nullOnly = checkNullOnly(inner);
  if (nullOnly !== null) {
    return nullOnly ? { [fkField]: null } : { [fkField]: { $ne: null } };
  }

  const directId = extractDirectIdMatch(inner);
  if (directId !== null) {
    if (directId.op === 'eq') return { [fkField]: directId.value };
    if (directId.op === 'neq') return { [fkField]: { $ne: directId.value } };
    if (directId.op === 'in') return { [fkField]: { $in: directId.value } };
    if (directId.op === 'not_in')
      return {
        $and: [
          { [fkField]: { $nin: directId.value } },
          { [fkField]: { $ne: null } },
        ],
      };
  }

  const targetQuery = renderRawFilterToMongo(metadata, inner, targetTable);
  if (!targetQuery || Object.keys(targetQuery).length === 0) return {};
  const matches = await db
    .collection(targetTable)
    .find(targetQuery, { projection: { _id: 1 } })
    .toArray();
  const ids = matches.map((m: any) => m._id).filter(Boolean);

  if (ids.length === 0) return { [fkField]: null };
  return { [fkField]: { $in: ids } };
}

async function resolveO2m(
  relFilter: any,
  relation: any,
  parentTable: string,
  targetTable: string,
  targetMeta: any,
  metadata: any,
  db: Db,
): Promise<any> {
  const fkField = relation.foreignKeyColumn || `${parentTable}Id`;
  const inner = unwrapIdLayer(relFilter);

  const nullOnly = checkNullOnly(inner);
  if (nullOnly !== null) {
    const parentIds = await collectParentIdsFromFk(
      db,
      targetTable,
      fkField,
      {},
    );
    if (nullOnly) {
      return parentIds.length === 0 ? {} : { _id: { $nin: parentIds } };
    }
    return parentIds.length === 0
      ? { __impossible__: true }
      : { _id: { $in: parentIds } };
  }

  const targetQuery = renderRawFilterToMongo(metadata, inner, targetTable);
  if (!targetQuery || Object.keys(targetQuery).length === 0) return {};
  const parentIds = await collectParentIdsFromFk(
    db,
    targetTable,
    fkField,
    targetQuery,
  );

  if (parentIds.length === 0) return { __impossible__: true };
  return { _id: { $in: parentIds } };
}

async function resolveM2m(
  relFilter: any,
  relation: any,
  parentTable: string,
  targetTable: string,
  targetMeta: any,
  metadata: any,
  db: Db,
): Promise<any> {
  const info = resolveMongoJunctionInfo(parentTable, relation);
  if (!info) return {};

  const inner = unwrapIdLayer(relFilter);
  const nullOnly = checkNullOnly(inner);

  if (nullOnly !== null) {
    const parentIds = await collectParentIdsFromJunction(
      db,
      info,
      targetTable,
      null,
    );
    if (nullOnly) {
      return parentIds.length === 0 ? {} : { _id: { $nin: parentIds } };
    }
    return parentIds.length === 0
      ? { __impossible__: true }
      : { _id: { $in: parentIds } };
  }

  const directId = extractDirectIdMatch(inner);
  if (directId !== null) {
    const idArr = Array.isArray(directId) ? directId : [directId];
    const parentIds = await collectParentIdsFromJunction(
      db,
      info,
      targetTable,
      { _id: { $in: idArr } },
    );
    if (parentIds.length === 0) return { __impossible__: true };
    return { _id: { $in: parentIds } };
  }

  const targetQuery = renderRawFilterToMongo(metadata, inner, targetTable);
  if (!targetQuery || Object.keys(targetQuery).length === 0) return {};

  const targetMatches = await db
    .collection(targetTable)
    .find(targetQuery, { projection: { _id: 1 } })
    .toArray();
  const targetIds = targetMatches.map((m: any) => m._id).filter(Boolean);

  if (targetIds.length === 0) return { __impossible__: true };
  const parentIds = await collectParentIdsFromJunction(db, info, targetTable, {
    _id: { $in: targetIds },
  });
  if (parentIds.length === 0) return { __impossible__: true };
  return { _id: { $in: parentIds } };
}

async function collectParentIdsFromFk(
  db: Db,
  targetCollection: string,
  fkField: string,
  targetQuery: any,
): Promise<any[]> {
  const matches = await db
    .collection(targetCollection)
    .find(targetQuery, { projection: { [fkField]: 1 } })
    .toArray();
  const seen = new Set<string>();
  const ids: any[] = [];
  for (const m of matches) {
    const fk = m[fkField];
    if (fk == null) continue;
    const key = String(fk);
    if (!seen.has(key)) {
      seen.add(key);
      ids.push(fk);
    }
  }
  return ids;
}

async function collectParentIdsFromJunction(
  db: Db,
  info: ReturnType<typeof resolveMongoJunctionInfo>,
  targetTable: string | null,
  targetFilter: any | null,
): Promise<any[]> {
  let targetIds: any[] | null = null;
  if (targetFilter && targetTable) {
    const matches = await db
      .collection(targetTable)
      .find(targetFilter, { projection: { _id: 1 } })
      .toArray();
    targetIds = matches.map((m: any) => m._id).filter(Boolean);
  }

  const junctionQuery = targetIds
    ? { [info!.otherColumn]: { $in: targetIds } }
    : {};

  const junctionRows = await db
    .collection(info!.junctionName)
    .find(junctionQuery, { projection: { [info!.selfColumn]: 1 } })
    .toArray();

  const seen = new Set<string>();
  const ids: any[] = [];
  for (const row of junctionRows) {
    const val = row[info!.selfColumn];
    if (val == null) continue;
    const key = String(val);
    if (!seen.has(key)) {
      seen.add(key);
      ids.push(val);
    }
  }
  return ids;
}

function unwrapIdLayer(relFilter: any): any {
  if (
    relFilter &&
    typeof relFilter === 'object' &&
    !Array.isArray(relFilter) &&
    Object.keys(relFilter).length === 1 &&
    relFilter.id &&
    typeof relFilter.id === 'object'
  ) {
    return relFilter.id;
  }
  return relFilter;
}

function checkNullOnly(filter: any): boolean | null {
  if (!filter || typeof filter !== 'object' || Array.isArray(filter))
    return null;
  const keys = Object.keys(filter);
  if (keys.length !== 1) return null;
  const k = keys[0];
  const v = filter[k];
  if (k === '_is_null') return v === true;
  if (k === '_is_not_null') return v !== true;
  if (k === '_eq') return v === null ? true : null;
  if (k === '_neq') return v === null ? false : null;
  return null;
}

function extractDirectIdMatch(filter: any): { op: string; value: any } | null {
  if (!filter || typeof filter !== 'object' || Array.isArray(filter))
    return null;
  const keys = Object.keys(filter);
  if (keys.length !== 1) return null;
  const k = keys[0];
  if (k === '_eq') return { op: 'eq', value: filter._eq };
  if (k === '_neq') return { op: 'neq', value: filter._neq };
  if (k === '_in' && Array.isArray(filter._in))
    return { op: 'in', value: filter._in };
  if (k === '_not_in' && Array.isArray(filter._not_in))
    return { op: 'not_in', value: filter._not_in };
  return null;
}
