import { buildJoinTree } from './build-join-tree';

export interface JoinEdge {
  alias: string;
  parentAlias: string;
  propertyPath: string; // relation name on parent
}

export interface JoinPlan {
  m2o_o2o: JoinEdge[];
  o2m_m2m: JoinEdge[];
  selectArr: string[];
}

export function buildJoinPlan(args: {
  rootTable: string;
  meta: any;
  fields?: string[];
  filter?: any;
  sort?: string[];
  metadataGetter: (tableName: string) => any;
}): JoinPlan {
  const { rootTable, meta, fields, filter, sort, metadataGetter } = args;
  const { joinArr, selectArr } = buildJoinTree({
    meta,
    fields: fields?.join(',') || '*',
    filter,
    sort,
    rootAlias: rootTable,
    metadataGetter,
    log: [],
  });

  const m2o_o2o: JoinEdge[] = [];
  const o2m_m2m: JoinEdge[] = [];

  for (const edge of joinArr) {
    // Determine relation type from parent's metadata
    const parentMeta = edge.parentAlias === rootTable
      ? meta
      : resolveMetaByAlias(edge.parentAlias, rootTable, meta, metadataGetter);
    const rel = parentMeta?.relations?.find((r: any) => r.propertyName === edge.propertyPath);
    if (!rel) continue;
    if (rel.type === 'many-to-one' || rel.type === 'one-to-one') m2o_o2o.push(edge);
    else o2m_m2m.push(edge);
  }

  return { m2o_o2o, o2m_m2m, selectArr };
}

function resolveMetaByAlias(
  alias: string,
  rootAlias: string,
  rootMeta: any,
  metadataGetter: (t: string) => any,
): any {
  // Alias format: `${rootAlias}_rel1_rel2_...`
  const parts = alias.replace(`${rootAlias}_`, '').split('_');
  let currentMeta = rootMeta;
  for (const p of parts) {
    const rel = currentMeta?.relations?.find((r: any) => r.propertyName === p);
    if (!rel) return null;
    currentMeta = metadataGetter(rel.targetTableName || rel.type);
  }
  return currentMeta;
}


