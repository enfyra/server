/**
 * Separate joins by type (O2M, M2M, regular)
 * O2M and M2M joins need special handling to avoid row multiplication
 */
export function separateJoinsByType(
  joinArr: any[],
  tableName: string,
  metadata: any,
  metadataGetter: (tableName: string) => any
) {
  const o2mJoins: any[] = [];
  const regularJoins: any[] = [];
  const o2mAliases = new Set<string>();
  const m2mAliases = new Set<string>();
  const aliasToMeta = new Map<string, any>();
  
  aliasToMeta.set(tableName, metadata);

  for (const join of joinArr) {
    const parentMeta = aliasToMeta.get(join.parentAlias);
    if (!parentMeta) {
      console.warn(`[QueryEngine] Cannot find metadata for parent alias: ${join.parentAlias}`);
      continue;
    }
    
    const relation = parentMeta.relations?.find((r: any) => r.propertyName === join.propertyPath);
    
    if (relation) {
      const targetMeta = metadataGetter(relation.targetTableName);
      if (targetMeta) {
        aliasToMeta.set(join.alias, targetMeta);
      }
      
      if (relation.type === 'one-to-many') {
        o2mJoins.push({ ...join, relation });
        o2mAliases.add(join.alias);
      } else if (relation.type === 'many-to-many') {
        regularJoins.push({ ...join, relation });
        m2mAliases.add(join.alias);
      } else {
        regularJoins.push({ ...join, relation });
      }
    }
  }

  // Filter out joins that are children of O2M or M2M
  const finalRegularJoins: any[] = [];
  for (const join of regularJoins) {
    if (o2mAliases.has(join.parentAlias) || m2mAliases.has(join.parentAlias)) {
      o2mJoins.push(join);
    } else {
      finalRegularJoins.push(join);
    }
  }

  return { regularJoins: finalRegularJoins, o2mJoins, aliasToMeta, o2mAliases, m2mAliases };
}


