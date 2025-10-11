import { extractRelationData, isRelationField } from './nest-relations';

/**
 * Find relation in metadata by property path
 */
export function findRelation(
  metadata: any,
  propertyPath: string,
  metadataGetter: (tableName: string) => any,
): any {
  const parts = propertyPath.split('.');
  let currentMeta = metadata;

  for (const part of parts) {
    const relation = currentMeta.relations?.find((r: any) => r.propertyName === part);
    if (!relation) return null;

    if (parts.indexOf(part) === parts.length - 1) {
      return relation;
    }

    currentMeta = metadataGetter(relation.targetTableName);
    if (!currentMeta) return null;
  }

  return null;
}

/**
 * Remap query results to nest relation data properly
 * - For many-to-many: group rows and nest into arrays, hide junction tables
 * - For many-to-one/one-to-one: nest into single object
 * - For one-to-many: keep as-is (handled by separate population)
 */
export function remapRelationsLegacy(
  rawRows: any[],
  joinArr: any[],
  metadata: any,
  metadataGetter: (tableName: string) => any,
): any[] {
  if (rawRows.length === 0) return rawRows;
  if (joinArr.length === 0) return rawRows;

  // Classify joins by relation type
  const m2mJoins: any[] = [];
  const o2mJoins: any[] = [];
  const singleJoins: any[] = [];

  for (const join of joinArr) {
    const relation = findRelation(metadata, join.propertyPath, metadataGetter);
    if (!relation) continue;

    if (relation.type === 'many-to-many') {
      m2mJoins.push({ ...join, relation });
    } else if (relation.type === 'one-to-many') {
      o2mJoins.push({ ...join, relation });
    } else if (relation.type === 'many-to-one' || relation.type === 'one-to-one') {
      singleJoins.push({ ...join, relation });
    }
  }
  
  // If only single relations (no grouping needed), just remap inline
  if (m2mJoins.length === 0 && o2mJoins.length === 0) {
    return rawRows.map((row) => {
      const mapped: any = {};

      for (const [key, value] of Object.entries(row)) {
        if (!isRelationField(key, joinArr)) {
          mapped[key] = value;
        }
      }

      // Build relation hierarchy
      const rootTableName = metadata.name;
      const aliasToObject = new Map<string, any>();
      aliasToObject.set(rootTableName, mapped);
      
      // Sort joins by depth
      const sortedJoins = [...singleJoins].sort((a, b) => {
        const depthA = (a.alias.match(/_/g) || []).length;
        const depthB = (b.alias.match(/_/g) || []).length;
        return depthA - depthB;
      });
      
      for (const join of sortedJoins) {
        const relationData = extractRelationData(row, join.alias);
        
        const parentObj = aliasToObject.get(join.parentAlias);
        if (parentObj) {
          parentObj[join.propertyPath] = relationData;
          if (relationData) {
            aliasToObject.set(join.alias, relationData);
          }
        }
      }

      return mapped;
    });
  }

  // If has many-to-many, need to group by parent ID
  const grouped = new Map<any, any>();

  for (const row of rawRows) {
    const parentId = row.id;

    if (!grouped.has(parentId)) {
      // First occurrence: create parent object
      const parent: any = {};

      // Copy root-level fields
      for (const [key, value] of Object.entries(row)) {
        if (!isRelationField(key, joinArr)) {
          parent[key] = value;
        }
      }

      // Initialize single relations (hierarchical)
      const rootTableName = metadata.name;
      const aliasToObject = new Map<string, any>();
      aliasToObject.set(rootTableName, parent);
      
      // Sort joins by depth
      const sortedJoins = [...singleJoins].sort((a, b) => {
        const depthA = (a.alias.match(/_/g) || []).length;
        const depthB = (b.alias.match(/_/g) || []).length;
        return depthA - depthB;
      });
      
      for (const join of sortedJoins) {
        const relationData = extractRelationData(row, join.alias);
        const parentObj = aliasToObject.get(join.parentAlias);
        if (parentObj) {
          parentObj[join.propertyPath] = relationData;
          if (relationData) {
            aliasToObject.set(join.alias, relationData);
          }
        }
      }

      // Initialize m2m relation arrays
      for (const join of m2mJoins) {
        const relationName = join.propertyPath;
        parent[relationName] = [];
      }

      grouped.set(parentId, parent);
    }

    // Extract and add m2m relation data
    const parent = grouped.get(parentId);

    for (const join of m2mJoins) {
      const relationName = join.propertyPath;
      const relationData = extractRelationData(row, join.alias);

      if (relationData && relationData.id) {
        // Deduplicate
        if (!parent[relationName].some((item: any) => item.id === relationData.id)) {
          parent[relationName].push(relationData);
        }
      }
    }
  }

  return Array.from(grouped.values());
}


