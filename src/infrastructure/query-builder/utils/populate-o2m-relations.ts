import { extractRelationData } from './nest-relations';

/**
 * Populate one-to-many relations separately to avoid row multiplication
 */
export async function populateOneToManyRelations(
  rows: any[],
  o2mJoins: any[],
  selectArr: string[],
  rootTableName: string,
  aliasToMeta: Map<string, any>,
  metadataGetter: (tableName: string) => any,
  m2mAliases: Set<string>,
  knex: any,
): Promise<any[]> {
  if (rows.length === 0) return rows;

  // Group joins by level (root vs nested vs M2M children)
  const rootJoins = o2mJoins.filter(j => j.parentAlias === rootTableName);
  const m2mChildJoins = o2mJoins.filter(j => 
    j.parentAlias !== rootTableName && m2mAliases.has(j.parentAlias)
  );
  const nestedJoins = o2mJoins.filter(j => 
    j.parentAlias !== rootTableName && !m2mAliases.has(j.parentAlias)
  );

  // Populate root-level one-to-many and many-to-many relations
  for (const join of rootJoins) {
    if (join.relation.type === 'many-to-many') {
      await populateSingleM2M(rows, join, selectArr, aliasToMeta, metadataGetter, knex);
    } else {
      await populateSingleO2M(rows, join, selectArr, aliasToMeta, metadataGetter, knex);
    }
  }

  // Populate nested one-to-many relations (children of M2O)
  for (const join of nestedJoins) {
    await populateNestedO2M(rows, join, selectArr, rootTableName, aliasToMeta, metadataGetter, knex);
  }
  
  // Populate O2M children of M2M relations
  for (const join of m2mChildJoins) {
    await populateM2MChildO2M(rows, join, selectArr, rootTableName, aliasToMeta, metadataGetter, knex);
  }

  return rows;
}

/**
 * Populate a single root-level many-to-many relation
 */
async function populateSingleM2M(
  parentRows: any[],
  join: any,
  selectArr: string[],
  aliasToMeta: Map<string, any>,
  metadataGetter: (tableName: string) => any,
  knex: any,
): Promise<void> {
  const relationName = join.propertyPath;
  const relation = join.relation;
  const targetTable = relation.targetTableName;
  const junctionTable = relation.junctionTableName;
  const sourceColumn = relation.junctionSourceColumn;
  const targetColumn = relation.junctionTargetColumn;
  
  if (!junctionTable || !sourceColumn || !targetColumn) {
    console.warn(`[QueryEngine] M2M relation "${relationName}" missing junction table info`);
    return;
  }

  // Get parent IDs
  const parentIds = [...new Set(parentRows.map(r => r.id))].filter(id => id != null);
  if (parentIds.length === 0) return;

  // Determine which fields to select
  const relAlias = join.alias;
  const relFields = selectArr
    .filter(f => f.startsWith(`${relAlias}.`))
    .map(f => f.split('.')[1]);

  if (relFields.length === 0) {
    relFields.push('id');
  }

  // Query through junction table
  const childRows = await knex(targetTable)
    .join(junctionTable, `${targetTable}.id`, `${junctionTable}.${targetColumn}`)
    .whereIn(`${junctionTable}.${sourceColumn}`, parentIds)
    .select([
      ...relFields.map(f => `${targetTable}.${f}`),
      `${junctionTable}.${sourceColumn} as __parentId`
    ]);

  // Group by parent ID
  const childrenByParent = new Map<any, any[]>();
  for (const child of childRows) {
    const parentId = child.__parentId;
    delete child.__parentId;
    
    if (!childrenByParent.has(parentId)) {
      childrenByParent.set(parentId, []);
    }
    childrenByParent.get(parentId).push(child);
  }

  // Assign to parent rows
  for (const parent of parentRows) {
    parent[relationName] = childrenByParent.get(parent.id) || [];
  }
}

/**
 * Populate a single root-level one-to-many relation
 */
async function populateSingleO2M(
  parentRows: any[],
  join: any,
  selectArr: string[],
  aliasToMeta: Map<string, any>,
  metadataGetter: (tableName: string) => any,
  knex: any,
): Promise<void> {
  const relationName = join.propertyPath;
  const relation = join.relation;
  const targetTable = relation.targetTableName;
  const parentMeta = aliasToMeta.get(join.parentAlias);
  
  if (!parentMeta) return;

  // Get target metadata to find inverse FK
  const targetMeta = metadataGetter(targetTable);
  if (!targetMeta) return;

  // Find inverse relation (many-to-one from child back to parent)
  const parentTableName = parentMeta.name;
  const inverseRelation = targetMeta.relations?.find(
    (r: any) => 
      r.targetTableName === parentTableName && 
      r.inversePropertyName === relationName
  );
  
  const fkColumn = inverseRelation?.foreignKeyColumn;
  if (!fkColumn) return;

  // Get parent IDs
  const parentIds = [...new Set(parentRows.map(r => r.id))].filter(id => id != null);
  if (parentIds.length === 0) return;

  // Determine which fields to select for this relation
  const relAlias = join.alias;
  const relFields = selectArr
    .filter(f => f.startsWith(`${relAlias}.`))
    .map(f => f.split('.')[1]);

  if (relFields.length === 0) {
    relFields.push('id');
  }

  // Always include FK column for grouping
  if (!relFields.includes(fkColumn)) {
    relFields.push(fkColumn);
  }

  // Check for nested relations (e.g., columns.table)
  const nestedJoins: any[] = [];
  const nestedRelationFields = selectArr.filter(f => f.startsWith(`${relAlias}_`));
  const directNestedAliases = new Set<string>();
  
  for (const field of nestedRelationFields) {
    const parts = field.split('.');
    if (parts.length >= 1) {
      const fullAlias = parts[0];
      const remainder = fullAlias.replace(`${relAlias}_`, '');
      const firstSegment = remainder.split('_')[0];
      const directAlias = `${relAlias}_${firstSegment}`;
      directNestedAliases.add(directAlias);
    }
  }
  
  // Build nested joins for direct relations only
  for (const directAlias of directNestedAliases) {
    const directRelName = directAlias.replace(`${relAlias}_`, '');
    const directRel = targetMeta.relations?.find((r: any) => r.propertyName === directRelName);
    
    if (directRel && (directRel.type === 'many-to-one' || directRel.type === 'one-to-one')) {
      const hasDeeper = Array.from(nestedRelationFields).some(f => 
        f.startsWith(`${directAlias}_`)
      );
      
      nestedJoins.push({
        alias: directAlias,
        propertyName: directRelName,
        relation: directRel,
        hasDeeper,
      });
    }
  }

  // Build query with nested joins
  let query = knex(targetTable).whereIn(`${targetTable}.${fkColumn}`, parentIds);
  
  // Add nested joins and check for deeper nesting
  for (const nJoin of nestedJoins) {
    const nRel = nJoin.relation;
    const nTargetTable = nRel.targetTableName;
    const nFkColumn = nRel.foreignKeyColumn;
    
    query = query.leftJoin(
      `${nTargetTable} as ${nJoin.alias}`,
      `${targetTable}.${nFkColumn}`,
      `${nJoin.alias}.id`
    );
    
    // Add nested fields to select
    const nFields = selectArr
      .filter(f => f.startsWith(`${nJoin.alias}.`))
      .map(f => {
        const colName = f.split('.')[1];
        return `${nJoin.alias}.${colName} as ${nJoin.alias}_${colName}`;
      });
    
    if (nFields.length > 0) {
      query = query.select(nFields);
    } else {
      query = query.select(`${nJoin.alias}.id as ${nJoin.alias}_id`);
    }
    
    // Check if this nested relation also has wildcard
    const nTargetMeta = metadataGetter(nTargetTable);
    if (nTargetMeta) {
      const nScalarFieldsSelected = selectArr.filter(f => f.startsWith(`${nJoin.alias}.`)).length;
      const nTotalScalarFields = nTargetMeta.columns?.length || 0;
      const nIsWildcard = nScalarFieldsSelected >= nTotalScalarFields - 1;
      
      if (nIsWildcard) {
        // Auto-join all m2o/o2o relations of this nested relation
        for (const deepRel of nTargetMeta.relations || []) {
          if (deepRel.type === 'many-to-one' || deepRel.type === 'one-to-one') {
            const deepAlias = `${nJoin.alias}_${deepRel.propertyName}`;
            const deepTargetTable = deepRel.targetTableName;
            const deepFkColumn = deepRel.foreignKeyColumn;
            
            query = query.leftJoin(
              `${deepTargetTable} as ${deepAlias}`,
              `${nJoin.alias}.${deepFkColumn}`,
              `${deepAlias}.id`
            );
            
            query = query.select(`${deepAlias}.id as ${deepAlias}_id`);
          }
        }
      }
    }
  }
  
  // Add main table fields
  query = query.select(relFields.map(f => `${targetTable}.${f}`));

  // Load child records with nested data
  const childRecords = await query;

  // Pre-compute nested alias patterns for faster lookup
  const nestedAliasPrefixes = nestedJoins.map(nj => nj.alias + '_');
  
  // Group by parent ID and nest nested relations
  const childrenByParentId = new Map<any, any[]>();
  for (const child of childRecords) {
    const parentId = child[fkColumn];
    if (!childrenByParentId.has(parentId)) {
      childrenByParentId.set(parentId, []);
    }
    
    // Build child object
    const childObj: any = {};
    const nestedData: Map<string, any> = new Map();
    
    // Initialize nested data containers
    for (const nJoin of nestedJoins) {
      nestedData.set(nJoin.alias, {});
    }
    
    // Single-pass iteration: classify fields
    for (const key in child) {
      if (key === fkColumn) continue;
      
      let isNested = false;
      for (let i = 0; i < nestedJoins.length; i++) {
        const nJoin = nestedJoins[i];
        const prefix = nestedAliasPrefixes[i];
        
        if (key.startsWith(prefix)) {
          isNested = true;
          const remainder = key.substring(prefix.length);
          const underscoreIdx = remainder.indexOf('_');
          
          const nData = nestedData.get(nJoin.alias)!;
          
          if (underscoreIdx > 0) {
            // Deeper nesting
            const deepRelName = remainder.substring(0, underscoreIdx);
            const deepFieldName = remainder.substring(underscoreIdx + 1);
            if (!nData[deepRelName]) {
              nData[deepRelName] = {};
            }
            nData[deepRelName][deepFieldName] = child[key];
          } else {
            // Direct field
            nData[remainder] = child[key];
          }
          break;
        }
      }
      
      if (!isNested) {
        childObj[key] = child[key];
      }
    }
    
    // Attach nested data to child object
    for (const nJoin of nestedJoins) {
      const nData = nestedData.get(nJoin.alias)!;
      if (Object.keys(nData).length > 0) {
        childObj[nJoin.propertyName] = nData;
      }
    }
    
    childrenByParentId.get(parentId).push(childObj);
  }

  // Populate into parent rows
  for (const row of parentRows) {
    row[relationName] = childrenByParentId.get(row.id) || [];
  }
  
  // Recursive O2M population for nested objects
  for (const nJoin of nestedJoins) {
    if (nJoin.hasDeeper) {
      const nTargetTable = nJoin.relation.targetTableName;
      const nTargetMeta = metadataGetter(nTargetTable);
      if (!nTargetMeta) continue;
      
      const nestedObjects: any[] = [];
      for (const row of parentRows) {
        const children = row[relationName] || [];
        for (const child of children) {
          if (child[nJoin.propertyName]) {
            nestedObjects.push(child[nJoin.propertyName]);
          }
        }
      }
      
      if (nestedObjects.length === 0) continue;
      
      const nestedO2MJoins: any[] = [];
      for (const o2mRel of nTargetMeta.relations || []) {
        if (o2mRel.type === 'one-to-many') {
          const o2mAlias = `${nJoin.alias}_${o2mRel.propertyName}`;
          const hasO2MFields = selectArr.some(f => f.startsWith(`${o2mAlias}.`));
          if (hasO2MFields) {
            nestedO2MJoins.push({
              alias: o2mAlias,
              propertyPath: o2mRel.propertyName,
              relation: o2mRel,
              parentAlias: nJoin.alias,
            });
          }
        }
      }
      
      for (const nestedO2M of nestedO2MJoins) {
        await populateSingleO2M(
          nestedObjects,
          nestedO2M,
          selectArr,
          aliasToMeta,
          metadataGetter,
          knex,
        );
      }
    }
  }
}

/**
 * Populate O2M children of M2M relations
 */
async function populateM2MChildO2M(
  rootRows: any[],
  join: any,
  selectArr: string[],
  rootTableName: string,
  aliasToMeta: Map<string, any>,
  metadataGetter: (tableName: string) => any,
  knex: any,
): Promise<void> {
  // Navigate to M2M array
  const aliasPath = join.parentAlias.replace(`${rootTableName}_`, '').split('_');
  const parentObjects: any[] = [];
  
  for (const rootRow of rootRows) {
    let current: any = rootRow;
    for (const segment of aliasPath) {
      if (current && current[segment]) {
        current = current[segment];
      } else {
        current = null;
        break;
      }
    }
    if (Array.isArray(current)) {
      parentObjects.push(...current);
    } else if (current) {
      parentObjects.push(current);
    }
  }

  if (parentObjects.length === 0) return;
  await populateSingleO2M(parentObjects, join, selectArr, aliasToMeta, metadataGetter, knex);
}

/**
 * Populate nested one-to-many relations (e.g., mainTable.columns)
 */
async function populateNestedO2M(
  rootRows: any[],
  join: any,
  selectArr: string[],
  rootTableName: string,
  aliasToMeta: Map<string, any>,
  metadataGetter: (tableName: string) => any,
  knex: any,
): Promise<void> {
  // Parse parentAlias to determine nesting path
  const aliasPath = join.parentAlias
    .replace(`${rootTableName}_`, '')
    .split('_');

  // Navigate to parent objects
  const parentObjects: any[] = [];
  for (const rootRow of rootRows) {
    let current = rootRow;
    for (const segment of aliasPath) {
      if (current && current[segment]) {
        current = current[segment];
      } else {
        current = null;
        break;
      }
    }
    if (current) {
      parentObjects.push(current);
    }
  }

  if (parentObjects.length === 0) return;
  await populateSingleO2M(parentObjects, join, selectArr, aliasToMeta, metadataGetter, knex);
}


