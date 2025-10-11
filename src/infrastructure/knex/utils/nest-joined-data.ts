/**
 * Nest joined relation data into objects (like TypeORM)
 * Converts flat: { id: 1, mainTable_id: 2, mainTable_name: 'test' }
 * Into nested: { id: 1, mainTable: { id: 2, name: 'test' } }
 */
export function nestJoinedData(
  rows: any[],
  relations: string[],
  tableName: string,
): any[] {
  if (!rows || rows.length === 0) return rows;
  if (!relations || relations.length === 0) return rows;

  return rows.map(row => {
    const nested: any = {};
    const relationData: Map<string, any> = new Map();
    
    // Initialize relation objects
    for (const rel of relations) {
      const relName = rel.split('.')[0]; // Handle nested like 'mainTable.columns'
      relationData.set(relName, {});
    }
    
    // Classify fields
    for (const [key, value] of Object.entries(row)) {
      let isRelationField = false;
      
      // Check if field belongs to a relation
      for (const rel of relations) {
        const relName = rel.split('.')[0];
        const prefix = `${relName}_`;
        
        if (key.startsWith(prefix)) {
          isRelationField = true;
          const fieldName = key.substring(prefix.length);
          const relObj = relationData.get(relName)!;
          relObj[fieldName] = value;
          break;
        }
      }
      
      // Root table field
      if (!isRelationField) {
        nested[key] = value;
      }
    }
    
    // Attach nested relation objects (only if has data)
    for (const rel of relations) {
      const relName = rel.split('.')[0];
      const relObj = relationData.get(relName);
      
      if (relObj && Object.keys(relObj).length > 0) {
        // Check if relation is null (LEFT JOIN with no match)
        if (relObj.id === null || relObj.id === undefined) {
          nested[relName] = null;
        } else {
          nested[relName] = relObj;
        }
      }
    }
    
    return nested;
  });
}


