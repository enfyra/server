/**
 * Check if a field name is a relation field (based on joins)
 * Internal helper function
 */
function isRelationField(fieldName: string, joinArr: any[]): boolean {
  for (const join of joinArr) {
    if (join.alias && fieldName.startsWith(`${join.alias}_`)) {
      return true;
    }
  }
  return false;
}

/**
 * Extract relation data from a row based on alias prefix
 * Example: { test_ec_id: 1, test_ec_name: 'foo' } → { id: 1, name: 'foo' }
 */
export function extractRelationData(row: any, alias: string): any {
  const prefix = `${alias}_`;
  const data: any = {};

  for (const [key, value] of Object.entries(row)) {
    if (key.startsWith(prefix)) {
      const fieldName = key.substring(prefix.length);
      data[fieldName] = value;
    }
  }

  // If no data or id is null (LEFT JOIN with no match), return null
  if (Object.keys(data).length === 0 || data.id === null || data.id === undefined) {
    return null;
  }

  return data;
}

/**
 * Remap joined relation fields to nested objects
 * Converts flat row: { id: 1, user_id: 2, user_name: 'John' }
 * Into nested: { id: 1, user: { id: 2, name: 'John' } }
 */
export function remapRelations(
  rows: any[],
  joinArr: any[],  // Array of join objects with .alias and .propertyPath
  tableName: string,
  aliasToMeta: Map<string, any>,
  metadataGetter: (tableName: string) => any,
  m2mAliases: Set<string>
): any[] {
  const remappedRows: any[] = [];

  for (const row of rows) {
    const newRow: any = {};
    const processedAliases = new Set<string>();

    // Extract root fields (fields without relation prefix)
    for (const key in row) {
      if (!isRelationField(key, joinArr)) {
        newRow[key] = row[key];
      }
    }

    // Extract joined relation data
    for (const join of joinArr) {
      if (processedAliases.has(join.alias)) continue;

      const relationData = extractRelationData(row, join.alias);

      if (relationData !== undefined) {
        // Rebuild full propertyPath from alias
        // e.g., "menu_definition_sidebar_parent" -> "sidebar.parent"
        const aliasParts = join.alias.split('_');
        const rootAlias = tableName;
        
        // Find where the actual path starts (after root table name)
        let pathParts: string[] = [];
        let foundRoot = false;
        let i = 0;
        
        // Skip root table name parts
        while (i < aliasParts.length) {
          const testName = aliasParts.slice(0, i + 1).join('_');
          if (testName === rootAlias) {
            foundRoot = true;
            pathParts = aliasParts.slice(i + 1);
            break;
          }
          i++;
        }
        
        if (!foundRoot) {
          pathParts = [join.propertyPath];
        }
        
        const propertyPath = pathParts.join('.');
        
        // Simple case: direct relation (e.g., sidebar, mainTable)
        if (!propertyPath.includes('.')) {
          newRow[propertyPath] = relationData;
        } else {
          // Nested relation (e.g., mainTable.columns)
          const parts = propertyPath.split('.');
          let currentObj = newRow;
          
          for (let i = 0; i < parts.length - 1; i++) {
            if (!currentObj[parts[i]]) {
              currentObj[parts[i]] = {};
            }
            currentObj = currentObj[parts[i]];
          }
          
          const lastPart = parts[parts.length - 1];
          if (m2mAliases.has(join.alias)) {
            if (!currentObj[lastPart]) {
              currentObj[lastPart] = [];
            }
            if (relationData && relationData.id !== null) {
              currentObj[lastPart].push(relationData);
            }
          } else {
            currentObj[lastPart] = relationData;
          }
        }
      }

      processedAliases.add(join.alias);
    }

    // Remove FK columns from result
    const finalRow = removeFKColumns(newRow, tableName, metadataGetter);
    remappedRows.push(finalRow);
  }

  // Deduplicate M2M arrays
  return deduplicateM2MArrays(remappedRows);
}

/**
 * Remove FK columns from nested objects recursively
 */
function removeFKColumns(obj: any, tableName: string, metadataGetter: (tableName: string) => any): any {
  if (!obj || typeof obj !== 'object' || Array.isArray(obj) || obj instanceof Date || Buffer.isBuffer(obj)) {
    return obj;
  }

  const meta = metadataGetter(tableName);
  if (!meta) return obj;

  const cleaned: any = {};

  for (const [key, value] of Object.entries(obj)) {
    // Check if this key is a relation
    const relation = meta.relations?.find((r: any) => r.propertyName === key);
    
    if (relation) {
      // It's a relation - process nested
      if (Array.isArray(value)) {
        cleaned[key] = value.map((item: any) => 
          removeFKColumns(item, relation.targetTableName, metadataGetter)
        );
      } else if (value && typeof value === 'object' && !(value instanceof Date) && !Buffer.isBuffer(value)) {
        cleaned[key] = removeFKColumns(value, relation.targetTableName, metadataGetter);
      } else {
        cleaned[key] = value;
      }
    } else {
      // Check if it's a FK column (support both camelCase and snake_case)
      const isFKColumn = meta.relations?.some((r: any) => 
        ['many-to-one', 'one-to-one'].includes(r.type) && 
        (r.foreignKeyColumn === key || 
         `${r.propertyName}Id` === key ||
         `${r.propertyName}_id` === key)
      );

      if (!isFKColumn) {
        cleaned[key] = value;
      }
      // Skip FK columns
    }
  }

  return cleaned;
}

/**
 * Deduplicate M2M arrays based on ID
 */
function deduplicateM2MArrays(rows: any[]): any[] {
  return rows.map(row => deduplicateObjectM2M(row));
}

function deduplicateObjectM2M(obj: any): any {
  if (!obj || typeof obj !== 'object' || obj instanceof Date || Buffer.isBuffer(obj)) return obj;
  
  if (Array.isArray(obj)) {
    return obj.map(item => deduplicateObjectM2M(item));
  }

  const result: any = {};
  
  for (const [key, value] of Object.entries(obj)) {
    if (Array.isArray(value)) {
      // Deduplicate by ID
      const seen = new Set();
      result[key] = value
        .filter((item: any) => {
          if (!item || typeof item !== 'object' || !item.id) return true;
          if (seen.has(item.id)) return false;
          seen.add(item.id);
          return true;
        })
        .map((item: any) => deduplicateObjectM2M(item));
    } else if (value && typeof value === 'object' && !(value instanceof Date) && !Buffer.isBuffer(value)) {
      result[key] = deduplicateObjectM2M(value);
    } else {
      result[key] = value;
    }
  }

  return result;
}

