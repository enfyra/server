export function optimizeMetadataForLLM(tableMetadata: any): any {
  const optimized: any = {
    name: tableMetadata.name,
    description: tableMetadata.description,
    isSingleRecord: tableMetadata.isSingleRecord || false,
  };

  if (tableMetadata.id !== undefined && tableMetadata.id !== null) {
    optimized.id = tableMetadata.id;
  }

  if (tableMetadata.uniques) {
    optimized.uniques = tableMetadata.uniques;
  }

  if (tableMetadata.indexes) {
    optimized.indexes = tableMetadata.indexes;
  }

  if (tableMetadata.columns && Array.isArray(tableMetadata.columns)) {
    optimized.columns = tableMetadata.columns
      .filter((col: any) => !col.isHidden)
      .map((col: any) => {
        const colData: any = {
          name: col.name,
          type: col.type,
          description: col.description,
          isNullable: col.isNullable,
          isPrimary: col.isPrimary || false,
          isGenerated: col.isGenerated || false,
        };

        if (col.defaultValue !== undefined && col.defaultValue !== null) {
          colData.defaultValue = col.defaultValue;
        }

        if (col.options) {
          colData.options = col.options;
        }

        return colData;
      });
  }

  if (tableMetadata.relations && Array.isArray(tableMetadata.relations)) {
    optimized.relations = tableMetadata.relations.map((rel: any) => ({
      propertyName: rel.propertyName,
      type: rel.type,
      targetTableName: rel.targetTableName,
      description: rel.description,
      isNullable: rel.isNullable,
      inversePropertyName: rel.inversePropertyName,
    }));
  }

  return optimized;
}









