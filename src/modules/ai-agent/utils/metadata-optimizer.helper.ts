export function optimizeMetadataForLLM(tableMetadata: any): any {
  const optimized: any = {
    name: tableMetadata.name,
    description: tableMetadata.description,
    isSingleRecord: tableMetadata.isSingleRecord || false,
  };

  if (tableMetadata.id !== undefined && tableMetadata.id !== null) {
    optimized.id = tableMetadata.id;
  }

  if (tableMetadata.isSystem !== undefined && tableMetadata.isSystem !== null) {
    optimized.isSystem = tableMetadata.isSystem;
  }

  if (tableMetadata.uniques) {
    optimized.uniques = tableMetadata.uniques;
  }

  if (tableMetadata.indexes) {
    optimized.indexes = tableMetadata.indexes;
  }

  if (tableMetadata.columns && Array.isArray(tableMetadata.columns)) {
    const visibleColumns = tableMetadata.columns.filter((col: any) => !col.isHidden);
    optimized.columns = visibleColumns
      .map((col: any) => {
        const colData: any = {
          name: col.name,
          type: col.type,
          isNullable: col.isNullable,
          isPrimary: col.isPrimary || false,
          isGenerated: col.isGenerated || false,
        };

        if (col.description) {
          colData.description = col.description;
        }

        if (col.defaultValue !== undefined && col.defaultValue !== null) {
          colData.defaultValue = col.defaultValue;
        }

        if (col.options && Object.keys(col.options).length > 0) {
          colData.options = col.options;
        }

        return colData;
      });
    optimized.columnCount = visibleColumns.length;
  }

  if (tableMetadata.relations && Array.isArray(tableMetadata.relations)) {
    optimized.relations = tableMetadata.relations.map((rel: any) => {
      const relData: any = {
        propertyName: rel.propertyName,
        type: rel.type,
        targetTableName: rel.targetTableName,
        description: rel.description,
        isNullable: rel.isNullable,
        inversePropertyName: rel.inversePropertyName,
      };

      if (rel.foreignKeyColumn) {
        relData.foreignKeyColumn = rel.foreignKeyColumn;
      }

      return relData;
    });
  }

  return optimized;
}









