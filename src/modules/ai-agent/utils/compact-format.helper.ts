import { optimizeMetadataForLLM } from './metadata-optimizer.helper';
import { CompactFormat } from './types';

export function toCompactFormat<T extends Record<string, any>>(
  items: T[],
  fieldOrder?: string[],
): CompactFormat | null {
  if (!items || items.length === 0) {
    return null;
  }

  const allFields = new Set<string>();
  items.forEach((item) => {
    Object.keys(item).forEach((key) => allFields.add(key));
  });

  const fields = fieldOrder
    ? fieldOrder.filter((f) => allFields.has(f)).concat(Array.from(allFields).filter((f) => !fieldOrder.includes(f)))
    : Array.from(allFields);

  const data = items.map((item) => fields.map((field) => item[field] ?? null));

  return { fields, data };
}

export function formatMetadataCompact(metadata: any): any {
  const optimized = optimizeMetadataForLLM(metadata);
  
  const result: any = {
    name: optimized.name,
  };

  if (optimized.id !== undefined && optimized.id !== null) {
    result.id = optimized.id;
  }

  if (optimized.description !== undefined) {
    result.description = optimized.description;
  }

  if (optimized.isSystem !== undefined) {
    result.isSystem = optimized.isSystem;
  }

  if (optimized.isSingleRecord !== undefined) {
    result.isSingleRecord = optimized.isSingleRecord;
  }

  if (optimized.uniques && Array.isArray(optimized.uniques) && optimized.uniques.length > 0) {
    result.uniques = optimized.uniques;
  }

  if (optimized.indexes && Array.isArray(optimized.indexes) && optimized.indexes.length > 0) {
    result.indexes = optimized.indexes;
  }

  if (optimized.columns && Array.isArray(optimized.columns) && optimized.columns.length > 0) {
    const columnFields = ['name', 'type', 'isNullable', 'isPrimary', 'isGenerated', 'defaultValue', 'options', 'description'];
    const columnCompact = toCompactFormat(optimized.columns, columnFields);
    if (columnCompact) {
      result.columns = columnCompact;
    }
  }

  if (optimized.relations && Array.isArray(optimized.relations) && optimized.relations.length > 0) {
    const relationFields = ['propertyName', 'type', 'targetTableName', 'isNullable', 'inversePropertyName', 'foreignKeyColumn', 'description'];
    const relationCompact = toCompactFormat(optimized.relations, relationFields);
    if (relationCompact) {
      result.relations = relationCompact;
    }
  }

  return result;
}

