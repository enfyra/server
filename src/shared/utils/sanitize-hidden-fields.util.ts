export interface SanitizeMetadata {
  tables: Map<
    string,
    {
      columns: Array<{ name: string; isHidden?: boolean }>;
      relations?: Array<{ propertyName: string; isHidden?: boolean }>;
    }
  >;
}

export function sanitizeHiddenFieldsDeep(
  value: any,
  metadata: SanitizeMetadata,
): any {
  if (Array.isArray(value)) {
    return value.map((v) => sanitizeHiddenFieldsDeep(v, metadata));
  }

  if (value && typeof value === 'object' && !(value instanceof Date)) {
    const sanitized = sanitizeHiddenFieldsObject(value, metadata);

    for (const key of Object.keys(sanitized)) {
      const val = sanitized[key];
      if (val instanceof Date) {
        sanitized[key] = val.toISOString();
      } else if (
        val &&
        typeof val === 'object' &&
        val.constructor &&
        val.constructor.name === 'Date'
      ) {
        sanitized[key] = new Date(val).toISOString();
      } else {
        sanitized[key] = sanitizeHiddenFieldsDeep(val, metadata);
      }
    }

    return sanitized;
  }

  return value;
}

function sanitizeHiddenFieldsObject(obj: any, metadata: SanitizeMetadata): any {
  if (!obj || typeof obj !== 'object') return obj;

  const sanitized = { ...obj };

  if (!metadata) return sanitized;

  for (const [, tableMetadata] of metadata.tables.entries()) {
    const columns = tableMetadata.columns || [];
    const relations = tableMetadata.relations || [];

    const objectKeys = Object.keys(obj);
    const matchingColumns = columns.filter((col) =>
      objectKeys.includes(col.name),
    );

    if (matchingColumns.length > 0) {
      for (const column of columns) {
        if (column.isHidden === true && column.name in sanitized) {
          sanitized[column.name] = null;
        }
      }

      for (const rel of relations) {
        if (rel.isHidden === true && rel.propertyName in sanitized) {
          sanitized[rel.propertyName] = null;
        }
      }
    }
  }

  return sanitized;
}
