export interface SanitizeMetadata {
  tables: Map<
    string,
    {
      columns: Array<{ name: string; isHidden?: boolean }>;
      relations?: Array<{
        propertyName: string;
        isHidden?: boolean;
        targetTable?: string;
      }>;
    }
  >;
}

export function sanitizeHiddenFieldsDeep(
  value: any,
  metadata: SanitizeMetadata,
  tableName?: string,
): any {
  const strategy: 'context' | 'global' = tableName ? 'context' : 'global';
  return sanitizeHiddenFieldsDeepInternal(value, metadata, tableName, strategy);
}

function sanitizeHiddenFieldsDeepInternal(
  value: any,
  metadata: SanitizeMetadata,
  tableName: string | undefined,
  strategy: 'context' | 'global' | 'none',
): any {
  if (Array.isArray(value)) {
    return value.map((v) =>
      sanitizeHiddenFieldsDeepInternal(v, metadata, tableName, strategy),
    );
  }

  if (!value || typeof value !== 'object') {
    return value;
  }

  if (value instanceof Date) {
    return value.toISOString();
  }

  if (value.constructor && value.constructor.name === 'Date') {
    return new Date(value).toISOString();
  }

  const sanitized = { ...value };

  if (metadata) {
    if (strategy === 'context') {
      sanitizeHiddenFieldsObjectByTableInPlace(sanitized, metadata, tableName);
    } else if (strategy === 'global') {
      sanitizeHiddenFieldsObjectGlobalInPlace(sanitized, metadata);
    }
  }

  const currentTableMeta =
    strategy === 'context' && tableName ? metadata?.tables?.get(tableName) : null;
  const relations = currentTableMeta?.relations || [];

  for (const key of Object.keys(sanitized)) {
    const val = sanitized[key];
    if (!val || typeof val !== 'object') continue;

    if (val instanceof Date) {
      sanitized[key] = val.toISOString();
      continue;
    }

    if (val.constructor && val.constructor.name === 'Date') {
      sanitized[key] = new Date(val).toISOString();
      continue;
    }

    if (strategy === 'context') {
      const rel = relations.find((r) => r.propertyName === key) || null;
      if (rel && rel.isHidden !== true && rel.targetTable) {
        sanitized[key] = sanitizeHiddenFieldsDeepInternal(
          val,
          metadata,
          rel.targetTable,
          'context',
        );
      } else {
        sanitized[key] = sanitizeHiddenFieldsDeepInternal(val, metadata, undefined, 'none');
      }
    } else {
      sanitized[key] = sanitizeHiddenFieldsDeepInternal(val, metadata, undefined, strategy);
    }
  }

  return sanitized;
}

function sanitizeHiddenFieldsObjectByTableInPlace(
  obj: any,
  metadata: SanitizeMetadata,
  tableName: string | undefined,
): void {
  if (!tableName) return;
  const tableMetadata = metadata.tables.get(tableName);
  if (!tableMetadata) return;

  const columns = tableMetadata.columns || [];
  for (const column of columns) {
    if (column.isHidden === true && column.name in obj) {
      obj[column.name] = null;
    }
  }

  const relations = tableMetadata.relations || [];
  for (const rel of relations) {
    if (rel.isHidden === true && rel.propertyName in obj) {
      obj[rel.propertyName] = null;
    }
  }
}

function sanitizeHiddenFieldsObjectGlobalInPlace(
  obj: any,
  metadata: SanitizeMetadata,
): void {
  const objectKeys = Object.keys(obj);
  const keySet = new Set(objectKeys);

  for (const [, tableMetadata] of metadata.tables.entries()) {
    const columns = tableMetadata.columns || [];
    let matched = false;
    for (const col of columns) {
      if (keySet.has(col.name)) {
        matched = true;
        break;
      }
    }
    if (!matched) continue;

    for (const column of columns) {
      if (column.isHidden === true && column.name in obj) {
        obj[column.name] = null;
      }
    }

    const relations = tableMetadata.relations || [];
    for (const rel of relations) {
      if (rel.isHidden === true && rel.propertyName in obj) {
        obj[rel.propertyName] = null;
      }
    }
  }
}
