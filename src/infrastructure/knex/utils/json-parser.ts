/**
 * Auto-parse JSON fields based on column metadata
 */

/**
 * Parse a single record based on table metadata
 */
export function parseRecordJsonFields(record: any, tableMetadata: any): any {
  if (!record || !tableMetadata?.columns) {
    return record;
  }

  const parsed = { ...record };

  for (const column of tableMetadata.columns) {
    const fieldName = column.name;
    const fieldType = column.type;

    // Auto-parse simple-json fields
    if (
      fieldType === 'simple-json' &&
      parsed[fieldName] &&
      typeof parsed[fieldName] === 'string'
    ) {
      try {
        parsed[fieldName] = JSON.parse(parsed[fieldName]);
      } catch (e) {
        // Keep as string if parse fails
      }
    }
  }

  return parsed;
}

/**
 * Parse multiple records based on table metadata
 */
export function parseRecordsJsonFields(records: any[], tableMetadata: any): any[] {
  if (!records || !Array.isArray(records)) {
    return records;
  }

  return records.map(record => parseRecordJsonFields(record, tableMetadata));
}

/**
 * Stringify fields for insert/update based on table metadata
 */
export function stringifyRecordJsonFields(record: any, tableMetadata: any): any {
  if (!record || !tableMetadata?.columns) {
    return record;
  }

  const stringified = { ...record };

  for (const column of tableMetadata.columns) {
    const fieldName = column.name;
    const fieldType = column.type;

    // Auto-stringify simple-json fields
    if (
      fieldType === 'simple-json' &&
      stringified[fieldName] !== null &&
      stringified[fieldName] !== undefined &&
      typeof stringified[fieldName] !== 'string'
    ) {
      try {
        stringified[fieldName] = JSON.stringify(stringified[fieldName]);
      } catch (e) {
        // Keep as-is if stringify fails
      }
    }
  }

  return stringified;
}









