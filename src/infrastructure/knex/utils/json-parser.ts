export function parseRecordJsonFields(record: any, tableMetadata: any): any {
  if (!record || !tableMetadata?.columns) {
    return record;
  }
  const parsed = { ...record };
  for (const column of tableMetadata.columns) {
    const fieldName = column.name;
    const fieldType = column.type;
    if (
      fieldType === 'simple-json' &&
      parsed[fieldName] &&
      typeof parsed[fieldName] === 'string'
    ) {
      try {
        parsed[fieldName] = JSON.parse(parsed[fieldName]);
      } catch (e) {}
    }
  }
  return parsed;
}
export function parseRecordsJsonFields(
  records: any[],
  tableMetadata: any,
): any[] {
  if (!records || !Array.isArray(records)) {
    return records;
  }
  return records.map((record) => parseRecordJsonFields(record, tableMetadata));
}
export function stringifyRecordJsonFields(
  record: any,
  tableMetadata: any,
): any {
  if (!record || !tableMetadata?.columns) {
    return record;
  }
  const stringified = { ...record };
  for (const column of tableMetadata.columns) {
    const fieldName = column.name;
    const fieldType = column.type;
    if (
      fieldType === 'simple-json' &&
      stringified[fieldName] !== null &&
      stringified[fieldName] !== undefined &&
      typeof stringified[fieldName] !== 'string'
    ) {
      try {
        stringified[fieldName] = JSON.stringify(stringified[fieldName]);
      } catch (e) {}
    }
  }
  return stringified;
}
