export function parseValue(fieldType: string, value: any): any {
  if (value === null || value === undefined) return value;

  const type = String(fieldType).toLowerCase();

  switch (type) {
    case 'int':
    case 'integer':
    case 'smallint':
    case 'bigint':
    case 'decimal':
    case 'numeric':
    case 'float':
    case 'double':
      return Number(value);

    case 'boolean':
      return value === true || value === 'true' || value === 1;

    case 'date':
    case 'datetime':
    case 'timestamp':
      // If it's already a Date object, return it
      if (value instanceof Date) return value;
      // Try to parse the date string
      const parsedDate = new Date(value);
      if (isNaN(parsedDate.getTime())) {
        throw new Error(`Invalid date value: ${value}`);
      }
      return parsedDate;

    case 'uuid':
    case 'varchar':
    case 'text':
    default:
      return String(value).trim();
  }
}
