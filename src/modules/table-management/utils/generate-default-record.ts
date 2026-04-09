import { v4 as uuidv4 } from 'uuid';

/**
 * Generate a default value based on column type
 */
export function generateDefaultValue(column: {
  name: string;
  type: string;
  isNullable?: boolean;
  isPrimary?: boolean;
  isGenerated?: boolean;
  defaultValue?: any;
  options?: any;
}): any {
  const {
    name,
    type,
    isNullable,
    isPrimary,
    isGenerated,
    defaultValue,
    options,
  } = column;

  // Skip primary key columns (auto-generated)
  if (isPrimary && isGenerated) {
    return undefined;
  }

  // Skip id, createdAt, updatedAt (system columns)
  if (
    name === 'id' ||
    name === '_id' ||
    name === 'createdAt' ||
    name === 'updatedAt'
  ) {
    return undefined;
  }

  // If column has explicit defaultValue, use it
  if (defaultValue !== undefined && defaultValue !== null) {
    return defaultValue;
  }

  // If column is nullable, return null
  if (isNullable !== false) {
    return null;
  }

  // Generate default based on type for required fields
  switch (type) {
    case 'varchar':
    case 'text':
      return '';

    case 'int':
    case 'float':
      return 0;

    case 'boolean':
      return false;

    case 'date':
      return new Date().toISOString().split('T')[0];

    case 'uuid':
      return uuidv4();

    case 'enum':
      // Try to get first option from options
      if (
        options?.values &&
        Array.isArray(options.values) &&
        options.values.length > 0
      ) {
        return options.values[0];
      }
      return null;

    case 'simple-json':
      return {};

    default:
      return null;
  }
}

/**
 * Generate a default record for a table based on its columns
 */
export function generateDefaultRecord(columns: any[]): Record<string, any> {
  const record: Record<string, any> = {};

  for (const column of columns) {
    const value = generateDefaultValue(column);

    // Only include defined values (skip undefined which means "skip this column")
    if (value !== undefined) {
      record[column.name] = value;
    }
  }

  return record;
}
