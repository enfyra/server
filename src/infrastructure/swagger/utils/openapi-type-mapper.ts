/**
 * OpenAPI Schema Object type
 */
interface SchemaObject {
  type?: string;
  format?: string;
  properties?: Record<string, SchemaObject>;
  items?: SchemaObject;
  required?: string[];
  enum?: any[];
  [key: string]: any;
}

/**
 * Map TypeORM column types to OpenAPI schema types
 */
export function mapColumnTypeToOpenAPI(columnType: string): SchemaObject {
  const typeMap: Record<string, SchemaObject> = {
    // String types
    varchar: { type: 'string' },
    char: { type: 'string' },
    text: { type: 'string' },
    
    // Integer types
    int: { type: 'integer', format: 'int32' },
    integer: { type: 'integer', format: 'int32' },
    smallint: { type: 'integer', format: 'int32' },
    bigint: { type: 'integer', format: 'int64' },
    
    // Number types
    float: { type: 'number', format: 'float' },
    double: { type: 'number', format: 'double' },
    decimal: { type: 'number' },
    numeric: { type: 'number' },
    real: { type: 'number' },
    
    // Boolean
    boolean: { type: 'boolean' },
    bool: { type: 'boolean' },
    
    // Date/Time
    date: { type: 'string', format: 'date' },
    datetime: { type: 'string', format: 'date-time' },
    timestamp: { type: 'string', format: 'date-time' },
    time: { type: 'string', format: 'time' },
    
    // UUID
    uuid: { type: 'string', format: 'uuid' },
    
    // JSON
    json: { type: 'object' },
    'simple-json': { type: 'object' },
    jsonb: { type: 'object' },
    
    // Enum (will be overridden with actual enum values)
    enum: { type: 'string' },
  };

  return typeMap[columnType] || { type: 'string' };
}

/**
 * Check if a column type is numeric
 */
export function isNumericType(columnType: string): boolean {
  const numericTypes = [
    'int', 'integer', 'smallint', 'bigint',
    'float', 'double', 'decimal', 'numeric', 'real'
  ];
  return numericTypes.includes(columnType);
}

/**
 * Check if a column type is a date/time type
 */
export function isDateTimeType(columnType: string): boolean {
  const dateTimeTypes = ['date', 'datetime', 'timestamp', 'time'];
  return dateTimeTypes.includes(columnType);
}

