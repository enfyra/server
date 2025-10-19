import { mapColumnTypeToOpenAPI } from './openapi-type-mapper';

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
  $ref?: string;
  [key: string]: any;
}

/**
 * Generate OpenAPI schemas from table definitions
 */
export function generateSchemasFromTables(tables: any[]): Record<string, SchemaObject> {
  const schemas: Record<string, SchemaObject> = {};

  for (const table of tables) {
    if (!table?.name) continue;

    const tableName = table.name;

    // Full schema (for responses)
    schemas[tableName] = generateTableSchema(table, false);

    // Input schema (for create - exclude id, timestamps)
    schemas[`${tableName}Input`] = generateTableSchema(table, true);

    // Update schema (for update - all fields optional except id)
    schemas[`${tableName}Update`] = generateTableSchema(table, true, true);
  }

  // Add common schemas
  schemas['PaginatedResponse'] = {
    type: 'object',
    properties: {
      data: {
        type: 'array',
        items: { type: 'object' }
      },
      meta: {
        type: 'object',
        properties: {
          totalCount: { type: 'integer' },
          filterCount: { type: 'integer' },
        }
      }
    }
  };

  return schemas;
}

/**
 * Generate schema for a single table
 */
function generateTableSchema(
  table: any,
  isInput: boolean = false,
  isUpdate: boolean = false
): SchemaObject {
  const properties: Record<string, SchemaObject> = {};
  const required: string[] = [];

  // Add columns
  for (const column of table.columns || []) {
    // Skip id, createdAt, updatedAt for input schemas
    if (isInput && (column.isPrimary || column.name === 'createdAt' || column.name === 'updatedAt')) {
      continue;
    }

    const fieldName = column.name;
    const schema = mapColumnTypeToOpenAPI(column.type);

    // Add enum values if available
    if (column.type === 'enum' && column.options) {
      schema.enum = column.options;
    }

    properties[fieldName] = schema;

    // Mark as required if not nullable (only for create input)
    if (!isUpdate && !column.isNullable && !column.isPrimary) {
      required.push(fieldName);
    }
  }

  // Add id field for update schema
  if (isUpdate) {
    properties.id = { type: 'string' };
    required.push('id');
  }

  return {
    type: 'object',
    properties,
    required: required.length > 0 ? required : undefined,
  };
}

/**
 * Generate response schema for a table with data wrapper
 */
export function generateResponseSchema(tableName: string): SchemaObject {
  return {
    type: 'object',
    properties: {
      data: {
        type: 'array',
        items: {
          $ref: `#/components/schemas/${tableName}`
        }
      },
      meta: {
        type: 'object',
        properties: {
          totalCount: { type: 'integer' },
          filterCount: { type: 'integer' },
        }
      }
    }
  };
}

/**
 * Generate single item response schema
 */
export function generateSingleItemSchema(tableName: string): SchemaObject {
  return {
    $ref: `#/components/schemas/${tableName}`
  };
}

/**
 * Generate error response schema
 */
export function generateErrorSchema(): SchemaObject {
  return {
    type: 'object',
    properties: {
      success: { type: 'boolean', example: false },
      message: { type: 'string' },
      statusCode: { type: 'integer' },
      error: {
        type: 'object',
        properties: {
          code: { type: 'string' },
          message: { type: 'string' },
          details: { type: 'object' },
          timestamp: { type: 'string', format: 'date-time' },
          path: { type: 'string' },
          method: { type: 'string' },
          correlationId: { type: 'string' },
        }
      }
    }
  };
}

