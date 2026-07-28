export type MongoBsonType =
  | 'string'
  | 'int'
  | 'long'
  | 'double'
  | 'bool'
  | 'date'
  | 'objectId'
  | 'object'
  | 'array';

const BSON_TYPE_MAP: Record<string, MongoBsonType> = {
  string: 'string',
  text: 'string',
  varchar: 'string',
  char: 'string',
  uuid: 'string',
  objectId: 'objectId',
  ObjectId: 'objectId',
  objectid: 'objectId',
  richtext: 'string',
  code: 'string',
  int: 'int',
  integer: 'int',
  smallint: 'int',
  tinyint: 'int',
  bigint: 'long',
  float: 'double',
  double: 'double',
  decimal: 'double',
  numeric: 'double',
  real: 'double',
  boolean: 'bool',
  bool: 'bool',
  date: 'date',
  datetime: 'date',
  timestamp: 'date',
  json: 'object',
  'simple-json': 'object',
  array: 'array',
  enum: 'string',
};

export function sqlTypeToBsonType(type: string): MongoBsonType {
  return BSON_TYPE_MAP[type] || 'string';
}

export interface MongoValidationColumnLike {
  name?: string;
  type?: string;
  isNullable?: boolean;
  isGenerated?: boolean;
  defaultValue?: unknown;
  description?: string;
}

export function buildMongoValidationSchema(
  columns: MongoValidationColumnLike[],
): Record<string, unknown> {
  const properties: Record<string, unknown> = {};
  const required: string[] = [];
  for (const col of columns) {
    if (
      !col.name ||
      col.name === '_id' ||
      col.name === 'createdAt' ||
      col.name === 'updatedAt'
    ) {
      continue;
    }
    const bsonType = sqlTypeToBsonType(col.type || 'string');
    properties[col.name] = {
      bsonType: col.isNullable ? [bsonType, 'null'] : bsonType,
      description: col.description || col.name,
    };
    if (!col.isNullable && !col.defaultValue && !col.isGenerated) {
      required.push(col.name);
    }
  }
  const schema: Record<string, unknown> = {
    bsonType: 'object',
    properties,
  };
  if (required.length > 0) {
    schema.required = required;
  }
  return schema;
}

export const MONGO_VALIDATION_LEVEL = 'moderate' as const;
export const MONGO_VALIDATION_ACTION = 'error' as const;
