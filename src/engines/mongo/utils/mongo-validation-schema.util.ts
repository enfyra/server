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
    // An omitted isNullable means nullable, matching runtime metadata
    // normalization and the SQL column contract. Treating it as non-nullable
    // here would build a validator the target contract can never match.
    const isNullable = col.isNullable !== false;
    properties[col.name] = {
      bsonType: isNullable ? [bsonType, 'null'] : bsonType,
      description: col.description || col.name,
    };
    // `false`, `0`, and `''` are declared defaults, so absence must be tested
    // explicitly instead of relying on truthiness.
    const hasDeclaredDefault =
      col.defaultValue !== undefined && col.defaultValue !== null;
    if (!isNullable && !hasDeclaredDefault && !col.isGenerated) {
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
