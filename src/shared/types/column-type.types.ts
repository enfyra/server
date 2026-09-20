/**
 * How one column is declared to a backend: the type it stores plus, for a
 * picker column such as `enum`, the accepted values. Both backends use this
 * same shape, so a column is described identically for SQL and MongoDB.
 */
export interface ColumnTypeDeclaration {
  type: string;
  options?: readonly string[];
}

/**
 * One Enfyra column type, declared once for every backend it exists on.
 *
 * `sqlType` is what PostgreSQL/MySQL store in `enfyra_column.type` and the label
 * the SQL DDL switches on. `mongoType` is what MongoDB stores there. Enfyra
 * semantic types carry the same label on both backends; only backend-native
 * primitives differ.
 */
export interface ColumnTypeContractEntry {
  sqlType: string;
  mongoType: string;
}

export const COLUMN_TYPE_CONTRACT = {
  int: { sqlType: 'int', mongoType: 'int' },
  varchar: { sqlType: 'varchar', mongoType: 'string' },
  text: { sqlType: 'text', mongoType: 'string' },
  longtext: { sqlType: 'longtext', mongoType: 'string' },
  boolean: { sqlType: 'boolean', mongoType: 'bool' },
  uuid: { sqlType: 'uuid', mongoType: 'uuid' },
  ObjectId: { sqlType: 'ObjectId', mongoType: 'objectId' },
  bigint: { sqlType: 'bigint', mongoType: 'long' },
  date: { sqlType: 'date', mongoType: 'date' },
  datetime: { sqlType: 'datetime', mongoType: 'date' },
  timestamp: { sqlType: 'timestamp', mongoType: 'date' },
  enum: { sqlType: 'enum', mongoType: 'enum' },
  'simple-json': { sqlType: 'simple-json', mongoType: 'json' },
  code: { sqlType: 'code', mongoType: 'code' },
  'array-select': { sqlType: 'array-select', mongoType: 'array-select' },
  richtext: { sqlType: 'richtext', mongoType: 'richtext' },
  float: { sqlType: 'float', mongoType: 'double' },
} as const satisfies Record<string, ColumnTypeContractEntry>;

export type EnfyraColumnType = keyof typeof COLUMN_TYPE_CONTRACT;

/**
 * Mongo primitives an author may pick that no SQL backend has a counterpart for.
 * They are valid stored types on MongoDB and stay out of the SQL picker.
 */
export const MONGO_ONLY_COLUMN_TYPES = ['object', 'array'] as const;

/** SQL types MySQL accepts and PostgreSQL has no counterpart for. */
export const MYSQL_ONLY_COLUMN_TYPES = ['longtext'] as const;

/**
 * The permissive JSON contract. `simple-json` accepts any JSON value, and BSON
 * has no single native type covering that, so the Mongo projection stores this
 * label and its validator admits every BSON shape.
 */
export const PERMISSIVE_MONGO_COLUMN_TYPE = 'json';

/**
 * Column types that hold arbitrary JSON. `simple-json` is the semantic label and
 * `object`/`array` are the Mongo primitives it materializes to, so every consumer
 * that must treat JSON-like columns alike derives the set from here.
 */
export const JSON_LIKE_COLUMN_TYPES = [
  'simple-json',
  PERMISSIVE_MONGO_COLUMN_TYPE,
  ...MONGO_ONLY_COLUMN_TYPES,
] as const;

const JSON_LIKE_COLUMN_TYPE_SET = new Set<string>(JSON_LIKE_COLUMN_TYPES);

export function isJsonLikeColumnType(type: unknown): boolean {
  return JSON_LIKE_COLUMN_TYPE_SET.has(String(type ?? ''));
}

/** Values of `enfyra_column.type` on SQL backends, in persisted enum order. */
export const ENFYRA_COLUMN_TYPE_OPTIONS = Object.keys(
  COLUMN_TYPE_CONTRACT,
) as EnfyraColumnType[];

/** Values of `enfyra_column.type` on MongoDB. */
export const MONGO_COLUMN_TYPE_OPTIONS = [
  ...new Set([
    ...Object.values(COLUMN_TYPE_CONTRACT).map((entry) => entry.mongoType),
    ...MONGO_ONLY_COLUMN_TYPES,
  ]),
];

/**
 * MongoDB has no integer primary key: every document's identity is an ObjectId
 * stored in `_id`, so a primary column always materializes to these values.
 */
export const MONGO_PRIMARY_KEY_NAME = '_id';
export const MONGO_PRIMARY_KEY_TYPE = 'objectId';

export interface MongoColumnTypeMigration {
  from: string;
  to: string;
}

const MYSQL_ONLY_SET = new Set<string>(MYSQL_ONLY_COLUMN_TYPES);
const MONGO_COLUMN_TYPE_SET = new Set<string>(MONGO_COLUMN_TYPE_OPTIONS);
const SQL_COLUMN_TYPE_SET = new Set<string>(ENFYRA_COLUMN_TYPE_OPTIONS);
const POSTGRES_COLUMN_TYPE_SET = new Set<string>(
  ENFYRA_COLUMN_TYPE_OPTIONS.filter((type) => !MYSQL_ONLY_SET.has(type)),
);
const MONGO_TYPE_BY_SQL_TYPE = new Map<string, string>(
  Object.values(COLUMN_TYPE_CONTRACT).map((entry) => [
    entry.sqlType,
    entry.mongoType,
  ]),
);

export function isSupportedMongoColumnType(type: unknown): boolean {
  return MONGO_COLUMN_TYPE_SET.has(String(type ?? ''));
}

export function isSupportedPostgresColumnType(type: unknown): boolean {
  return POSTGRES_COLUMN_TYPE_SET.has(String(type ?? ''));
}

export function isSupportedMySqlColumnType(type: unknown): boolean {
  return SQL_COLUMN_TYPE_SET.has(String(type ?? ''));
}

export function toMongoTypeForSqlType(sqlType: unknown): string {
  const normalized = String(sqlType ?? '');
  return MONGO_TYPE_BY_SQL_TYPE.get(normalized) ?? normalized;
}
