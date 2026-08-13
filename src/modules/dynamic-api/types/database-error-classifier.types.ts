export type DynamicDatabaseBackend =
  | 'postgres'
  | 'mysql'
  | 'mongodb'
  | 'unknown';

export type DynamicDatabaseDriver = 'pg' | 'mysql2' | 'mongodb' | 'unknown';

export type DynamicDatabaseErrorKind =
  | 'postgres_incompatible_operator'
  | 'unknown';

export interface DynamicDatabaseErrorClassification {
  backend: DynamicDatabaseBackend;
  driver: DynamicDatabaseDriver;
  code: string | null;
  kind: DynamicDatabaseErrorKind;
}
