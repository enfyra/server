import type {
  BootstrapSnapshot,
  SnapshotColumnDefinition,
} from '../../engines/bootstrap/types';
import type {
  ColumnModificationDef,
  ColumnModifyDef,
  SchemaMigrationDef,
} from '../types/schema-migration.types';
import {
  COLUMN_TYPE_CONTRACT,
  MONGO_COLUMN_TYPE_OPTIONS,
  MONGO_PRIMARY_KEY_NAME,
  MONGO_PRIMARY_KEY_TYPE,
  isSupportedMongoColumnType,
  isSupportedMySqlColumnType,
  isSupportedPostgresColumnType,
  toMongoTypeForSqlType,
  type ColumnTypeDeclaration,
  type MongoColumnTypeMigration,
} from '../types';

export {
  isSupportedMongoColumnType,
  isSupportedMySqlColumnType,
  isSupportedPostgresColumnType,
};

/**
 * Legacy metadata stores a SQL type and MongoDB metadata stores the Mongo type.
 * Every contract entry whose two labels differ is one upgrade mapping, so this
 * list cannot drift from the contract.
 */
export const MONGO_COLUMN_TYPE_MIGRATIONS = Object.values(COLUMN_TYPE_CONTRACT)
  .filter((entry) => entry.sqlType !== entry.mongoType)
  .map((entry) => ({
    from: entry.sqlType,
    to: entry.mongoType,
  })) as readonly MongoColumnTypeMigration[];

const MONGO_TYPE_BY_LEGACY_TYPE = new Map<string, string>(
  MONGO_COLUMN_TYPE_MIGRATIONS.map(({ from, to }) => [from, to]),
);

export function toMongoTargetColumnType(type: unknown): string {
  const normalized = String(type ?? '');
  return MONGO_TYPE_BY_LEGACY_TYPE.get(normalized) ?? normalized;
}

type DeclaredKind = 'sqlType' | 'mongoType';

/**
 * Flattens one declaration into the single `type` field the active backend reads.
 * `kind` names which declaration belongs to that backend, so the other one is
 * dropped rather than merged. A side that declares nothing for the active backend
 * carries no `type`, which is how a migration states "this field is not changed".
 */
function materializeDeclaration(
  column: Record<string, any>,
  kind: DeclaredKind,
  fallbackType?: string,
): Record<string, any> {
  const { sqlType, mongoType, ...rest } = column;
  const declaration: ColumnTypeDeclaration | undefined =
    kind === 'sqlType' ? sqlType : mongoType;
  const resolved = declaration ?? fallbackOrUndefined(fallbackType ?? rest.type);
  const isMongoPrimary =
    kind === 'mongoType' &&
    rest.isPrimary === true &&
    (rest.name === 'id' || rest.name === '_id');
  const resolvedType = isMongoPrimary
    ? MONGO_PRIMARY_KEY_TYPE
    : resolved?.type;

  return {
    ...rest,
    ...(isMongoPrimary ? { name: MONGO_PRIMARY_KEY_NAME } : {}),
    ...(resolvedType !== undefined ? { type: resolvedType } : {}),
    ...(resolved?.options !== undefined ? { options: resolved.options } : {}),
  };
}

function fallbackOrUndefined(
  type: unknown,
): ColumnTypeDeclaration | undefined {
  if (type === undefined || type === null || String(type) === '') {
    return undefined;
  }
  return { type: String(type) };
}

function materializeColumn(
  column: Record<string, any>,
  declaration: ColumnTypeDeclaration | undefined,
): Record<string, any> {
  return materializeDeclaration(column, 'mongoType', declaration?.type);
}

export function toSqlTargetColumn<
  T extends SnapshotColumnDefinition | Record<string, any>,
>(column: T): T {
  return materializeDeclaration(
    column as Record<string, any>,
    'sqlType',
  ) as unknown as T;
}

export function toMongoTargetColumn<
  T extends SnapshotColumnDefinition | Record<string, any>,
>(column: T): T {
  const entry = column as Record<string, any>;
  if (entry.mongoType) return materializeDeclaration(entry, 'mongoType') as T;
  if (
    entry.isPrimary === true &&
    (entry.name === 'id' || entry.name === '_id')
  ) {
    return materializeColumn(entry, { type: MONGO_PRIMARY_KEY_TYPE }) as T;
  }
  return materializeColumn(entry, {
    type: toMongoTargetColumnType(entry.sqlType?.type ?? entry.type),
  }) as T;
}

function toTargetColumnModification(
  modification: ColumnModifyDef,
  kind: DeclaredKind,
): ColumnModifyDef {
  return {
    from: materializeDeclaration(
      modification.from as Record<string, any>,
      kind,
    ) as ColumnModificationDef,
    to: materializeDeclaration(
      modification.to as Record<string, any>,
      kind,
    ) as ColumnModificationDef,
  };
}

export function toSqlTargetMigration(
  migration: SchemaMigrationDef | null,
): SchemaMigrationDef | null {
  if (!migration) return null;
  // `mongoColumnTypesToModify` is a MongoDB-only bulk rewrite; keeping it on the
  // SQL side would let coverage validation excuse a SQL type change that no
  // declaration actually covers.
  const { mongoColumnTypesToModify: _mongoOnly, ...rest } = migration;
  return {
    ...rest,
    tables: migration.tables.map((table) => ({
      ...table,
      ...(table.columnsToModify
        ? {
            columnsToModify: table.columnsToModify.map((modification) =>
              toTargetColumnModification(modification, 'sqlType'),
            ),
          }
        : {}),
    })),
  };
}

export function toMongoTargetMigration(
  migration: SchemaMigrationDef | null,
): SchemaMigrationDef | null {
  if (!migration) return null;
  return {
    ...migration,
    ...(migration.mongoColumnTypesToModify
      ? { mongoColumnTypesToModify: [...migration.mongoColumnTypesToModify] }
      : {}),
    tables: migration.tables.map((table) => ({
      ...table,
      ...(table.columnsToModify
        ? {
            columnsToModify: table.columnsToModify.map((modification) =>
              toTargetColumnModification(modification, 'mongoType'),
            ),
          }
        : {}),
    })),
  };
}

function toTargetRecord(
  record: Record<string, any>,
  kind: DeclaredKind,
): Record<string, any> {
  if (record.sqlType === undefined && record.mongoType === undefined) {
    return record;
  }
  const { _unique } = record;
  const projected = materializeDeclaration(record, kind);
  return { _unique, ...projected };
}

export function toSqlTargetDataMigration<T extends Record<string, any>>(
  dataMigration: T,
): T {
  return Object.fromEntries(
    Object.entries(dataMigration).map(([tableName, value]) => [
      tableName,
      Array.isArray(value)
        ? value.map((record) => toTargetRecord(record, 'sqlType'))
        : toTargetRecord(value, 'sqlType'),
    ]),
  ) as T;
}

export function toMongoTargetDataMigration<T extends Record<string, any>>(
  dataMigration: T,
): T {
  return Object.fromEntries(
    Object.entries(dataMigration).map(([tableName, value]) => [
      tableName,
      Array.isArray(value)
        ? value.map((record) => toTargetRecord(record, 'mongoType'))
        : toTargetRecord(value, 'mongoType'),
    ]),
  ) as T;
}

export function toSqlTargetSnapshot(
  snapshot: BootstrapSnapshot,
): BootstrapSnapshot {
  return Object.fromEntries(
    Object.entries(snapshot).map(([tableName, definition]) => [
      tableName,
      {
        ...definition,
        columns: (definition.columns ?? []).map((column: Record<string, any>) =>
          toSqlTargetColumn(column),
        ),
      },
    ]),
  );
}

export function toMongoTargetSnapshot(
  snapshot: BootstrapSnapshot,
): BootstrapSnapshot {
  return Object.fromEntries(
    Object.entries(snapshot).map(([tableName, definition]) => [
      tableName,
      {
        ...definition,
        columns: (definition.columns ?? []).map((column: Record<string, any>) =>
          toMongoTargetColumn(column),
        ),
      },
    ]),
  );
}

export { MONGO_COLUMN_TYPE_OPTIONS, toMongoTypeForSqlType };
