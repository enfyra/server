import type {
  RuntimeSchemaColumnContract,
  RuntimeSchemaConstraintConflict,
  RuntimeSchemaNormalizationOptions,
  RuntimeSchemaRelationContract,
  RuntimeTableSchemaContract,
} from '../types/runtime-schema-mutation.types';
import { getSqlCanonicalConstraintGroups } from '../../../engines/knex/utils/sql-physical-schema-contract';
import { getMongoCanonicalConstraintGroups } from '../../../engines/mongo';
import { getForeignKeyColumnName } from '@enfyra/kernel';
import { getSqlJunctionPhysicalNames } from './sql-junction-naming.util';
import { normalizeJsonFieldValue } from '../../../shared/utils/json-field-normalizer.util';
import {
  getTableConstraintFields,
  hasSingleColumnUniqueConstraint,
  normalizeTableConstraints,
  parseTableConstraintGroups,
} from './table-constraints.util';

export interface KeyedRuntimeSchemaColumn extends RuntimeSchemaColumnContract {
  key: string;
}

export interface NormalizedRuntimeTableSchema {
  contract: RuntimeTableSchemaContract;
  keyedColumns: readonly KeyedRuntimeSchemaColumn[];
}

export function normalizeRuntimePolicyMetadata(
  metadata: unknown,
  options: { includeEmptySubjects?: boolean } = {},
): unknown | null {
  if (!metadata || typeof metadata !== 'object') return null;
  const includeEmptySubjects = options.includeEmptySubjects === true;
  const value = metadata as Record<string, any>;
  const normalizeEntries = (entries: unknown, kind: 'permission' | 'rule') => {
    if (!Array.isArray(entries)) return [];
    return entries
      .map((entry: any) => {
        if (kind === 'rule') {
          return normalizeJsonValue({
            ruleType: entry?.ruleType ?? null,
            value: normalizeJsonFieldValue(entry?.value ?? null),
            message: entry?.message ?? null,
            description: entry?.description ?? null,
            isEnabled: entry?.isEnabled !== false,
          });
        }
        const role = entry?.role;
        const allowedUsers = Array.isArray(entry?.allowedUsers)
          ? entry.allowedUsers
              .map((user: any) => stringValue(user?.id ?? user?._id ?? user))
              .filter(Boolean)
              .sort()
          : [];
        return normalizeJsonValue({
          action: entry?.action ?? null,
          effect: entry?.effect ?? entry?.decision ?? 'allow',
          condition: normalizeJsonFieldValue(entry?.condition ?? null),
          description: entry?.description ?? null,
          isEnabled: entry?.isEnabled !== false,
          role: stringValue(role?.id ?? role?._id ?? role),
          allowedUsers,
        });
      })
      .sort((left, right) => JSON.stringify(left).localeCompare(JSON.stringify(right)));
  };
  const normalizeSubjects = (items: unknown, identity: 'column' | 'relation') =>
    (Array.isArray(items) ? items : [])
      .filter((item: any) => item && typeof item === 'object')
      .map((item: any) => {
        const fieldPermissions = normalizeEntries(item?.fieldPermissions, 'permission');
        const rules = normalizeEntries(item?.rules, 'rule');
        return normalizeJsonValue({
          key: stringValue(item?.name ?? item?.propertyName ?? item?.id ?? item?._id),
          fieldPermissions,
          rules,
          identity,
        });
      })
      .filter(
        (item: any) =>
          includeEmptySubjects || item.fieldPermissions.length > 0 || item.rules.length > 0,
      )
      .sort((left, right) => JSON.stringify(left).localeCompare(JSON.stringify(right)));
  return {
    columns: normalizeSubjects(value.columns, 'column'),
    relations: normalizeSubjects(value.relations, 'relation'),
  };
}

export function assertRuntimeNestedMetadataIdsOwned(
  operation: 'create' | 'update' | 'delete',
  beforeMetadata: any,
  afterMetadata: any,
): void {
  const owned = new Map<string, string>();
  const collect = (metadata: any) => {
    for (const subjectType of ['columns', 'relations'] as const) {
      for (const subject of Array.isArray(metadata?.[subjectType]) ? metadata[subjectType] : []) {
        const owner = `${subjectType === 'columns' ? 'column' : 'relation'}:${String(subject?.id ?? subject?._id ?? subject?.name ?? subject?.propertyName)}`;
        for (const kind of ['fieldPermissions', 'rules'] as const) {
          for (const item of Array.isArray(subject?.[kind]) ? subject[kind] : []) {
            const id = item?.id ?? item?._id;
            if (id != null) owned.set(`${kind}:${String(id)}`, owner);
          }
        }
      }
    }
  };
  collect(beforeMetadata);
  if (operation === 'delete') return;
  const seen = new Set<string>();
  for (const subjectType of ['columns', 'relations'] as const) {
    for (const subject of Array.isArray(afterMetadata?.[subjectType]) ? afterMetadata[subjectType] : []) {
      const owner = `${subjectType === 'columns' ? 'column' : 'relation'}:${String(subject?.id ?? subject?._id ?? subject?.name ?? subject?.propertyName)}`;
      for (const kind of ['fieldPermissions', 'rules'] as const) {
        for (const item of Array.isArray(subject?.[kind]) ? subject[kind] : []) {
          const id = item?.id ?? item?._id;
          if (id == null) continue;
          const key = `${kind}:${String(id)}`;
          if (seen.has(key)) {
            throw new Error(`${kind} id ${String(id)} appears more than once in the table aggregate`);
          }
          seen.add(key);
          const previousOwner = owned.get(key);
          if (!previousOwner) {
            if (operation === 'update') throw new Error(`${kind} id ${String(id)} is not owned by this table aggregate`);
            throw new Error(`${kind} id ${String(id)} cannot be supplied while creating a table`);
          }
          if (previousOwner !== owner) {
            throw new Error(`${kind} id ${String(id)} is owned by ${previousOwner}, not ${owner}`);
          }
        }
      }
    }
  }
}

export function normalizeRuntimeTableSchema(
  metadata: unknown,
  options: RuntimeSchemaNormalizationOptions = {},
): NormalizedRuntimeTableSchema | null {
  if (!metadata || typeof metadata !== 'object') return null;
  const value = metadata as Record<string, any>;
  const tableName = stringValue(value.name);
  const effectiveConstraints = normalizeTableConstraints({
    uniques: value.uniques,
    indexes: value.indexes,
    columns: Array.isArray(value.columns) ? value.columns : [],
  });
  const effectiveUniques = effectiveConstraints.uniques;
  const keyedColumns = (Array.isArray(value.columns) ? value.columns : [])
    .map((column: any) => {
      const isMongoPrimary =
        options.backend === 'mongodb' &&
        column?.isPrimary === true &&
        (column?.name === 'id' || column?.name === '_id');
      return {
        key: stringValue(column?.id ?? column?._id ?? column?.name),
        name: isMongoPrimary ? '_id' : stringValue(column?.name),
        type: isMongoPrimary ? 'ObjectId' : stringValue(column?.type),
        isNullable: column?.isNullable ?? true,
        isPrimary: !!column?.isPrimary,
        isGenerated: !!column?.isGenerated,
        defaultValue: normalizeJsonValue(
          normalizeJsonFieldValue(column?.defaultValue ?? null),
        ),
        description: stringValue(column?.description),
        values: normalizeJsonValue(
          normalizeJsonFieldValue(column?.values ?? null),
        ),
        isUnique:
          !!column?.isUnique ||
          hasSingleColumnUniqueConstraint(
            effectiveUniques,
            stringValue(column?.name),
          ),
        isPublished: column?.isPublished ?? true,
        isUpdatable: column?.isUpdatable ?? true,
        isEncrypted: !!column?.isEncrypted,
        isIndex: !!column?.isIndex,
        options: normalizeJsonValue(
          normalizeJsonFieldValue(column?.options ?? null),
        ),
        metadata: normalizeJsonValue(
          normalizeJsonFieldValue(column?.metadata ?? null),
        ),
        placeholder: stringValue(column?.placeholder),
      };
    })
    .sort((left, right) =>
      `${left.key}|${left.name}`.localeCompare(`${right.key}|${right.name}`),
    );
  const columns = keyedColumns
    .map(({ key: _key, ...column }) => column)
    .sort((left, right) => left.name.localeCompare(right.name));
  const relations = normalizeRelations(value.relations, tableName, options);
  const canonicalConstraints =
    options.backend === 'postgresql' || options.backend === 'mysql'
      ? getSqlCanonicalConstraintGroups(tableName, {
          uniques: parseTableConstraintGroups(effectiveUniques).map(
            getTableConstraintFields,
          ),
          indexes: parseTableConstraintGroups(effectiveConstraints.indexes).map(
            getTableConstraintFields,
          ),
          columns: columns as any[],
          relations: relations as any[],
        })
      : options.backend === 'mongodb'
        ? getMongoCanonicalConstraintGroups({
            collectionName: tableName,
            uniques: parseTableConstraintGroups(effectiveUniques).map(
              getTableConstraintFields,
            ),
            indexes: parseTableConstraintGroups(
              effectiveConstraints.indexes,
            ).map(getTableConstraintFields),
            columns: columns as any[],
            relations: relations as any[],
          })
        : null;
  const indexes = canonicalConstraints
    ? normalizeConstraintValue(canonicalConstraints.indexes)
    : normalizeEffectiveIndexes(
        effectiveConstraints.indexes,
        effectiveUniques,
        options,
        tableName,
        columns,
        relations,
      );
  return {
    contract: {
      name: tableName,
      description: stringValue(value.description),
      alias: stringValue(value.alias),
      isSingleRecord: !!value.isSingleRecord,
      graphqlEnabled: value.graphqlEnabled ?? true,
      validateBody: value.validateBody ?? true,
      columns,
      relations,
      uniques: normalizeConstraintValue(
        canonicalConstraints?.uniques ?? effectiveUniques,
      ),
      indexes,
    },
    keyedColumns,
  };
}

function normalizeEffectiveIndexes(
  value: unknown,
  uniques: unknown,
  options: RuntimeSchemaNormalizationOptions,
  tableName: string,
  columns: readonly RuntimeSchemaColumnContract[],
  relations: readonly RuntimeSchemaRelationContract[],
): unknown {
  const normalized = normalizeConstraintValue(value);
  if (!options.backend) {
    return normalized;
  }
  const explicit = parseTableConstraintGroups(normalized).map(
    getTableConstraintFields,
  );
  if (options.backend !== 'mongodb') {
    return normalizeConstraintValue(
      getSqlCanonicalConstraintGroups(tableName, {
        indexes: explicit,
        uniques: parseTableConstraintGroups(uniques).map(
          getTableConstraintFields,
        ),
        columns: columns as any[],
        relations: relations as any[],
      }).indexes,
    );
  }
  return normalizeConstraintValue(
    getMongoCanonicalConstraintGroups({
      collectionName: tableName,
      indexes: explicit,
      uniques: parseTableConstraintGroups(uniques).map(
        getTableConstraintFields,
      ),
      columns: columns as any[],
      relations: relations as any[],
    }).indexes,
  );
}

export function runtimeRelationDiffKey(
  relation: RuntimeSchemaRelationContract,
): string {
  return `${relation.propertyName}|${relation.type}|${relation.targetTableName}|${relation.mappedBy}|${relation.foreignKeyColumn}|${relation.junctionTableName}|${relation.inversePropertyName}`;
}

export function findRuntimeSchemaConstraintConflicts(
  metadata: unknown,
): RuntimeSchemaConstraintConflict[] {
  if (!metadata || typeof metadata !== 'object') return [];
  const value = metadata as Record<string, unknown>;
  const uniqueGroups = parseTableConstraintGroups(value.uniques).map(
    getTableConstraintFields,
  );
  if (uniqueGroups.length === 0) return [];
  return parseTableConstraintGroups(value.indexes)
    .map(getTableConstraintFields)
    .filter(
      (index) =>
        !uniqueGroups.some(
          (unique) =>
            unique.length === index.length &&
            unique.every(
              (field, indexPosition) => field === index[indexPosition],
            ),
        ),
    )
    .map((index) => {
      const uniqueConstraints = uniqueGroups
        .map((fields) => ({
          fields,
          matchingFields: index.filter((field) => fields.includes(field)),
        }))
        .filter(({ matchingFields }) => matchingFields.length > 0);
      return {
        index,
        uniqueFields: [
          ...new Set(
            uniqueConstraints.flatMap(({ matchingFields }) => matchingFields),
          ),
        ],
        uniqueConstraints,
      };
    })
    .filter((conflict) => conflict.uniqueFields.length > 0);
}

function normalizeRelations(
  relations: unknown,
  tableName: string,
  options: RuntimeSchemaNormalizationOptions,
): RuntimeSchemaRelationContract[] {
  return (Array.isArray(relations) ? relations : [])
    .map((relation: any) => {
      const propertyName = stringValue(relation?.propertyName);
      const type = stringValue(relation?.type);
      const targetTableName = stringValue(
        relation?.targetTableName ??
          relation?.targetTable?.name ??
          relation?.targetTable,
      );
      const mappedBy = relationReferenceValue(
        relation?.mappedBy ??
          relation?.mappedById ??
          relation?.mappedByRelationId,
      );
      const ownsForeignKey =
        (type === 'many-to-one' || type === 'one-to-one') && !mappedBy;
      const ownsJunction = type === 'many-to-many' && !mappedBy;
      const generatedJunction =
        ownsJunction && tableName && propertyName && targetTableName
          ? getSqlJunctionPhysicalNames({
              sourceTable: tableName,
              propertyName,
              targetTable: targetTableName,
            }).junctionTableName
          : '';
      return {
        propertyName,
        type,
        targetTableName,
        mappedBy,
        foreignKeyColumn: ownsForeignKey
          ? stringValue(relation?.foreignKeyColumn) ||
            (options.backend === 'mongodb'
              ? propertyName
              : getForeignKeyColumnName(propertyName))
          : '',
        junctionTableName: ownsJunction
          ? stringValue(relation?.junctionTableName) || generatedJunction
          : '',
        isNullable: relation?.isNullable ?? true,
        onDelete: stringValue(relation?.onDelete) || 'SET NULL',
        inversePropertyName:
          options.mode === 'persisted'
            ? ''
            : stringValue(relation?.inversePropertyName),
        description: stringValue(relation?.description),
        isEager: !!relation?.isEager,
        isInverseEager: !!relation?.isInverseEager,
        isPublished: relation?.isPublished ?? true,
        isUpdatable: relation?.isUpdatable ?? true,
      };
    })
    .sort((left, right) =>
      runtimeRelationDiffKey(left).localeCompare(runtimeRelationDiffKey(right)),
    );
}

function normalizeConstraintValue(value: unknown): unknown {
  if (value == null) return [];
  const parsed = parseJson(value);
  if (!Array.isArray(parsed)) return normalizeJsonValue(parsed);
  return parsed
    .map((entry) => normalizeJsonValue(entry))
    .sort((left, right) =>
      JSON.stringify(left).localeCompare(JSON.stringify(right)),
    );
}

function parseJson(value: unknown): unknown {
  if (typeof value !== 'string') return value;
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
}

function normalizeJsonValue(value: unknown): unknown {
  if (value == null || typeof value !== 'object') return value;
  if (value instanceof Date) return value.toISOString();
  if (Array.isArray(value)) return value.map(normalizeJsonValue);
  if (typeof (value as { toHexString?: unknown }).toHexString === 'function') {
    return String(value);
  }
  return Object.fromEntries(
    Object.entries(value as Record<string, unknown>)
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([key, nested]) => [key, normalizeJsonValue(nested)]),
  );
}

function relationReferenceValue(value: unknown): string {
  if (value == null) return '';
  if (typeof value !== 'object') return String(value);
  const reference = value as Record<string, unknown>;
  return stringValue(
    reference.propertyName ?? reference.name ?? reference.id ?? reference._id,
  );
}

function stringValue(value: unknown): string {
  return value == null ? '' : String(value);
}
