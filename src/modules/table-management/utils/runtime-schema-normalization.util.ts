import type {
  RuntimeSchemaColumnContract,
  RuntimeSchemaConstraintConflict,
  RuntimeSchemaRelationContract,
  RuntimeTableSchemaContract,
} from '../types/runtime-schema-mutation.types';

export interface KeyedRuntimeSchemaColumn extends RuntimeSchemaColumnContract {
  key: string;
}

export interface NormalizedRuntimeTableSchema {
  contract: RuntimeTableSchemaContract;
  keyedColumns: readonly KeyedRuntimeSchemaColumn[];
}

export function normalizeRuntimeTableSchema(
  metadata: unknown,
): NormalizedRuntimeTableSchema | null {
  if (!metadata || typeof metadata !== 'object') return null;
  const value = metadata as Record<string, any>;
  const keyedColumns = (Array.isArray(value.columns) ? value.columns : [])
    .map((column: any) => ({
      key: stringValue(column?.id ?? column?._id ?? column?.name),
      name: stringValue(column?.name),
      type: stringValue(column?.type),
      isNullable: column?.isNullable ?? true,
      isPrimary: !!column?.isPrimary,
      isGenerated: !!column?.isGenerated,
      defaultValue: normalizeJsonValue(column?.defaultValue ?? null),
    }))
    .sort((left, right) =>
      `${left.key}|${left.name}`.localeCompare(`${right.key}|${right.name}`),
    );
  const columns = keyedColumns
    .map(({ key: _key, ...column }) => column)
    .sort((left, right) => left.name.localeCompare(right.name));
  const relations = normalizeRelations(value.relations);
  return {
    contract: {
      name: stringValue(value.name),
      columns,
      relations,
      uniques: normalizeConstraintValue(value.uniques),
      indexes: normalizeConstraintValue(value.indexes),
    },
    keyedColumns,
  };
}

export function normalizeRuntimeSchemaRelations(
  relations: unknown,
): readonly RuntimeSchemaRelationContract[] {
  return normalizeRelations(relations);
}

export function runtimeRelationDiffKey(
  relation: RuntimeSchemaRelationContract,
): string {
  return `${relation.propertyName}|${relation.type}|${relation.targetTableName}|${relation.mappedBy}|${relation.foreignKeyColumn}|${relation.junctionTableName}`;
}

export function findRuntimeSchemaConstraintConflicts(
  metadata: unknown,
): RuntimeSchemaConstraintConflict[] {
  if (!metadata || typeof metadata !== 'object') return [];
  const value = metadata as Record<string, unknown>;
  const uniqueGroups = parseConstraintGroups(value.uniques);
  if (uniqueGroups.length === 0) return [];
  return parseConstraintGroups(value.indexes)
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
): RuntimeSchemaRelationContract[] {
  return (Array.isArray(relations) ? relations : [])
    .map((relation: any) => ({
      propertyName: stringValue(relation?.propertyName),
      type: stringValue(relation?.type),
      targetTableName: stringValue(
        relation?.targetTableName ??
          relation?.targetTable?.name ??
          relation?.targetTable,
      ),
      mappedBy: stringValue(relation?.mappedBy),
      foreignKeyColumn: stringValue(relation?.foreignKeyColumn),
      junctionTableName: stringValue(relation?.junctionTableName),
      isNullable: relation?.isNullable ?? true,
      onDelete: stringValue(relation?.onDelete) || 'SET NULL',
    }))
    .sort((left, right) =>
      runtimeRelationDiffKey(left).localeCompare(runtimeRelationDiffKey(right)),
    );
}

function normalizeConstraintValue(value: unknown): unknown {
  if (value == null) return null;
  const parsed = parseJson(value);
  if (!Array.isArray(parsed)) return normalizeJsonValue(parsed);
  return parsed
    .map((entry) => normalizeJsonValue(entry))
    .sort((left, right) =>
      JSON.stringify(left).localeCompare(JSON.stringify(right)),
    );
}

function parseConstraintGroups(value: unknown): string[][] {
  const parsed = parseJson(value);
  if (!Array.isArray(parsed)) return [];
  return parsed
    .map((group: any) => {
      const fields = Array.isArray(group) ? group : group?.value;
      if (!Array.isArray(fields)) return null;
      const normalized = fields
        .map((field: unknown) => stringValue(field).trim())
        .filter(Boolean);
      return normalized.length > 0 ? normalized : null;
    })
    .filter((group: string[] | null): group is string[] => group !== null);
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

function stringValue(value: unknown): string {
  return value == null ? '' : String(value);
}
