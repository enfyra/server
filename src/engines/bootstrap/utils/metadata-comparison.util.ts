import type { SchemaMigrationDef } from '../../../shared/types/schema-migration.types';

export const TABLE_FIELDS = [
  'isSystem',
  'isSingleRecord',
  'uniques',
  'indexes',
  'alias',
  'description',
  'metadata',
  'validateBody',
];

export const COLUMN_FIELDS = [
  'name',
  'type',
  'isPrimary',
  'isGenerated',
  'isNullable',
  'isSystem',
  'isUpdatable',
  'isPublished',
  'isEncrypted',
  'defaultValue',
  'options',
  'description',
  'placeholder',
];

export const RELATION_FIELDS = [
  'propertyName',
  'type',
  'targetTable',
  'mappedBy',
  'inversePropertyName',
  'isNullable',
  'isSystem',
  'isUpdatable',
  'isPublished',
  'onDelete',
  'description',
  'foreignKeyColumn',
  'referencedColumn',
  'constraintName',
  'junctionTableName',
  'junctionSourceColumn',
  'junctionTargetColumn',
  'metadata',
];

export const RELATION_UPDATE_FIELDS = [
  'propertyName',
  'type',
  'isNullable',
  'isSystem',
  'isUpdatable',
  'isPublished',
  'onDelete',
  'description',
  'foreignKeyColumn',
  'referencedColumn',
  'constraintName',
  'junctionTableName',
  'junctionSourceColumn',
  'junctionTargetColumn',
  'metadata',
];

export const RELATION_EXPLICIT_TARGET_FIELDS = new Set([
  'foreignKeyColumn',
  'referencedColumn',
  'constraintName',
  'junctionTableName',
  'junctionSourceColumn',
  'junctionTargetColumn',
  'metadata',
]);

export const TABLE_DEFAULTS: Record<string, any> = {
  isSystem: false,
  isSingleRecord: false,
  uniques: [],
  indexes: [],
  alias: null,
  description: null,
  metadata: null,
  validateBody: true,
};

export const COLUMN_DEFAULTS: Record<string, any> = {
  isPrimary: false,
  isGenerated: false,
  isNullable: true,
  isSystem: false,
  isUpdatable: true,
  isPublished: true,
  isEncrypted: false,
  defaultValue: null,
  options: null,
  description: null,
  placeholder: null,
};

export const RELATION_DEFAULTS: Record<string, any> = {
  mappedBy: null,
  inversePropertyName: null,
  isNullable: true,
  isSystem: false,
  isUpdatable: true,
  isPublished: true,
  onDelete: 'SET NULL',
  description: null,
  metadata: null,
};

export function relationFieldsForTarget(target: Record<string, any>): string[] {
  return RELATION_FIELDS.filter(
    (field) => !RELATION_EXPLICIT_TARGET_FIELDS.has(field) || field in target,
  );
}

export function comparableValue(
  record: Record<string, any>,
  field: string,
  defaults: Record<string, any>,
): any {
  const value =
    (record[field] === undefined || record[field] === null) && field in defaults
      ? defaults[field]
      : record[field];
  if (typeof defaults[field] === 'boolean') {
    return value === true || value === 1 || value === '1';
  }
  if (
    typeof value === 'string' &&
    ['defaultValue', 'options', 'uniques', 'indexes', 'metadata'].includes(
      field,
    )
  ) {
    try {
      return JSON.parse(value);
    } catch {
      if (value.startsWith('{') && value.endsWith('}')) {
        const content = value.slice(1, -1);
        if (!content) return [];
        try {
          return JSON.parse(`[${content}]`);
        } catch {
          return content.split(',').map((entry) => entry.trim());
        }
      }
      return value;
    }
  }
  return value ?? null;
}

export function changedFields(
  current: Record<string, any>,
  target: Record<string, any>,
  fields: string[],
  defaults: Record<string, any>,
): string[] {
  return fields.filter(
    (field) =>
      JSON.stringify(comparableValue(current, field, defaults)) !==
      JSON.stringify(comparableValue(target, field, defaults)),
  );
}

export function inverseRelationType(type: string): string {
  if (type === 'many-to-one') return 'one-to-many';
  if (type === 'one-to-many') return 'many-to-one';
  return type;
}

export function buildExpectedRelations(
  snapshot: Record<string, any>,
): Map<string, Record<string, any>> {
  const expected = new Map<string, Record<string, any>>();
  const generatedInverseKeys = new Set<string>();
  const declaredRelations = new Map<string, Record<string, any>>();
  for (const [tableName, table] of Object.entries(snapshot)) {
    for (const relation of (table as any).relations ?? []) {
      declaredRelations.set(`${tableName}.${relation.propertyName}`, relation);
    }
  }

  for (const [tableName, table] of Object.entries(snapshot)) {
    for (const relation of (table as any).relations ?? []) {
      const currentKey = `${tableName}.${relation.propertyName}`;
      if (generatedInverseKeys.has(currentKey)) continue;

      const isOneToManyInverse =
        relation.type === 'one-to-many' && relation.inversePropertyName;
      expected.set(currentKey, {
        ...relation,
        mappedBy: isOneToManyInverse
          ? relation.inversePropertyName
          : relation.mappedBy,
      });
      if (!relation.inversePropertyName) continue;

      const inverseKey = `${relation.targetTable}.${relation.inversePropertyName}`;
      generatedInverseKeys.add(inverseKey);
      const declaredInverse = declaredRelations.get(inverseKey);
      expected.set(inverseKey, {
        isNullable: relation.isNullable !== false,
        isSystem: relation.isSystem ?? false,
        isUpdatable: relation.isUpdatable ?? true,
        isPublished: true,
        onDelete: 'SET NULL',
        description: null,
        ...declaredInverse,
        propertyName: relation.inversePropertyName,
        type: inverseRelationType(relation.type),
        targetTable: tableName,
        mappedBy: isOneToManyInverse ? null : relation.propertyName,
        inversePropertyName: relation.propertyName,
      });
    }
  }
  return expected;
}

export function duplicateValues(values: string[]): string[] {
  const seen = new Set<string>();
  const duplicates = new Set<string>();
  for (const value of values) {
    if (seen.has(value)) duplicates.add(value);
    seen.add(value);
  }
  return [...duplicates];
}

export function validateModificationTarget(
  label: string,
  changed: string[],
  target: Record<string, any>,
  modification: { to: Record<string, any> } | undefined,
  defaults: Record<string, any>,
  errors: string[],
): void {
  if (!modification) {
    errors.push(`${label} updates ${changed.join(', ')} without migration`);
    return;
  }

  const incomplete = changed.filter(
    (field) =>
      !(field in modification.to) ||
      JSON.stringify(comparableValue(modification.to, field, defaults)) !==
        JSON.stringify(comparableValue(target, field, defaults)),
  );
  if (incomplete.length > 0) {
    errors.push(
      `${label} migration does not fully declare target fields: ${incomplete.join(', ')}`,
    );
  }
}

export function validateModificationSource(
  label: string,
  current: Record<string, any>,
  target: Record<string, any>,
  modification: { from: Record<string, any> } | undefined,
  fields: string[],
  defaults: Record<string, any>,
  errors: string[],
): void {
  if (!modification) return;
  const mismatched = Object.keys(modification.from).filter((field) => {
    if (!fields.includes(field)) return false;
    const currentValue = JSON.stringify(
      comparableValue(current, field, defaults),
    );
    return (
      currentValue !==
        JSON.stringify(comparableValue(modification.from, field, defaults)) &&
      currentValue !== JSON.stringify(comparableValue(target, field, defaults))
    );
  });
  if (mismatched.length > 0) {
    errors.push(
      `${label} migration source does not match current fields: ${mismatched.join(', ')}`,
    );
  }
}
