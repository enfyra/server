import { ValidationException } from '../../../domain/exceptions';
import { findRuntimeSchemaConstraintConflicts } from './runtime-schema-normalization.util';
import { getTableConstraintFields, parseTableConstraintGroups } from './table-constraints.util';

export function assertNoManualRelationIndexes(
  target: unknown,
  requestedIndexes: unknown,
  existingIndexes: unknown = [],
): void {
  if (!target || typeof target !== 'object') return;
  const relations = (target as Record<string, unknown>).relations;
  if (!Array.isArray(relations)) return;
  const owningFields = new Set(relations.filter(relation =>
    !relation.mappedBy && !relation.mappedById && ['many-to-one', 'one-to-one'].includes(relation.type),
  ).flatMap(relation => [relation.propertyName, relation.foreignKeyColumn].filter(Boolean)));
  const existingKeys = new Set(parseTableConstraintGroups(existingIndexes).map(group => JSON.stringify(getTableConstraintFields(group))));
  const indexes = parseTableConstraintGroups(requestedIndexes).map(getTableConstraintFields)
    .filter(group => !existingKeys.has(JSON.stringify(group)))
    .filter(group => group.length === 1 && owningFields.has(group[0]));
  if (indexes.length > 0) {
    throw new ValidationException('Owning relations already have automatic lookup indexes. Remove the manually declared relation index.', {
      code: 'SCHEMA_INDEX_AUTO_MANAGED', indexes,
    });
  }
}

export function assertNoNewRedundantIndexes(target: unknown, source?: unknown): void {
  const existing = new Set(findRuntimeSchemaConstraintConflicts(source).flatMap(conflict =>
    conflict.uniqueConstraints.map(unique => JSON.stringify([conflict.index, unique.fields])),
  ));
  const conflicts = findRuntimeSchemaConstraintConflicts(target).filter(conflict =>
    conflict.uniqueConstraints.some(unique => !existing.has(JSON.stringify([conflict.index, unique.fields]))),
  );
  if (conflicts.length > 0) {
    throw new ValidationException('An index is already covered by the leading fields of a unique constraint. Remove the redundant index.', {
      code: 'SCHEMA_INDEX_OVER_UNIQUE_FIELD',
      conflicts,
    });
  }
}
