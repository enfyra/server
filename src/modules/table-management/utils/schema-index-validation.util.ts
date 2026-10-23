import { ValidationException } from '../../../domain/exceptions';
import { findRuntimeSchemaConstraintConflicts } from './runtime-schema-normalization.util';
import { getTableConstraintFields, parseTableConstraintGroups, type TableConstraintGroup } from './table-constraints.util';

export function stripOwningRelationIndexes(
  target: unknown,
  requestedIndexes: unknown,
): unknown {
  if (!target || typeof target !== 'object') return requestedIndexes;
  const relations = (target as Record<string, unknown>).relations;
  if (!Array.isArray(relations)) return requestedIndexes;
  const owningFields = new Set<string>(
    relations
      .filter(relation => !relation.mappedBy && !relation.mappedById && ['many-to-one', 'one-to-one'].includes(relation.type))
      .flatMap(relation => [relation.propertyName, relation.foreignKeyColumn])
      .filter((field): field is string => typeof field === 'string' && field.length > 0),
  );
  const groups = parseTableConstraintGroups(requestedIndexes);
  const isOwningSingleton = (group: TableConstraintGroup) => {
    const fields = getTableConstraintFields(group);
    return fields.length === 1 && fields.some(field => owningFields.has(field));
  };
  if (!groups.some(isOwningSingleton)) return requestedIndexes;
  return groups.filter(group => !isOwningSingleton(group)).map(getTableConstraintFields);
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
