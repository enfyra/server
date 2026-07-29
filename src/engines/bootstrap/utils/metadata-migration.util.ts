export {
  TABLE_FIELDS,
  COLUMN_FIELDS,
  RELATION_FIELDS,
  TABLE_DEFAULTS,
  COLUMN_DEFAULTS,
  RELATION_DEFAULTS,
  relationFieldsForTarget,
  comparableValue,
  changedFields,
  inverseRelationType,
  buildExpectedRelations,
  duplicateValues,
  validateModificationTarget,
  validateModificationSource,
} from './metadata-comparison.util';

export {
  hasSchemaMigrations,
  getValidTableRenames,
  hasMetadataChanges,
  hasTableMetadataChanges,
  hasColumnMetadataChanges,
  hasRelationMetadataChanges,
  buildTableMetadataUpdate,
  buildColumnMetadataUpdate,
  buildRelationMetadataUpdate,
  getLegacyScriptTargetColumn,
} from './metadata-diff.util';

export { validateSnapshotMigrationDefinition } from './metadata-migration-validation.util';

export { validateSnapshotMigrationCoverage } from './metadata-migration-coverage.util';

export { validateSnapshotTargetState } from './metadata-target-state.util';
