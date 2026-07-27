import snapshotMigration from '../../../data/snapshot-migration';
import type { SchemaMigrationDef } from '../../../shared/types/schema-migration.types';

export function loadRelationRenameMap(
  migration: SchemaMigrationDef = snapshotMigration,
): Record<string, Record<string, string>> {
  const out: Record<string, Record<string, string>> = {};
  const tables = migration.tables || [];
  for (const table of tables) {
    const tableName = table._unique?.name?._eq;
    if (!tableName || !Array.isArray(table.relationsToModify)) continue;
    for (const relation of table.relationsToModify) {
      if (
        !relation ||
        typeof relation !== 'object' ||
        !relation.from ||
        !relation.to
      ) {
        continue;
      }
      const fromName = relation.from.propertyName;
      const toName = relation.to.propertyName;
      if (fromName && toName && fromName !== toName) {
        if (!out[tableName]) out[tableName] = {};
        out[tableName][toName] = fromName;
      }
    }
  }
  return out;
}
