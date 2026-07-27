import type {
  ColumnModifyDef,
  RelationModifyDef,
  SchemaMigrationDef,
  TableModifyDef,
  TableRenameDef,
} from '../../../shared/types/schema-migration.types';
import { getScriptLegacyField } from '../../../shared/utils/script-code.util';
import {
  COLUMN_FIELDS,
  RELATION_UPDATE_FIELDS,
  TABLE_FIELDS,
} from './metadata-comparison.util';

export function hasSchemaMigrations(
  migrations: SchemaMigrationDef | null | undefined,
): migrations is SchemaMigrationDef {
  if (!migrations) return false;
  return (
    (migrations.coreTablesToRename?.length ?? 0) > 0 ||
    (migrations.tablesToRename?.length ?? 0) > 0 ||
    (migrations.physicalTablesToRename?.length ?? 0) > 0 ||
    (migrations.physicalTablesToDrop?.length ?? 0) > 0 ||
    (migrations.tables?.length ?? 0) > 0 ||
    (migrations.tablesToDrop?.length ?? 0) > 0
  );
}

export function getValidTableRenames(
  renames: TableRenameDef[],
): TableRenameDef[] {
  return renames.filter(
    (rename) => rename.from && rename.to && rename.from !== rename.to,
  );
}

export function hasMetadataChanges(mod: { from: any; to: any }): boolean {
  return Object.entries(mod.to).some(
    ([key, value]) => JSON.stringify(value) !== JSON.stringify(mod.from[key]),
  );
}

export const hasTableMetadataChanges = hasMetadataChanges as (
  mod: TableModifyDef,
) => boolean;

export const hasColumnMetadataChanges = hasMetadataChanges as (
  mod: ColumnModifyDef,
) => boolean;

export const hasRelationMetadataChanges = hasMetadataChanges as (
  mod: RelationModifyDef,
) => boolean;

function pickDefined(source: Record<string, any>, fields: string[]): any {
  return Object.fromEntries(
    fields
      .filter((field) => source[field] !== undefined)
      .map((field) => [field, source[field]]),
  );
}

export function buildTableMetadataUpdate(mod: TableModifyDef): any {
  return pickDefined(mod.to, TABLE_FIELDS);
}

export function buildColumnMetadataUpdate(mod: ColumnModifyDef): any {
  return pickDefined(mod.to, COLUMN_FIELDS);
}

export function buildRelationMetadataUpdate(mod: RelationModifyDef): any {
  return pickDefined(mod.to, RELATION_UPDATE_FIELDS);
}

export function getLegacyScriptTargetColumn(
  tableName: string,
  colName: string,
): string | null {
  return getScriptLegacyField(tableName) === colName ? 'sourceCode' : null;
}
