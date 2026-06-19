import * as fs from 'fs';
import * as path from 'path';
import {
  ColumnModifyDef,
  RelationModifyDef,
  SchemaMigrationDef,
  TableRenameDef,
} from '../../../shared/types/schema-migration.types';
import { getScriptLegacyField } from '../../../shared/utils/script-code.util';

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

export function loadSnapshotMigrationFile(
  cwd = process.cwd(),
): SchemaMigrationDef | null {
  const filePath = path.join(cwd, 'data/snapshot-migration.json');
  if (!fs.existsSync(filePath)) return null;

  const parsed = JSON.parse(fs.readFileSync(filePath, 'utf8'));
  return hasSchemaMigrations(parsed) ? parsed : null;
}

export function getValidTableRenames(
  renames: TableRenameDef[],
): TableRenameDef[] {
  return renames.filter(
    (rename) => rename.from && rename.to && rename.from !== rename.to,
  );
}

export function hasColumnMetadataChanges(mod: ColumnModifyDef): boolean {
  return (
    mod.to.name !== mod.from.name ||
    (mod.to.isNullable !== undefined &&
      mod.to.isNullable !== mod.from.isNullable) ||
    (mod.to.isUpdatable !== undefined &&
      mod.to.isUpdatable !== mod.from.isUpdatable) ||
    mod.to.description !== undefined
  );
}

export function buildColumnMetadataUpdate(mod: ColumnModifyDef): any {
  const updateData: any = {};

  if (mod.to.name !== mod.from.name) {
    updateData.name = mod.to.name;
  }
  if (
    mod.to.isNullable !== undefined &&
    mod.to.isNullable !== mod.from.isNullable
  ) {
    updateData.isNullable = mod.to.isNullable;
  }
  if (
    mod.to.isUpdatable !== undefined &&
    mod.to.isUpdatable !== mod.from.isUpdatable
  ) {
    updateData.isUpdatable = mod.to.isUpdatable;
  }
  if (mod.to.description !== undefined) {
    updateData.description = mod.to.description;
  }

  return updateData;
}

export function hasRelationMetadataChanges(mod: RelationModifyDef): boolean {
  return (
    mod.to.propertyName !== mod.from.propertyName ||
    (mod.to.mappedBy !== undefined && mod.to.mappedBy !== mod.from.mappedBy) ||
    (mod.to.isNullable !== undefined &&
      mod.to.isNullable !== mod.from.isNullable) ||
    (mod.to.isUpdatable !== undefined &&
      mod.to.isUpdatable !== mod.from.isUpdatable) ||
    mod.to.onDelete !== undefined
  );
}

export function getLegacyScriptTargetColumn(
  tableName: string,
  colName: string,
): string | null {
  return getScriptLegacyField(tableName) === colName ? 'sourceCode' : null;
}
