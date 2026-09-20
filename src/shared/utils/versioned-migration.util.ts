import type {
  SchemaMigrationDef,
  TableMigrationDef,
  VersionedDataMigration,
  VersionedSchemaMigration,
} from '../types/schema-migration.types';
import {
  compareEnfyraVersions,
  isEnfyraVersion,
  normalizeEnfyraVersion,
} from './enfyra-version.util';

export interface ResolvedVersionRange {
  appliedSteps: number;
  /**
   * The version the declarations were scoped against. A database that predates
   * version tracking has no recorded version, so the oldest supported source is
   * assumed and reported here.
   */
  sourceVersion: string;
  assumedSourceVersion: boolean;
}

export interface ResolvedSchemaMigration extends ResolvedVersionRange {
  /**
   * Every applicable step merged into the single definition the migration runner
   * consumes, or null when the database is already at the target version.
   */
  migration: SchemaMigrationDef | null;
}

export interface ResolvedDataMigration extends ResolvedVersionRange {
  data: Record<string, any> | null;
}

function mergeTableMigrations(
  steps: readonly VersionedSchemaMigration[],
): TableMigrationDef[] {
  const merged = new Map<string, TableMigrationDef>();
  for (const step of steps) {
    for (const table of step.schema.tables ?? []) {
      const key = table._unique.name._eq;
      const existing = merged.get(key);
      if (!existing) {
        merged.set(key, {
          ...table,
          columnsToModify: [...(table.columnsToModify ?? [])],
          columnsToRemove: [...(table.columnsToRemove ?? [])],
          relationsToModify: [...(table.relationsToModify ?? [])],
          relationsToRemove: [...(table.relationsToRemove ?? [])],
        });
        continue;
      }
      merged.set(key, {
        ...existing,
        ...(table.tableToModify !== undefined
          ? { tableToModify: table.tableToModify }
          : {}),
        columnsToModify: [
          ...(existing.columnsToModify ?? []),
          ...(table.columnsToModify ?? []),
        ],
        columnsToRemove: [
          ...(existing.columnsToRemove ?? []),
          ...(table.columnsToRemove ?? []),
        ],
        relationsToModify: [
          ...(existing.relationsToModify ?? []),
          ...(table.relationsToModify ?? []),
        ],
        relationsToRemove: [
          ...(existing.relationsToRemove ?? []),
          ...(table.relationsToRemove ?? []),
        ],
      });
    }
  }
  return [...merged.values()];
}

function mergeSteps(
  steps: readonly VersionedSchemaMigration[],
): SchemaMigrationDef {
  return {
    ...(steps.some((step) => (step.schema.mongoColumnTypesToModify ?? []).length > 0)
      ? {
          mongoColumnTypesToModify: steps.flatMap(
            (step) => step.schema.mongoColumnTypesToModify ?? [],
          ),
        }
      : {}),
    coreTablesToRename: steps.flatMap(
      (step) => step.schema.coreTablesToRename ?? [],
    ),
    tablesToRename: steps.flatMap((step) => step.schema.tablesToRename ?? []),
    physicalTablesToDrop: steps.flatMap(
      (step) => step.schema.physicalTablesToDrop ?? [],
    ),
    physicalTablesToRename: steps.flatMap(
      (step) => step.schema.physicalTablesToRename ?? [],
    ),
    tables: mergeTableMigrations(steps),
    tablesToDrop: steps.flatMap((step) => step.schema.tablesToDrop ?? []),
  };
}

/**
 * Selects the migration steps that carry a database from its recorded version to
 * the running version. A database that predates version tracking is assumed to sit
 * at the oldest supported source, and an older one fails loudly instead of being
 * migrated with declarations written for a different starting point.
 */
export function resolveSchemaMigration(
  steps: readonly VersionedSchemaMigration[],
  dbVersion: unknown,
): ResolvedSchemaMigration {
  const { applicable, ...range } = selectApplicableSteps(steps, dbVersion);
  if (applicable.length === 0) return { migration: null, ...range };
  return {
    migration: mergeSteps(applicable as VersionedSchemaMigration[]),
    ...range,
  };
}

export function resolveDataMigration(
  steps: readonly VersionedDataMigration[],
  dbVersion: unknown,
): ResolvedDataMigration {
  const { applicable, ...range } = selectApplicableSteps(steps, dbVersion);
  if (applicable.length === 0) return { data: null, ...range };

  const merged: Record<string, any> = {};
  for (const step of applicable as VersionedDataMigration[]) {
    for (const [key, value] of Object.entries(step.data ?? {})) {
      merged[key] = [...toRecordList(merged[key]), ...toRecordList(value)];
    }
  }
  return { data: merged, ...range };
}

function toRecordList(value: unknown): any[] {
  if (value === undefined || value === null) return [];
  return Array.isArray(value) ? value : [value];
}

function selectApplicableSteps<T extends { fromVersion: string; toVersion: string }>(
  steps: readonly T[],
  dbVersion: unknown,
): ResolvedVersionRange & { applicable: T[] } {
  if (steps.length === 0) {
    return {
      appliedSteps: 0,
      sourceVersion: normalizeEnfyraVersion(dbVersion),
      assumedSourceVersion: false,
      applicable: [],
    };
  }

  const ordered = [...steps].sort((left, right) =>
    compareEnfyraVersions(left.toVersion, right.toVersion),
  );
  assertContiguousSteps(ordered);
  const oldestSource = ordered[0].fromVersion;
  const recordedVersion = normalizeEnfyraVersion(dbVersion);
  const assumedSourceVersion = recordedVersion === '';
  if (!assumedSourceVersion && !isEnfyraVersion(recordedVersion)) {
    throw new Error(
      `enfyra_setting.enfyraVersion holds "${recordedVersion}", which is not a valid Enfyra version. Fix the recorded value before upgrading.`,
    );
  }
  const sourceVersion = assumedSourceVersion ? oldestSource : recordedVersion;

  if (compareEnfyraVersions(sourceVersion, oldestSource) < 0) {
    throw new Error(
      `Cannot upgrade from Enfyra ${sourceVersion}: the oldest supported upgrade source is ${oldestSource}. Upgrade to ${oldestSource} first.`,
    );
  }

  const applicable = ordered.filter(
    (step) => compareEnfyraVersions(sourceVersion, step.toVersion) < 0,
  );
  return {
    appliedSteps: applicable.length,
    sourceVersion,
    assumedSourceVersion,
    applicable,
  };
}

/**
 * Steps are selected by comparing versions, so a gap or an overlap would apply
 * declarations written for a starting point the database never reached. These
 * declarations drive destructive DDL, so the chain is asserted instead of trusted.
 */
function assertContiguousSteps(
  ordered: readonly { fromVersion: string; toVersion: string }[],
): void {
  for (let index = 1; index < ordered.length; index++) {
    const previous = ordered[index - 1];
    const current = ordered[index];
    if (compareEnfyraVersions(previous.toVersion, current.fromVersion) !== 0) {
      throw new Error(
        `Enfyra migration steps are not contiguous: ${previous.toVersion} does not lead to ${current.fromVersion}.`,
      );
    }
  }
}
