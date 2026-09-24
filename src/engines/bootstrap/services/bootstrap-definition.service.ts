import { bootstrapSourceArtifacts } from '../../../data';
import { validateBootstrapDataFiles } from '../../../domain/bootstrap/utils/bootstrap-data-validator.util';
import { setBootstrapSnapshot } from '../../../domain/bootstrap/utils/snapshot-meta.util';
import type {
  SchemaMigrationDef,
  VersionedDataMigration,
  VersionedSchemaMigration,
} from '../../../shared/types/schema-migration.types';
import type {
  BootstrapDataMigration,
  BootstrapDefaultData,
  BootstrapDefinition,
  BootstrapSnapshot,
  BootstrapSourceArtifacts,
} from '../types';
import { applyDataMigrationMetadataTargets } from '../utils/data-migration-target.util';
import { validateSnapshotMigrationDefinition } from '../utils/metadata-migration.util';
import type { DatabaseConfigService } from '../../../shared/services';
import {
  resolveDataMigration,
  resolveSchemaMigration,
} from '../../../shared/utils/versioned-migration.util';
import { compareEnfyraVersions } from '../../../shared/utils/enfyra-version.util';
import {
  toMongoTargetDataMigration,
  toMongoTargetMigration,
  toMongoTargetSnapshot,
  toSqlTargetDataMigration,
  toSqlTargetMigration,
  toSqlTargetSnapshot,
} from '../../../shared/utils/column-type.util';

/**
 * Merges record targets per table, keeping declaration order. Standing corrections
 * come first so a version step can refine the same record.
 */
function mergeRecords(
  base: Record<string, any>,
  extra: Record<string, any> | null,
): Record<string, any> {
  const merged: Record<string, any> = {};
  const append = (source: Record<string, any>) => {
    for (const [key, value] of Object.entries(source)) {
      merged[key] = [
        ...(merged[key] ?? []),
        ...(Array.isArray(value) ? value : [value]),
      ];
    }
  };
  append(base);
  if (extra) append(extra);
  return merged;
}

/**
 * Holds the backend-projected bootstrap target plus the versioned declarations that
 * carry an older database onto it. The target itself never depends on the recorded
 * version; only the declarations do, and they are resolved once at boot by
 * `resolveForVersion` before any consumer reads them.
 */

export class BootstrapDefinitionService {
  private readonly sourceSnapshot: BootstrapSnapshot;
  private readonly defaultData: BootstrapDefaultData;
  private readonly migrations: VersionedSchemaMigration[];
  private readonly dataMigrations: VersionedDataMigration[];
  private readonly dataCorrections: Record<string, any>;
  private readonly isMongo: boolean;
  private readonly sqlSnapshot: BootstrapSnapshot;
  private resolvedVersion: string | null = null;
  private definition: BootstrapDefinition;

  constructor(
    deps?: { databaseConfigService?: DatabaseConfigService },
    sources: BootstrapSourceArtifacts = bootstrapSourceArtifacts,
  ) {
    this.sourceSnapshot = sources.snapshot;
    this.defaultData = sources.defaultData;
    this.migrations = sources.migrations;
    this.dataMigrations = sources.dataMigrations;
    this.dataCorrections = sources.dataCorrections;

    const dbType = deps?.databaseConfigService?.getDbType();
    this.isMongo = dbType === 'mongodb';
    this.sqlSnapshot = toSqlTargetSnapshot(this.sourceSnapshot);

    // The runner applies the whole chain, so the merged result is what must agree
    // with the final snapshot. Validating each step against the snapshot in
    // isolation would reject a later step that refines an earlier one's target.
    validateSnapshotMigrationDefinition(
      this.sqlSnapshot,
      toSqlTargetMigration(
        resolveSchemaMigration(this.migrations, this.oldestSourceVersion())
          .migration,
      ),
    );

    const issues = validateBootstrapDataFiles({
      snapshot: this.sqlSnapshot,
      defaultData: this.defaultData,
      dataMigration: toSqlTargetDataMigration(
        this.collectDeclaredData(),
      ) as unknown as Record<string, any>,
    });
    if (issues.length > 0) {
      throw new Error(
        `Invalid bootstrap data:\n- ${issues
          .map(
            (issue) =>
              `${issue.file}:${issue.table}${issue.path ? `:${issue.path}` : ''}:${issue.field} ${issue.message}`,
          )
          .join('\n- ')}`,
      );
    }

    this.definition = this.buildDefinition(
      null,
      mergeRecords(this.dataCorrections, null),
    );
    setBootstrapSnapshot(this.definition.snapshot);
  }

  /**
   * Selects the declarations that carry a database from its recorded version to the
   * running version. A database that predates version tracking resolves against the
   * oldest supported source. Re-resolving for the same version is a no-op.
   */
  resolveForVersion(dbVersion: unknown): BootstrapDefinition {
    const schema = resolveSchemaMigration(this.migrations, dbVersion);
    if (this.resolvedVersion === schema.sourceVersion) return this.definition;

    const data = resolveDataMigration(this.dataMigrations, dbVersion);
    this.definition = this.buildDefinition(
      schema.migration,
      mergeRecords(this.dataCorrections, data.data),
    );
    this.resolvedVersion = schema.sourceVersion;
    return this.definition;
  }

  getDefinition(): BootstrapDefinition {
    return this.definition;
  }

  getSnapshot(): BootstrapSnapshot {
    return this.definition.snapshot;
  }

  getMigration(): SchemaMigrationDef | null {
    return this.definition.migration;
  }

  getDefaultData(): BootstrapDefaultData {
    return this.definition.defaultData;
  }

  getDataMigration(): BootstrapDataMigration {
    return this.definition.dataMigration;
  }

  getDataTargetSnapshot(): BootstrapSnapshot {
    return this.definition.dataTargetSnapshot;
  }

  getResolvedSourceVersion(): string {
    return this.resolvedVersion ?? '';
  }

  /**
   * The oldest version the declared chain can start from, which is the point where
   * every step is applicable and the merged result must match the final target.
   */
  private oldestSourceVersion(): string {
    return this.migrations.length === 0
      ? ''
      : this.migrations
          .map((step) => step.fromVersion)
          .reduce((oldest, candidate) =>
            compareEnfyraVersions(candidate, oldest) < 0 ? candidate : oldest,
          );
  }

  /**
   * The standing corrections plus every declared step, merged per table. Used for
   * validation, which must see the union of what any version could apply.
   */
  private collectDeclaredData(): Record<string, any> {
    return this.dataMigrations.reduce<Record<string, any>>(
      (merged, step) => mergeRecords(merged, step.data ?? {}),
      mergeRecords(this.dataCorrections, null),
    );
  }

  private buildDefinition(
    resolvedSchema: SchemaMigrationDef | null,
    resolvedData: Record<string, any>,
  ): BootstrapDefinition {
    const project = this.isMongo ? toMongoTargetSnapshot : toSqlTargetSnapshot;
    const projectMigration = this.isMongo
      ? toMongoTargetMigration
      : toSqlTargetMigration;
    const projectData = this.isMongo
      ? toMongoTargetDataMigration
      : toSqlTargetDataMigration;

    const dataMigration = projectData(resolvedData);

    return this.deepFreeze({
      snapshot: project(this.sourceSnapshot),
      migration: projectMigration(resolvedSchema),
      defaultData: this.defaultData,
      dataMigration,
      dataTargetSnapshot: project(
        applyDataMigrationMetadataTargets(this.sourceSnapshot, dataMigration),
      ),
    });
  }

  private deepFreeze<T>(value: T): T {
    if (!value || typeof value !== 'object' || Object.isFrozen(value)) {
      return value;
    }
    for (const nested of Object.values(value as Record<string, unknown>)) {
      this.deepFreeze(nested);
    }
    return Object.freeze(value);
  }
}
