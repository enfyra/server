import { bootstrapSourceArtifacts } from '../../../data';
import { validateBootstrapDataFiles } from '../../../domain/bootstrap/utils/bootstrap-data-validator.util';
import { setBootstrapSnapshot } from '../../../domain/bootstrap/utils/snapshot-meta.util';
import type { SchemaMigrationDef } from '../../../shared/types/schema-migration.types';
import type {
  BootstrapDataMigration,
  BootstrapDefaultData,
  BootstrapDefinition,
  BootstrapSnapshot,
  BootstrapSourceArtifacts,
} from '../types';
import { applyDataMigrationMetadataTargets } from '../utils/data-migration-target.util';
import { validateSnapshotMigrationDefinition } from '../utils/metadata-migration.util';

export class BootstrapDefinitionService {
  private readonly definition: BootstrapDefinition;

  constructor(
    _deps?: unknown,
    sources: BootstrapSourceArtifacts = bootstrapSourceArtifacts,
  ) {
    const { snapshot, migration, defaultData, dataMigration } = sources;

    validateSnapshotMigrationDefinition(snapshot, migration);
    const issues = validateBootstrapDataFiles({
      snapshot,
      defaultData,
      dataMigration,
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

    this.definition = this.deepFreeze({
      snapshot,
      migration,
      defaultData,
      dataMigration,
      dataTargetSnapshot: applyDataMigrationMetadataTargets(
        snapshot,
        dataMigration,
      ),
    });
    setBootstrapSnapshot(this.definition.snapshot);
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
