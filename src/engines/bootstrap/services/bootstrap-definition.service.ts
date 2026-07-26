import * as fs from 'node:fs';
import * as path from 'node:path';
import { validateBootstrapDataFiles } from '../../../domain/bootstrap/utils/bootstrap-data-validator.util';
import { setBootstrapSnapshot } from '../../../domain/bootstrap/utils/snapshot-meta.util';
import type { SchemaMigrationDef } from '../../../shared/types/schema-migration.types';
import type {
  BootstrapDataMigration,
  BootstrapDefaultData,
  BootstrapDefinition,
  BootstrapSnapshot,
} from '../types';
import { applyDataMigrationMetadataTargets } from '../utils/data-migration-target.util';
import { validateSnapshotMigrationDefinition } from '../utils/metadata-migration.util';

export class BootstrapDefinitionService {
  private readonly definition: BootstrapDefinition;

  constructor(deps: { bootstrapDataRoot: string }) {
    const snapshot = this.readRequired<BootstrapSnapshot>(
      deps.bootstrapDataRoot,
      'snapshot.json',
    );
    const migration = this.readOptional<SchemaMigrationDef>(
      deps.bootstrapDataRoot,
      'snapshot-migration.json',
    );
    const defaultData = this.readRequired<BootstrapDefaultData>(
      deps.bootstrapDataRoot,
      'default-data.json',
    );
    const dataMigration =
      this.readOptional<BootstrapDataMigration>(
        deps.bootstrapDataRoot,
        'data-migration.json',
      ) ?? {};

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

  private readRequired<T>(root: string, fileName: string): T {
    const filePath = path.join(root, 'data', fileName);
    if (!fs.existsSync(filePath)) {
      throw new Error(`Required bootstrap file not found: ${filePath}`);
    }
    return this.parseFile<T>(filePath);
  }

  private readOptional<T>(root: string, fileName: string): T | null {
    const filePath = path.join(root, 'data', fileName);
    return fs.existsSync(filePath) ? this.parseFile<T>(filePath) : null;
  }

  private parseFile<T>(filePath: string): T {
    try {
      return JSON.parse(fs.readFileSync(filePath, 'utf8')) as T;
    } catch (error) {
      throw new Error(
        `Failed to load bootstrap file ${filePath}: ${(error as Error).message}`,
      );
    }
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
