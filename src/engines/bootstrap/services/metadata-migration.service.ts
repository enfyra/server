import { Logger } from '../../../shared/logger';
import { QueryBuilderService } from '@enfyra/kernel';
import { Db } from 'mongodb';
import type {
  SchemaMigrationDef,
  SnapshotMigrationMetadataState,
  TableMigrationDef,
} from '../../../shared/types/schema-migration.types';
import { bootstrapVerboseLog } from '../utils/bootstrap-logging.util';
import { compileMetadataMigrationExecutionPlan } from '../utils/metadata-migration-plan.util';
import { SystemCoreTableResolver } from './system-core-table-resolver.service';
import {
  validateSnapshotMigrationCoverage,
  validateSnapshotTargetState,
} from '../utils/metadata-migration.util';
import { MetadataPhysicalMigrationHelper } from '../utils/metadata-physical-migration.util';
import {
  applyMongoSchemaMigrations,
  applySqlSchemaMigrations,
} from '../../../shared/utils/provision-schema-migration';
import { normalizeMongoPrimaryKeyColumn } from '../../../modules/table-management/utils/mongo-primary-key.util';
import { BootstrapDefinitionService } from './bootstrap-definition.service';
import { MetadataTableMigrationService } from './metadata-migration/metadata-table-migration.service';
import { MetadataTableRenameService } from './metadata-migration/metadata-table-rename.service';
import type {
  BootstrapSchemaExecutionPlan,
  BootstrapSchemaOperation,
  BootstrapSchemaOperationCompleted,
} from '../types';

export class MetadataMigrationService {
  private readonly logger = new Logger(MetadataMigrationService.name);
  private readonly queryBuilderService: QueryBuilderService;
  private readonly systemCoreTableResolver: SystemCoreTableResolver;
  private readonly bootstrapDefinitionService: BootstrapDefinitionService;
  private readonly physicalMigration: MetadataPhysicalMigrationHelper;
  private readonly tableMigration: MetadataTableMigrationService;
  private readonly tableRename: MetadataTableRenameService;
  private migrations: SchemaMigrationDef | null = null;
  private executionPlan: BootstrapSchemaExecutionPlan | null = null;
  private readonly executedPlanNodeIds = new Set<string>();

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    systemCoreTableResolver: SystemCoreTableResolver;
    bootstrapDefinitionService?: BootstrapDefinitionService;
  }) {
    this.queryBuilderService = deps.queryBuilderService;
    this.systemCoreTableResolver = deps.systemCoreTableResolver;
    this.bootstrapDefinitionService =
      deps.bootstrapDefinitionService ?? new BootstrapDefinitionService();
    this.physicalMigration = new MetadataPhysicalMigrationHelper({
      queryBuilderService: this.queryBuilderService,
      verbose: (message) => this.verbose(message),
    });
    const migrationDeps = {
      queryBuilderService: this.queryBuilderService,
      systemCoreTableResolver: this.systemCoreTableResolver,
      physicalMigration: this.physicalMigration,
      verbose: (message: string) => this.verbose(message),
    };
    this.tableMigration = new MetadataTableMigrationService(migrationDeps);
    this.tableRename = new MetadataTableRenameService(migrationDeps);
    this.migrations = this.bootstrapDefinitionService.getMigration();
    if (this.migrations) {
      this.verbose(
        `Loaded snapshot-migration.ts with ${this.migrations.tables?.length || 0} table migration(s)`,
      );
    }
  }

  private getMongoDb(): Db | null {
    if (!this.queryBuilderService.isMongoDb()) return null;
    return this.queryBuilderService.getMongoDb();
  }

  private async hasMetadataStore(): Promise<boolean> {
    const coreNames = await this.systemCoreTableResolver.getNames();
    if (this.queryBuilderService.isMongoDb()) {
      return this.physicalMigration.mongoCollectionExists(coreNames.table);
    }
    return this.queryBuilderService.getKnex().schema.hasTable(coreNames.table);
  }

  async prepareMigrationExecutionPlan(): Promise<BootstrapSchemaExecutionPlan> {
    const hasMetadataStore = await this.hasMetadataStore();
    let observedMetadata = { tables: 0, columns: 0, relations: 0 };
    if (hasMetadataStore) {
      const { snapshot, dataTargetSnapshot, state } =
        await this.loadSnapshotMigrationState();
      validateSnapshotMigrationCoverage(
        snapshot,
        this.migrations,
        state,
        dataTargetSnapshot,
      );
      observedMetadata = {
        tables: state.tables.length,
        columns: state.columns.length,
        relations: state.relations.length,
      };
    }

    const plan = compileMetadataMigrationExecutionPlan(this.migrations, {
      mode: hasMetadataStore ? 'upgrade' : 'install',
      database: this.getPlanDatabase(),
      targetTableCount: Object.keys(
        this.bootstrapDefinitionService.getSnapshot(),
      ).length,
      observedMetadata,
    });
    this.executionPlan = plan;
    this.executedPlanNodeIds.clear();
    this.tableRename.reset();
    return this.executionPlan;
  }

  async validateMigrationCoverageBeforeMetadataSync(): Promise<void> {
    await this.prepareMigrationExecutionPlan();
  }

  getExecutionPlan(): BootstrapSchemaExecutionPlan {
    if (!this.executionPlan) {
      throw new Error(
        'Snapshot migration execution plan has not been prepared.',
      );
    }
    return this.executionPlan;
  }

  private getPlanDatabase(): BootstrapSchemaExecutionPlan['database'] {
    if (this.queryBuilderService.isMongoDb()) return 'mongodb';
    const client = String(
      this.queryBuilderService.getKnex().client?.config?.client ?? '',
    ).toLowerCase();
    return client.includes('pg') || client.includes('postgres')
      ? 'postgresql'
      : 'mysql';
  }

  async executeCoreMigrationPlan(
    onOperationCompleted?: BootstrapSchemaOperationCompleted,
    beforeNode?: () => Promise<void>,
  ): Promise<void> {
    await this.executePlanCheckpoint('core', onOperationCompleted, beforeNode);
  }

  async executeRemainingMigrationPlan(
    onOperationCompleted?: BootstrapSchemaOperationCompleted,
    beforeNode?: () => Promise<void>,
  ): Promise<void> {
    await this.executePlanCheckpoint('remaining', onOperationCompleted, beforeNode);
  }

  private async executePlanCheckpoint(
    checkpoint: 'core' | 'remaining',
    onOperationCompleted?: BootstrapSchemaOperationCompleted,
    beforeNode?: () => Promise<void>,
  ): Promise<void> {
    const plan = this.getExecutionPlan();
    for (const phase of plan.phases) {
      const nodes = phase.nodes.filter(
        (node) =>
          node.checkpoint === checkpoint &&
          !this.executedPlanNodeIds.has(node.id),
      );
      if (nodes.length === 0) continue;
      this.verbose(`Executing migration phase ${phase.index}`);
      for (const node of nodes) {
        if (beforeNode) {
          await beforeNode();
        }
        if (node.command.backend !== plan.database) {
          throw new Error(
            `Bootstrap migration node ${node.id} targets ${node.command.backend}, not ${plan.database}.`,
          );
        }
        const missingDependency = node.dependsOn.find(
          (dependencyId) => !this.executedPlanNodeIds.has(dependencyId),
        );
        if (missingDependency) {
          throw new Error(
            `Bootstrap migration node ${node.id} cannot run before ${missingDependency}.`,
          );
        }
        await this.executePlanCommand(node.command);
        this.executedPlanNodeIds.add(node.id);
        if (node.completesChange) {
          await onOperationCompleted?.(node.command.operation);
        }
      }
    }
  }

  private async executePlanCommand(
    command: BootstrapSchemaExecutionPlan['phases'][number]['nodes'][number]['command'],
  ): Promise<void> {
    const isMongoDB = this.queryBuilderService.isMongoDb();
    const operation = command.operation;
    switch (command.kind) {
      case 'rename-core-table':
        if (operation.kind !== 'rename-core-table') {
          throw new Error(`Invalid command payload for ${command.kind}.`);
        }
        if (isMongoDB) {
          await this.tableRename.runMongoCoreTableRenames([operation.rename]);
        } else {
          await this.tableRename.runSqlCoreTableRenames([operation.rename]);
        }
        return;
      case 'rename-table':
        if (operation.kind !== 'rename-table') {
          throw new Error(`Invalid command payload for ${command.kind}.`);
        }
        await this.tableRename.runTableRenames([operation.rename], isMongoDB);
        return;
      case 'rename-physical-table':
        if (operation.kind !== 'rename-physical-table') {
          throw new Error(`Invalid command payload for ${command.kind}.`);
        }
        await this.physicalMigration.runPhysicalTableRenames(
          [operation.rename],
          isMongoDB,
        );
        return;
      case 'drop-physical-table':
        if (operation.kind !== 'drop-physical-table') {
          throw new Error(`Invalid command payload for ${command.kind}.`);
        }
        await this.physicalMigration.dropPhysicalTables(
          [operation.tableName],
          isMongoDB,
        );
        return;
      case 'apply-physical-change':
        await this.executePhysicalOperation(operation);
        return;
      case 'apply-metadata-change':
        if (operation.kind === 'drop-table') {
          await this.tableMigration.dropTableMetadata(
            [operation.tableName],
            isMongoDB,
          );
          return;
        }
        const tableMigration = this.toTableMigration(operation);
        if (!tableMigration) {
          throw new Error(`Invalid command payload for ${command.kind}.`);
        }
        await this.tableMigration.migrateTableMetadata(
          tableMigration,
          isMongoDB,
        );
        return;
      case 'cleanup-renamed-table':
        if (
          operation.kind !== 'rename-core-table' &&
          operation.kind !== 'rename-table'
        ) {
          throw new Error(`Invalid command payload for ${command.kind}.`);
        }
        await this.tableRename.cleanupRenamedTables(
          [operation.rename],
          isMongoDB,
        );
    }
  }

  private async executePhysicalOperation(
    operation: BootstrapSchemaOperation,
  ): Promise<void> {
    const tableMigration = this.toTableMigration(operation);
    const migration: SchemaMigrationDef = {
      tables: tableMigration ? [tableMigration] : [],
      tablesToDrop:
        operation.kind === 'drop-table' ? [operation.tableName] : [],
    };
    if (this.queryBuilderService.isMongoDb()) {
      const snapshot = this.bootstrapDefinitionService.getSnapshot();
      const preserveFieldsByCollection = Object.fromEntries(
        Object.entries(snapshot).map(
          ([tableName, definition]: [string, any]) => [
            tableName,
            [
              ...(definition.columns ?? []).map((column: any) => column.name),
              ...(definition.relations ?? []).map(
                (relation: any) => relation.propertyName,
              ),
            ],
          ],
        ),
      );
      await applyMongoSchemaMigrations(
        this.queryBuilderService.getMongoDb(),
        migration,
        { preserveFieldsByCollection },
      );
      return;
    }
    await applySqlSchemaMigrations(
      this.queryBuilderService.getKnex(),
      migration,
    );
  }

  private toTableMigration(
    operation: BootstrapSchemaOperation,
  ): TableMigrationDef | null {
    const _unique = {
      name: { _eq: 'tableName' in operation ? operation.tableName : '' },
    };
    switch (operation.kind) {
      case 'modify-table':
        return { _unique, tableToModify: operation.modification };
      case 'modify-column':
        return { _unique, columnsToModify: [operation.modification] };
      case 'remove-column':
        return { _unique, columnsToRemove: [operation.columnName] };
      case 'modify-relation':
        return { _unique, relationsToModify: [operation.modification] };
      case 'remove-relation':
        return { _unique, relationsToRemove: [operation.propertyName] };
      default:
        return null;
    }
  }

  async assertSnapshotTargetStateAfterHealing(): Promise<void> {
    const { snapshot, dataTargetSnapshot, state } =
      await this.loadSnapshotMigrationState();
    validateSnapshotTargetState(
      snapshot,
      state,
      this.migrations,
      dataTargetSnapshot,
    );
  }

  private async loadSnapshotMigrationState(): Promise<{
    snapshot: Record<string, any>;
    dataTargetSnapshot: Record<string, any>;
    state: SnapshotMigrationMetadataState;
  }> {
    let snapshot = this.bootstrapDefinitionService.getSnapshot();
    let dataTargetSnapshot =
      this.bootstrapDefinitionService.getDataTargetSnapshot();
    const coreNames = await this.systemCoreTableResolver.getNames();
    let tables: any[];
    let columns: any[];
    let relations: any[];

    if (this.queryBuilderService.isMongoDb()) {
      const normalizeSnapshot = (input: Record<string, any>) =>
        Object.fromEntries(
          Object.entries(input).map(
            ([tableName, definition]: [string, any]) => [
              tableName,
              {
                ...definition,
                uniques: definition.uniques ?? null,
                indexes: definition.indexes ?? null,
                columns: (definition.columns ?? []).map(
                  normalizeMongoPrimaryKeyColumn,
                ),
              },
            ],
          ),
        );
      snapshot = normalizeSnapshot(snapshot);
      dataTargetSnapshot = normalizeSnapshot(dataTargetSnapshot);
      const db = this.getMongoDb()!;
      [tables, columns, relations] = await Promise.all([
        db.collection(coreNames.table).find({}).toArray(),
        db.collection(coreNames.column).find({}).toArray(),
        db.collection(coreNames.relation).find({}).toArray(),
      ]);
    } else {
      const knex = this.queryBuilderService.getKnex();
      [tables, columns, relations] = await Promise.all([
        knex(coreNames.table).select('*'),
        knex(coreNames.column).select('*'),
        knex(coreNames.relation).select('*'),
      ]);
    }

    const isMongoDB = this.queryBuilderService.isMongoDb();
    const tableById = new Map(
      tables.map((table) => [String(isMongoDB ? table._id : table.id), table]),
    );
    const relationById = new Map(
      relations.map((relation) => [
        String(isMongoDB ? relation._id : relation.id),
        relation,
      ]),
    );
    const state: SnapshotMigrationMetadataState = {
      tables,
      columns: columns.map((column) => {
        const normalizedColumn = isMongoDB
          ? normalizeMongoPrimaryKeyColumn(column)
          : column;
        return {
          ...normalizedColumn,
          tableName: tableById.get(
            String(isMongoDB ? column.table : column.tableId),
          )?.name,
        };
      }),
      relations: relations.map((relation) => {
        const relationId = isMongoDB ? relation._id : relation.id;
        const mappedById = isMongoDB ? relation.mappedBy : relation.mappedById;
        const counterpart = mappedById
          ? relationById.get(String(mappedById))
          : relations.find(
              (candidate) =>
                String(
                  isMongoDB ? candidate.mappedBy : candidate.mappedById,
                ) === String(relationId),
            );
        return {
          ...relation,
          sourceTableName: tableById.get(
            String(isMongoDB ? relation.sourceTable : relation.sourceTableId),
          )?.name,
          targetTable: tableById.get(
            String(isMongoDB ? relation.targetTable : relation.targetTableId),
          )?.name,
          targetTableName: tableById.get(
            String(isMongoDB ? relation.targetTable : relation.targetTableId),
          )?.name,
          mappedBy: mappedById
            ? relationById.get(String(mappedById))?.propertyName
            : null,
          mappedByPropertyName: mappedById
            ? relationById.get(String(mappedById))?.propertyName
            : undefined,
          inversePropertyName: counterpart?.propertyName,
        };
      }),
    };

    return { snapshot, dataTargetSnapshot, state };
  }

  private verbose(message: string): void {
    bootstrapVerboseLog(this.logger, message);
  }
}
