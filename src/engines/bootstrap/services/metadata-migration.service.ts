import { Logger } from '../../../shared/logger';
import { QueryBuilderService } from '@enfyra/kernel';
import { Db } from 'mongodb';
import {
  SchemaMigrationDef,
  SnapshotMigrationMetadataState,
  TableMigrationDef,
  ColumnModifyDef,
  RelationModifyDef,
  TableRenameDef,
} from '../../../shared/types/schema-migration.types';
import { bootstrapVerboseLog } from '../utils/bootstrap-logging.util';
import { SystemCoreTableResolver } from './system-core-table-resolver.service';
import {
  buildColumnMetadataUpdate,
  buildRelationMetadataUpdate,
  buildTableMetadataUpdate,
  getLegacyScriptTargetColumn,
  getValidTableRenames,
  hasColumnMetadataChanges,
  hasRelationMetadataChanges,
  hasSchemaMigrations,
  hasTableMetadataChanges,
  validateSnapshotMigrationCoverage,
  validateSnapshotTargetState,
} from '../utils/metadata-migration.util';
import {
  CORE_SYSTEM_TABLES,
  LEGACY_CORE_SYSTEM_TABLES,
  SYSTEM_TABLES,
} from '../../../shared/utils/system-tables.constants';
import { MetadataPhysicalMigrationHelper } from '../utils/metadata-physical-migration.util';
import {
  applyMongoSchemaMigrations,
  applySqlSchemaMigrations,
} from '../../../shared/utils/provision-schema-migration';
import { normalizeMongoPrimaryKeyColumn } from '../../../modules/table-management/utils/mongo-primary-key.util';
import { BootstrapDefinitionService } from './bootstrap-definition.service';
import type { BootstrapSchemaExecutionPlan } from '../types';

export class MetadataMigrationService {
  private readonly logger = new Logger(MetadataMigrationService.name);
  private readonly queryBuilderService: QueryBuilderService;
  private readonly systemCoreTableResolver: SystemCoreTableResolver;
  private readonly bootstrapDefinitionService: BootstrapDefinitionService;
  private readonly physicalMigration: MetadataPhysicalMigrationHelper;
  private migrations: SchemaMigrationDef | null = null;
  private executionPlan: BootstrapSchemaExecutionPlan | null = null;
  private readonly sqlCoreTableIdRemap = new Map<string, any>();
  private readonly mongoCoreTableIdRemap = new Map<string, any>();

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
    this.migrations = this.bootstrapDefinitionService.getMigration();
    if (this.migrations) {
      this.verbose(
        `Loaded snapshot-migration.ts with ${this.migrations.tables?.length || 0} table migration(s)`,
      );
    }
  }

  hasMigrations(): boolean {
    return hasSchemaMigrations(this.migrations);
  }

  private getMongoDb(): Db | null {
    if (!this.queryBuilderService.isMongoDb()) return null;
    return this.queryBuilderService.getMongoDb();
  }

  async runMigrations(): Promise<void> {
    if (!this.hasMigrations()) {
      this.verbose('No metadata migrations to run');
      return;
    }
    if (!(await this.hasMetadataStore())) {
      this.verbose(
        'Metadata store does not exist yet; skipping legacy metadata migrations',
      );
      return;
    }
    this.getExecutionPlan();

    this.verbose('Running metadata migrations from snapshot-migration.ts...');

    const isMongoDB = this.queryBuilderService.isMongoDb();

    const migrations = this.migrations!;
    await this.runTableRenames(migrations.tablesToRename ?? [], isMongoDB);

    const tablesToDrop = migrations.tablesToDrop ?? [];
    if (tablesToDrop.length > 0) {
      await this.dropTableMetadata(tablesToDrop, isMongoDB);
    }

    for (const tableMigration of migrations.tables || []) {
      await this.migrateTableMetadata(tableMigration, isMongoDB);
    }

    this.verbose('Metadata migrations completed');
  }

  private async hasMetadataStore(): Promise<boolean> {
    const coreNames = await this.systemCoreTableResolver.getNames();
    if (this.queryBuilderService.isMongoDb()) {
      return this.physicalMigration.mongoCollectionExists(coreNames.table);
    }
    return this.queryBuilderService.getKnex().schema.hasTable(coreNames.table);
  }

  async runCoreTableRenamesBeforeMetadataSync(): Promise<void> {
    if (!this.migrations?.coreTablesToRename?.length) return;

    const isMongoDB = this.queryBuilderService.isMongoDb();
    if (isMongoDB) {
      await this.runMongoCoreTableRenames(this.migrations.coreTablesToRename);
      return;
    }

    await this.runSqlCoreTableRenames(this.migrations.coreTablesToRename);
  }

  async runTableRenamesBeforeMetadataSync(): Promise<void> {
    if (!this.migrations?.tablesToRename?.length) return;

    await this.runTableRenames(
      this.migrations.tablesToRename,
      this.queryBuilderService.isMongoDb(),
    );
  }

  async runPhysicalTableRenamesAndDropsAfterCoverage(): Promise<void> {
    if (!this.hasMigrations()) return;
    this.getExecutionPlan();
    await this.physicalMigration.runPhysicalTableRenames(
      this.migrations!.physicalTablesToRename ?? [],
      this.queryBuilderService.isMongoDb(),
    );

    await this.physicalMigration.dropPhysicalTables(
      this.migrations!.physicalTablesToDrop ?? [],
      this.queryBuilderService.isMongoDb(),
    );
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

    const migration = this.migrations;
    const plan: BootstrapSchemaExecutionPlan = {
      mode: hasMetadataStore ? 'upgrade' : 'install',
      database: this.queryBuilderService.isMongoDb() ? 'mongodb' : 'sql',
      targetTableCount: Object.keys(
        this.bootstrapDefinitionService.getSnapshot(),
      ).length,
      observedMetadata,
      operations: {
        tableRenames: (migration?.tablesToRename ?? []).map(
          (entry) => `${entry.from}->${entry.to}`,
        ),
        physicalTableRenames: (migration?.physicalTablesToRename ?? []).map(
          (entry) => `${entry.from}->${entry.to}`,
        ),
        physicalTableDrops: [...(migration?.physicalTablesToDrop ?? [])],
        tableMigrations: (migration?.tables ?? []).map(
          (entry) => entry._unique.name._eq,
        ),
        tableDrops: [...(migration?.tablesToDrop ?? [])],
      },
    };
    Object.freeze(plan.operations.tableRenames);
    Object.freeze(plan.operations.physicalTableRenames);
    Object.freeze(plan.operations.physicalTableDrops);
    Object.freeze(plan.operations.tableMigrations);
    Object.freeze(plan.operations.tableDrops);
    Object.freeze(plan.operations);
    Object.freeze(plan.observedMetadata);
    this.executionPlan = Object.freeze(plan);
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

  async runPhysicalMigrationsBeforeMetadataSync(): Promise<void> {
    if (!this.hasMigrations()) return;
    this.getExecutionPlan();

    const migrations = this.migrations!;
    for (const tableMigration of migrations.tables || []) {
      const tableName = tableMigration._unique.name._eq;
      for (const columnMigration of tableMigration.columnsToModify || []) {
        if (columnMigration.from.name === columnMigration.to.name) continue;
        if (this.queryBuilderService.isMongoDb()) {
          await this.physicalMigration.renameMongoDocumentFieldIfNeeded(
            tableName,
            columnMigration.from.name,
            columnMigration.to.name,
          );
        } else {
          await this.physicalMigration.renameSqlPhysicalColumnIfNeeded(
            tableName,
            columnMigration.from.name,
            columnMigration.to.name,
          );
        }
      }
      if (!this.queryBuilderService.isMongoDb()) continue;
      for (const relationMigration of tableMigration.relationsToModify || []) {
        if (
          relationMigration.from.propertyName ===
          relationMigration.to.propertyName
        )
          continue;
        await this.physicalMigration.renameMongoDocumentFieldIfNeeded(
          tableName,
          relationMigration.from.propertyName,
          relationMigration.to.propertyName,
        );
      }
    }

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
        migrations,
        { preserveFieldsByCollection },
      );
      return;
    }

    await applySqlSchemaMigrations(
      this.queryBuilderService.getKnex(),
      migrations,
    );
  }

  private async runTableRenames(
    renames: TableRenameDef[],
    isMongoDB: boolean,
  ): Promise<void> {
    for (const rename of renames) {
      if (!rename.from || !rename.to || rename.from === rename.to) continue;
      if (isMongoDB) {
        await this.renameMongoTable(rename);
      } else {
        await this.renameSqlTable(rename);
      }
    }
  }

  private async runSqlCoreTableRenames(
    renames: TableRenameDef[],
  ): Promise<void> {
    const knex = this.queryBuilderService.getKnex();
    const validRenames = getValidTableRenames(renames);

    for (const rename of validRenames) {
      const oldExists = await knex.schema.hasTable(rename.from);
      const newExists = await knex.schema.hasTable(rename.to);
      if (oldExists && newExists) {
        await this.reconcileSqlCoreTableOverlap(rename);
        this.verbose(
          `  Core SQL table overlap detected: ${rename.from} and ${rename.to} both exist; continuing with canonical ${rename.to}`,
        );
      }
    }

    for (const rename of validRenames) {
      const oldExists = await knex.schema.hasTable(rename.from);
      const newExists = await knex.schema.hasTable(rename.to);
      if (oldExists && !newExists) {
        await knex.schema.renameTable(rename.from, rename.to);
        this.verbose(`  Renamed core SQL table: ${rename.from} → ${rename.to}`);
      }
    }

    for (const rename of validRenames) {
      await this.renameSqlTableMetadataRow(SYSTEM_TABLES.table, rename);
      await this.updateSqlCanonicalRoutePath(rename);
    }
  }

  private async runMongoCoreTableRenames(
    renames: TableRenameDef[],
  ): Promise<void> {
    const db = this.getMongoDb()!;
    const validRenames = getValidTableRenames(renames);

    for (const rename of validRenames) {
      const oldExists = await this.physicalMigration.mongoCollectionExists(
        rename.from,
      );
      const newExists = await this.physicalMigration.mongoCollectionExists(
        rename.to,
      );
      if (oldExists && newExists) {
        await this.reconcileMongoCoreTableOverlap(rename);
        this.verbose(
          `  Core Mongo collection overlap detected: ${rename.from} and ${rename.to} both exist; continuing with canonical ${rename.to}`,
        );
      }
    }

    for (const rename of validRenames) {
      const oldExists = await this.physicalMigration.mongoCollectionExists(
        rename.from,
      );
      const newExists = await this.physicalMigration.mongoCollectionExists(
        rename.to,
      );
      if (oldExists && !newExists) {
        await db.collection(rename.from).rename(rename.to);
        this.verbose(
          `  Renamed core Mongo collection: ${rename.from} → ${rename.to}`,
        );
      }
    }

    for (const rename of validRenames) {
      await this.renameMongoTableMetadataRow(SYSTEM_TABLES.table, rename);
      await this.updateMongoCanonicalRoutePath(rename);
    }
  }

  private async renameSqlTable(rename: TableRenameDef): Promise<void> {
    const knex = this.queryBuilderService.getKnex();
    const oldExists = await knex.schema.hasTable(rename.from);
    const newExists = await knex.schema.hasTable(rename.to);

    if (oldExists && newExists) {
      await this.reconcileSqlTableOverlap(rename);
      this.verbose(
        `  SQL table overlap detected: ${rename.from} and ${rename.to} both exist; continuing with canonical ${rename.to}`,
      );
    }

    const tableStoreBefore =
      await this.systemCoreTableResolver.getTableName('table');
    const tableRecord = await this.findSqlTableRecord(
      tableStoreBefore,
      rename.from,
    );
    await this.updateSqlCanonicalRoutePath(rename, tableRecord?.id);

    if (oldExists && !newExists) {
      await knex.schema.renameTable(rename.from, rename.to);
      this.verbose(`  Renamed SQL table: ${rename.from} → ${rename.to}`);
    }

    const tableStoreAfter =
      await this.systemCoreTableResolver.getTableName('table');
    await this.renameSqlTableMetadataRow(
      tableStoreAfter,
      rename,
      tableRecord?.id,
    );
  }

  private async renameMongoTable(rename: TableRenameDef): Promise<void> {
    const db = this.getMongoDb()!;
    const oldExists = await this.physicalMigration.mongoCollectionExists(
      rename.from,
    );
    const newExists = await this.physicalMigration.mongoCollectionExists(
      rename.to,
    );

    if (oldExists && newExists) {
      await this.reconcileMongoTableOverlap(rename);
      this.verbose(
        `  Mongo collection overlap detected: ${rename.from} and ${rename.to} both exist; continuing with canonical ${rename.to}`,
      );
    }

    const tableStoreBefore =
      await this.systemCoreTableResolver.getTableName('table');
    const tableRecord = await db
      .collection(tableStoreBefore)
      .findOne({ name: rename.from });
    await this.updateMongoCanonicalRoutePath(rename, tableRecord?._id);

    if (oldExists && !newExists) {
      await db.collection(rename.from).rename(rename.to);
      this.verbose(`  Renamed Mongo collection: ${rename.from} → ${rename.to}`);
    }

    const tableStoreAfter =
      await this.systemCoreTableResolver.getTableName('table');
    await this.renameMongoTableMetadataRow(
      tableStoreAfter,
      rename,
      tableRecord?._id,
    );
  }

  private async findSqlTableRecord(
    tableStore: string,
    tableName: string,
  ): Promise<any | null> {
    const knex = this.queryBuilderService.getKnex();
    if (!(await knex.schema.hasTable(tableStore))) return null;
    return knex(tableStore).where({ name: tableName }).first();
  }

  private getCoreMetadataRowKey(
    rename: TableRenameDef,
    row: any,
    options: { remapOwnerIds?: boolean } = {},
  ): string | null {
    const remapOwnerIds = options.remapOwnerIds !== false;
    const tableName = rename.to || rename.from;
    if (tableName === SYSTEM_TABLES.table || tableName === 'table_definition') {
      return row?.name
        ? `table:${this.normalizeCoreTableName(row.name)}`
        : null;
    }

    if (
      tableName === SYSTEM_TABLES.column ||
      tableName === 'column_definition'
    ) {
      const owner = remapOwnerIds
        ? this.remapCoreTableId(rename, row?.tableId ?? row?.table)
        : (row?.tableId ?? row?.table);
      const name = row?.name;
      return owner !== undefined && owner !== null && name
        ? `column:${String(owner)}:${name}`
        : null;
    }

    if (
      tableName === SYSTEM_TABLES.relation ||
      tableName === 'relation_definition'
    ) {
      const owner = remapOwnerIds
        ? this.remapCoreTableId(rename, row?.sourceTableId ?? row?.sourceTable)
        : (row?.sourceTableId ?? row?.sourceTable);
      const propertyName = row?.propertyName;
      return owner !== undefined && owner !== null && propertyName
        ? `relation:${String(owner)}:${propertyName}`
        : null;
    }

    if (row?.name) return `name:${row.name}`;
    if (row?.propertyName) return `property:${row.propertyName}`;
    return null;
  }

  private normalizeCoreTableName(tableName: string): string {
    const entries = Object.entries(LEGACY_CORE_SYSTEM_TABLES) as Array<
      [keyof typeof LEGACY_CORE_SYSTEM_TABLES, string]
    >;
    const matched = entries.find(([, legacyName]) => legacyName === tableName);
    return matched ? CORE_SYSTEM_TABLES[matched[0]] : tableName;
  }

  private remapCoreTableId(rename: TableRenameDef, value: any): any {
    if (value === undefined || value === null) return value;
    const tableName = rename.to || rename.from;
    if (
      tableName !== SYSTEM_TABLES.column &&
      tableName !== 'column_definition' &&
      tableName !== SYSTEM_TABLES.relation &&
      tableName !== 'relation_definition'
    ) {
      return value;
    }

    const map = this.queryBuilderService.isMongoDb()
      ? this.mongoCoreTableIdRemap
      : this.sqlCoreTableIdRemap;
    return map.get(String(value)) ?? value;
  }

  private async getSqlOverlapColumns(
    oldTable: string,
    newTable: string,
  ): Promise<string[]> {
    const knex = this.queryBuilderService.getKnex();
    const [oldInfo, newInfo] = await Promise.all([
      knex(oldTable).columnInfo(),
      knex(newTable).columnInfo(),
    ]);
    return Object.keys(oldInfo).filter((column) => column in newInfo);
  }

  private async getSqlMergedColumns(
    oldTable: string,
    newTable: string,
  ): Promise<string[]> {
    const knex = this.queryBuilderService.getKnex();
    const [oldInfo, newInfo] = await Promise.all([
      knex(oldTable).columnInfo(),
      knex(newTable).columnInfo(),
    ]);
    const missingColumns = Object.keys(oldInfo).filter(
      (column) => !(column in newInfo),
    );
    if (missingColumns.length > 0) {
      await this.addMissingSqlColumns(newTable, oldInfo, missingColumns);
    }
    const refreshedNewInfo = await knex(newTable).columnInfo();
    return Object.keys(oldInfo).filter((column) => column in refreshedNewInfo);
  }

  private async addMissingSqlColumns(
    tableName: string,
    sourceInfo: Record<string, any>,
    columns: string[],
  ): Promise<void> {
    const knex = this.queryBuilderService.getKnex();
    await knex.schema.alterTable(tableName, (table: any) => {
      for (const column of columns) {
        table.specificType(
          column,
          this.getPortableSqlColumnType(sourceInfo[column]),
        );
      }
    });
    this.verbose(
      `  Added ${columns.length} legacy column(s) to ${tableName} before overlap merge`,
    );
  }

  private getPortableSqlColumnType(columnInfo: any): string {
    const type = String(columnInfo?.type || '').toLowerCase();
    const maxLength = Number(
      columnInfo?.maxLength || columnInfo?.characterMaximumLength || 0,
    );

    if (!type) return 'text';
    if (type.includes('bigint')) return 'bigint';
    if (type.includes('int')) return 'integer';
    if (type.includes('bool') || type === 'tinyint(1)') return 'boolean';
    if (type.includes('double')) return 'double precision';
    if (type.includes('float')) return 'float';
    if (type.includes('decimal') || type.includes('numeric')) return 'decimal';
    if (type.includes('jsonb')) return 'jsonb';
    if (type.includes('json')) return 'json';
    if (type.includes('timestamp')) return 'timestamp';
    if (type === 'date') return 'date';
    if (type.includes('time')) return 'time';
    if (type.includes('uuid')) return 'uuid';
    if (type.includes('text')) return 'text';
    if (type.includes('char'))
      return `varchar(${maxLength > 0 ? maxLength : 255})`;
    return 'text';
  }

  private getOverlapRowKey(
    rename: TableRenameDef,
    row: any,
    columns: string[],
    options: { remapCoreOwnerIds?: boolean } = {},
  ): string | null {
    const logicalKey = this.getCoreMetadataRowKey(rename, row, {
      remapOwnerIds: options.remapCoreOwnerIds,
    });
    if (logicalKey) return logicalKey;

    if ('id' in row && columns.includes('id') && row.id != null)
      return `id:${row.id}`;
    if ('_id' in row && columns.includes('_id') && row._id != null)
      return `_id:${row._id}`;

    if (rename.mergeKeys?.length) {
      const values = rename.mergeKeys.map((column) => row?.[column]);
      if (
        rename.mergeKeys.every((column) => columns.includes(column)) &&
        values.every((value) => value !== undefined && value !== null)
      ) {
        return `merge:${rename.mergeKeys
          .map((column, index) => `${column}:${String(values[index])}`)
          .join('|')}`;
      }
    }

    return null;
  }

  private projectRowToColumns(row: any, columns: string[]): any {
    return Object.fromEntries(
      columns
        .filter((column) => row[column] !== undefined)
        .map((column) => [column, row[column]]),
    );
  }

  private rowsConflict(left: any, right: any, columns: string[]): boolean {
    return columns.some((column) => {
      if (
        left?.[column] === undefined ||
        right?.[column] === undefined ||
        right?.[column] === null ||
        column === 'createdAt' ||
        column === 'updatedAt'
      ) {
        return false;
      }
      return JSON.stringify(left[column]) !== JSON.stringify(right[column]);
    });
  }

  private findRowByOverlapKey(
    rename: TableRenameDef,
    rows: any[],
    key: string,
    columns: string[],
  ): any | null {
    return (
      rows.find(
        (row) =>
          this.getOverlapRowKey(rename, row, columns, {
            remapCoreOwnerIds: false,
          }) === key,
      ) ?? null
    );
  }

  private getMissingRowValues(
    legacyRow: any,
    canonicalRow: any,
    columns: string[],
  ): Record<string, any> {
    return Object.fromEntries(
      columns
        .filter(
          (column) =>
            column !== 'id' &&
            column !== '_id' &&
            column !== 'createdAt' &&
            column !== 'updatedAt' &&
            legacyRow?.[column] !== undefined &&
            (canonicalRow?.[column] === undefined ||
              canonicalRow?.[column] === null),
        )
        .map((column) => [column, legacyRow[column]]),
    );
  }

  private getRowIdentityFilter(
    rename: TableRenameDef,
    row: any,
  ): Record<string, any> | null {
    if (row?.id !== undefined && row.id !== null) return { id: row.id };
    if (row?._id !== undefined && row._id !== null) return { _id: row._id };
    if (rename.mergeKeys?.length) {
      const entries = rename.mergeKeys
        .map((column) => [column, row?.[column]])
        .filter(([, value]) => value !== undefined && value !== null);
      if (entries.length === rename.mergeKeys.length) {
        return Object.fromEntries(entries);
      }
    }
    return null;
  }

  private projectCoreRowToColumns(
    rename: TableRenameDef,
    row: any,
    columns: string[],
  ): any {
    const projected = this.projectRowToColumns(row, columns);
    const tableName = rename.to || rename.from;
    if (
      (tableName === SYSTEM_TABLES.table || tableName === 'table_definition') &&
      typeof projected.name === 'string'
    ) {
      projected.name = this.normalizeCoreTableName(projected.name);
    }
    if (
      tableName === SYSTEM_TABLES.column ||
      tableName === 'column_definition'
    ) {
      if ('tableId' in projected)
        projected.tableId = this.remapCoreTableId(rename, projected.tableId);
      if ('table' in projected)
        projected.table = this.remapCoreTableId(rename, projected.table);
    }
    if (
      tableName === SYSTEM_TABLES.relation ||
      tableName === 'relation_definition'
    ) {
      if ('sourceTableId' in projected) {
        projected.sourceTableId = this.remapCoreTableId(
          rename,
          projected.sourceTableId,
        );
      }
      if ('targetTableId' in projected) {
        projected.targetTableId = this.remapCoreTableId(
          rename,
          projected.targetTableId,
        );
      }
      if ('sourceTable' in projected) {
        projected.sourceTable = this.remapCoreTableId(
          rename,
          projected.sourceTable,
        );
      }
      if ('targetTable' in projected) {
        projected.targetTable = this.remapCoreTableId(
          rename,
          projected.targetTable,
        );
      }
    }
    return projected;
  }

  private isCoreTableMetadataStore(rename: TableRenameDef): boolean {
    const tableName = rename.to || rename.from;
    return (
      tableName === SYSTEM_TABLES.table || tableName === 'table_definition'
    );
  }

  private isCoreRelationMetadataStore(rename: TableRenameDef): boolean {
    const tableName = rename.to || rename.from;
    return (
      tableName === SYSTEM_TABLES.relation ||
      tableName === 'relation_definition'
    );
  }

  private getRelationMappedByKey(
    rename: TableRenameDef,
    row: any,
  ): string | null {
    if (!this.isCoreRelationMetadataStore(rename)) return null;
    const mappedById = row?.mappedById ?? row?.mappedBy;
    return mappedById !== undefined && mappedById !== null
      ? `mappedBy:${String(mappedById)}`
      : null;
  }

  private trackCanonicalCoreTableId(rename: TableRenameDef, row: any): void {
    if (!this.isCoreTableMetadataStore(rename) || !row?.name) return;
    const id = row.id ?? row._id;
    if (id === undefined || id === null) return;
    const map = this.queryBuilderService.isMongoDb()
      ? this.mongoCoreTableIdRemap
      : this.sqlCoreTableIdRemap;
    map.set(String(id), id);
  }

  private trackExistingCoreRowRemap(
    rename: TableRenameDef,
    legacyRow: any,
    canonicalRows: any[],
  ): void {
    if (!this.isCoreTableMetadataStore(rename) || !legacyRow?.name) return;
    const legacyId = legacyRow.id ?? legacyRow._id;
    if (legacyId === undefined || legacyId === null) return;
    const normalizedName = this.normalizeCoreTableName(legacyRow.name);
    const canonicalRow = canonicalRows.find(
      (row) => row?.name === normalizedName,
    );
    const canonicalId = canonicalRow?.id ?? canonicalRow?._id;
    if (canonicalId === undefined || canonicalId === null) return;
    const map = this.queryBuilderService.isMongoDb()
      ? this.mongoCoreTableIdRemap
      : this.sqlCoreTableIdRemap;
    map.set(String(legacyId), canonicalId);
  }

  private sqlProjectedIdConflicts(
    projected: any,
    canonicalRows: any[],
  ): boolean {
    if (projected?.id === undefined || projected.id === null) return false;
    return canonicalRows.some((row) => row?.id === projected.id);
  }

  private mongoProjectedIdConflicts(
    projected: any,
    canonicalRows: any[],
  ): boolean {
    if (projected?._id === undefined || projected._id === null) return false;
    return canonicalRows.some(
      (row) => String(row?._id) === String(projected._id),
    );
  }

  private async trackInsertedSqlCoreRowRemap(
    rename: TableRenameDef,
    legacyRow: any,
    projected: any,
  ): Promise<void> {
    if (!this.isCoreTableMetadataStore(rename)) return;
    const legacyId = legacyRow?.id;
    if (legacyId === undefined || legacyId === null) return;
    let canonicalId = projected?.id;
    if (
      (canonicalId === undefined || canonicalId === null) &&
      projected?.name
    ) {
      const inserted = await this.queryBuilderService
        .getKnex()(rename.to)
        .where({ name: projected.name })
        .first();
      canonicalId = inserted?.id;
    }
    if (canonicalId === undefined || canonicalId === null) return;
    this.sqlCoreTableIdRemap.set(String(legacyId), canonicalId);
  }

  private async trackInsertedMongoCoreRowRemap(
    rename: TableRenameDef,
    legacyRow: any,
    projected: any,
  ): Promise<void> {
    if (!this.isCoreTableMetadataStore(rename)) return;
    const legacyId = legacyRow?._id;
    if (legacyId === undefined || legacyId === null) return;
    let canonicalId = projected?._id;
    if (
      (canonicalId === undefined || canonicalId === null) &&
      projected?.name
    ) {
      const inserted = await this.getMongoDb()!
        .collection(rename.to)
        .findOne({ name: projected.name });
      canonicalId = inserted?._id;
    }
    if (canonicalId === undefined || canonicalId === null) return;
    this.mongoCoreTableIdRemap.set(String(legacyId), canonicalId);
  }

  private async reconcileSqlCoreTableOverlap(
    rename: TableRenameDef,
  ): Promise<void> {
    const knex = this.queryBuilderService.getKnex();
    const columns = await this.getSqlMergedColumns(rename.from, rename.to);
    const [legacyRows, canonicalRows] = await Promise.all([
      knex(rename.from).select(columns),
      knex(rename.to).select(columns),
    ]);

    const canonicalKeys = new Set<string>();
    const canonicalMappedByKeys = new Set<string>();
    for (const row of canonicalRows) {
      this.trackCanonicalCoreTableId(rename, row);
      const key = this.getOverlapRowKey(rename, row, columns, {
        remapCoreOwnerIds: false,
      });
      if (key !== null && key !== undefined) canonicalKeys.add(key);
      const mappedByKey = this.getRelationMappedByKey(rename, row);
      if (mappedByKey) canonicalMappedByKeys.add(mappedByKey);
    }
    const occupiedIds = new Set(
      canonicalRows
        .map((row: any) => row?.id)
        .filter((id: any) => id !== undefined && id !== null)
        .map((id: any) => String(id)),
    );
    const rowsToInsert = legacyRows.filter((row: any) => {
      const key = this.getOverlapRowKey(rename, row, columns);
      if (key === null || key === undefined) return false;
      if (canonicalKeys.has(key)) {
        this.trackExistingCoreRowRemap(rename, row, canonicalRows);
        return false;
      }
      const mappedByKey = this.getRelationMappedByKey(rename, row);
      if (mappedByKey && canonicalMappedByKeys.has(mappedByKey)) {
        return false;
      }
      return true;
    });

    let insertedCount = 0;
    for (const row of rowsToInsert) {
      const projected = this.projectCoreRowToColumns(rename, row, columns);
      if (
        projected?.id !== undefined &&
        projected?.id !== null &&
        occupiedIds.has(String(projected.id))
      ) {
        delete projected.id;
      }
      await knex(rename.to).insert(projected);
      insertedCount += 1;
      await this.trackInsertedSqlCoreRowRemap(rename, row, projected);
      const mappedByKey = this.getRelationMappedByKey(rename, projected);
      if (mappedByKey) canonicalMappedByKeys.add(mappedByKey);
      const insertedId =
        projected?.id ??
        (projected?.name
          ? (await knex(rename.to).where({ name: projected.name }).first())?.id
          : undefined);
      if (insertedId !== undefined && insertedId !== null) {
        occupiedIds.add(String(insertedId));
      }
    }

    if (insertedCount > 0) {
      this.verbose(
        `  Copied ${insertedCount} missing core metadata row(s) from ${rename.from} to ${rename.to}`,
      );
    }
  }

  private async reconcileSqlTableOverlap(
    rename: TableRenameDef,
  ): Promise<void> {
    const knex = this.queryBuilderService.getKnex();
    const columns = await this.getSqlMergedColumns(rename.from, rename.to);
    const [legacyRows, canonicalRows] = await Promise.all([
      knex(rename.from).select(columns),
      knex(rename.to).select(columns),
    ]);
    const canonicalKeys = new Set<string>();
    const occupiedIds = new Set<string>();
    const canonicalMappedByKeys = new Set<string>();
    for (const row of canonicalRows) {
      const key = this.getOverlapRowKey(rename, row, columns, {
        remapCoreOwnerIds: false,
      });
      if (key) canonicalKeys.add(key);
      const mappedByKey = this.getRelationMappedByKey(rename, row);
      if (mappedByKey) canonicalMappedByKeys.add(mappedByKey);
      if (row?.id !== undefined && row.id !== null) {
        occupiedIds.add(String(row.id));
      }
    }

    let insertedCount = 0;
    let conflictCount = 0;
    let skippedCount = 0;
    let updatedCount = 0;
    for (const row of legacyRows) {
      const key = this.getOverlapRowKey(rename, row, columns);
      if (!key) {
        skippedCount += 1;
        continue;
      }
      if (canonicalKeys.has(key)) {
        const canonicalRow = this.findRowByOverlapKey(
          rename,
          canonicalRows,
          key,
          columns,
        );
        if (canonicalRow && this.rowsConflict(row, canonicalRow, columns)) {
          conflictCount += 1;
        }
        if (canonicalRow) {
          const missingValues = this.getMissingRowValues(
            row,
            canonicalRow,
            columns,
          );
          const filter = this.getRowIdentityFilter(rename, canonicalRow);
          if (filter && Object.keys(missingValues).length > 0) {
            await knex(rename.to).where(filter).update(missingValues);
            Object.assign(canonicalRow, missingValues);
            updatedCount += 1;
          }
        }
        continue;
      }
      const mappedByKey = this.getRelationMappedByKey(rename, row);
      if (mappedByKey && canonicalMappedByKeys.has(mappedByKey)) {
        conflictCount += 1;
        continue;
      }
      const projected = this.projectRowToColumns(row, columns);
      if (
        projected?.id !== undefined &&
        projected?.id !== null &&
        occupiedIds.has(String(projected.id))
      ) {
        delete projected.id;
      }
      await knex(rename.to).insert(projected);
      insertedCount += 1;
      canonicalKeys.add(key);
      const insertedMappedByKey = this.getRelationMappedByKey(
        rename,
        projected,
      );
      if (insertedMappedByKey) canonicalMappedByKeys.add(insertedMappedByKey);
      if (projected?.id !== undefined && projected.id !== null) {
        occupiedIds.add(String(projected.id));
      }
    }
    this.verbose(
      `  SQL table overlap reconciled for ${rename.from} → ${rename.to}: copied ${insertedCount}, updated ${updatedCount}, conflicts ${conflictCount}, skipped ${skippedCount}`,
    );
  }

  private async reconcileMongoCoreTableOverlap(
    rename: TableRenameDef,
  ): Promise<void> {
    const db = this.getMongoDb()!;
    const [legacyRows, canonicalRows] = await Promise.all([
      db.collection(rename.from).find({}).toArray(),
      db.collection(rename.to).find({}).toArray(),
    ]);

    const canonicalKeys = new Set<string>();
    const canonicalMappedByKeys = new Set<string>();
    for (const row of canonicalRows) {
      this.trackCanonicalCoreTableId(rename, row);
      const key = this.getCoreMetadataRowKey(rename, row, {
        remapOwnerIds: false,
      });
      if (key !== null && key !== undefined) canonicalKeys.add(key);
      const mappedByKey = this.getRelationMappedByKey(rename, row);
      if (mappedByKey) canonicalMappedByKeys.add(mappedByKey);
    }
    const rowsToInsert = legacyRows.filter((row) => {
      const key = this.getCoreMetadataRowKey(rename, row);
      if (key === null || key === undefined) return false;
      if (canonicalKeys.has(key)) {
        this.trackExistingCoreRowRemap(rename, row, canonicalRows);
        return false;
      }
      const mappedByKey = this.getRelationMappedByKey(rename, row);
      if (mappedByKey && canonicalMappedByKeys.has(mappedByKey)) {
        return false;
      }
      return true;
    });

    const projectedRows = rowsToInsert.map((row) => {
      const projected = this.projectCoreRowToColumns(
        rename,
        row,
        Object.keys(row),
      );
      if (this.mongoProjectedIdConflicts(projected, canonicalRows)) {
        delete projected._id;
      }
      return projected;
    });

    if (projectedRows.length > 0) {
      await db.collection(rename.to).insertMany(projectedRows);
      for (let index = 0; index < rowsToInsert.length; index += 1) {
        await this.trackInsertedMongoCoreRowRemap(
          rename,
          rowsToInsert[index],
          projectedRows[index],
        );
        const mappedByKey = this.getRelationMappedByKey(
          rename,
          projectedRows[index],
        );
        if (mappedByKey) canonicalMappedByKeys.add(mappedByKey);
      }
      this.verbose(
        `  Copied ${projectedRows.length} missing core metadata row(s) from ${rename.from} to ${rename.to}`,
      );
    }
  }

  private async reconcileMongoTableOverlap(
    rename: TableRenameDef,
  ): Promise<void> {
    const db = this.getMongoDb()!;
    const [legacyRows, canonicalRows] = await Promise.all([
      db.collection(rename.from).find({}).toArray(),
      db.collection(rename.to).find({}).toArray(),
    ]);
    const columns = [
      ...new Set([
        ...legacyRows.flatMap((row) => Object.keys(row)),
        ...canonicalRows.flatMap((row) => Object.keys(row)),
      ]),
    ];
    const canonicalKeys = new Set<string>();
    const occupiedIds = new Set<string>();
    const canonicalMappedByKeys = new Set<string>();
    for (const row of canonicalRows) {
      const key = this.getOverlapRowKey(rename, row, columns, {
        remapCoreOwnerIds: false,
      });
      if (key) canonicalKeys.add(key);
      const mappedByKey = this.getRelationMappedByKey(rename, row);
      if (mappedByKey) canonicalMappedByKeys.add(mappedByKey);
      if (row?._id !== undefined && row._id !== null) {
        occupiedIds.add(String(row._id));
      }
    }

    let conflictCount = 0;
    let skippedCount = 0;
    let updatedCount = 0;
    const rowsToInsert: any[] = [];
    for (const row of legacyRows) {
      const key = this.getOverlapRowKey(rename, row, columns);
      if (!key) {
        skippedCount += 1;
        continue;
      }
      if (canonicalKeys.has(key)) {
        const canonicalRow = this.findRowByOverlapKey(
          rename,
          canonicalRows,
          key,
          columns,
        );
        if (canonicalRow && this.rowsConflict(row, canonicalRow, columns)) {
          conflictCount += 1;
        }
        if (canonicalRow) {
          const missingValues = this.getMissingRowValues(
            row,
            canonicalRow,
            columns,
          );
          const filter = this.getRowIdentityFilter(rename, canonicalRow);
          if (filter && Object.keys(missingValues).length > 0) {
            await db
              .collection(rename.to)
              .updateOne(filter, { $set: missingValues });
            Object.assign(canonicalRow, missingValues);
            updatedCount += 1;
          }
        }
        continue;
      }
      const mappedByKey = this.getRelationMappedByKey(rename, row);
      if (mappedByKey && canonicalMappedByKeys.has(mappedByKey)) {
        conflictCount += 1;
        continue;
      }
      const projected = this.projectRowToColumns(row, columns);
      if (
        projected?._id !== undefined &&
        projected?._id !== null &&
        occupiedIds.has(String(projected._id))
      ) {
        delete projected._id;
      }
      rowsToInsert.push(projected);
      canonicalKeys.add(key);
      const projectedMappedByKey = this.getRelationMappedByKey(
        rename,
        projected,
      );
      if (projectedMappedByKey) canonicalMappedByKeys.add(projectedMappedByKey);
      if (projected?._id !== undefined && projected._id !== null) {
        occupiedIds.add(String(projected._id));
      }
    }
    if (rowsToInsert.length > 0) {
      await db.collection(rename.to).insertMany(rowsToInsert);
    }
    this.verbose(
      `  Mongo collection overlap reconciled for ${rename.from} → ${rename.to}: copied ${rowsToInsert.length}, updated ${updatedCount}, conflicts ${conflictCount}, skipped ${skippedCount}`,
    );
  }

  private async renameSqlTableMetadataRow(
    tableStore: string,
    rename: TableRenameDef,
    tableId?: any,
  ): Promise<void> {
    const knex = this.queryBuilderService.getKnex();
    if (!(await knex.schema.hasTable(tableStore))) return;
    const targetRow = await knex(tableStore).where({ name: rename.to }).first();
    if (targetRow) return;
    const query = tableId
      ? knex(tableStore).where({ id: tableId })
      : knex(tableStore).where({ name: rename.from });
    await query.update({ name: rename.to });
  }

  private async renameMongoTableMetadataRow(
    tableStore: string,
    rename: TableRenameDef,
    tableId?: any,
  ): Promise<void> {
    const db = this.getMongoDb()!;
    const targetRow = await db
      .collection(tableStore)
      .findOne({ name: rename.to });
    if (targetRow) return;

    const filter = tableId ? { _id: tableId } : { name: rename.from };
    await db.collection(tableStore).updateOne(filter, {
      $set: { name: rename.to, updatedAt: new Date() },
    });
  }

  private async updateSqlCanonicalRoutePath(
    rename: TableRenameDef,
    tableId?: any,
  ): Promise<void> {
    const routeTable = await this.detectSqlRouteTable();
    if (!routeTable) return;

    const knex = this.queryBuilderService.getKnex();
    const query = knex(routeTable).where({ path: `/${rename.from}` });
    if (tableId) query.andWhere({ mainTableId: tableId });
    await query.update({ path: `/${rename.to}` });
  }

  private async updateMongoCanonicalRoutePath(
    rename: TableRenameDef,
    tableId?: any,
  ): Promise<void> {
    const routeTable = await this.detectMongoRouteTable();
    if (!routeTable) return;

    const filter: any = { path: `/${rename.from}` };
    if (tableId) filter.mainTable = tableId;
    await this.getMongoDb()!
      .collection(routeTable)
      .updateMany(filter, {
        $set: { path: `/${rename.to}`, updatedAt: new Date() },
      });
  }

  private async detectSqlRouteTable(): Promise<string | null> {
    const knex = this.queryBuilderService.getKnex();
    if (await knex.schema.hasTable(SYSTEM_TABLES.route))
      return SYSTEM_TABLES.route;
    if (await knex.schema.hasTable('route_definition'))
      return 'route_definition';
    return null;
  }

  private async detectMongoRouteTable(): Promise<string | null> {
    if (await this.physicalMigration.mongoCollectionExists(SYSTEM_TABLES.route))
      return SYSTEM_TABLES.route;
    if (await this.physicalMigration.mongoCollectionExists('route_definition'))
      return 'route_definition';
    return null;
  }

  private async mongoCollectionExists(
    collectionName: string,
  ): Promise<boolean> {
    const matches = await this.getMongoDb()!
      .listCollections({ name: collectionName })
      .toArray();
    return matches.length > 0;
  }

  private async findTableId(
    tableName: string,
    isMongoDB: boolean,
  ): Promise<{ tableId: any; tableIdField: string } | null> {
    const coreNames = await this.systemCoreTableResolver.getNames();
    if (isMongoDB) {
      const db = this.getMongoDb()!;
      const table = await db
        .collection(coreNames.table)
        .findOne({ name: tableName });
      if (!table) return null;
      return { tableId: table._id, tableIdField: 'table' };
    }

    const knex = this.queryBuilderService.getKnex();
    const table = await knex(coreNames.table).where('name', tableName).first();
    if (!table) return null;
    return { tableId: table.id, tableIdField: 'tableId' };
  }

  private async dropTableMetadata(
    tableNames: string[],
    isMongoDB: boolean,
  ): Promise<void> {
    this.verbose(`Dropping metadata for ${tableNames.length} table(s)...`);

    for (const tableName of tableNames) {
      const found = await this.findTableId(tableName, isMongoDB);
      if (!found) continue;

      const { tableId } = found;
      const coreNames = await this.systemCoreTableResolver.getNames();

      if (isMongoDB) {
        const db = this.getMongoDb()!;
        const columns = await db
          .collection(coreNames.column)
          .find({ table: tableId })
          .toArray();
        const allRelations = await db
          .collection(coreNames.relation)
          .find({})
          .toArray();
        const touchingRelations = allRelations.filter(
          (relation: any) =>
            String(relation.sourceTable) === String(tableId) ||
            String(relation.targetTable) === String(tableId),
        );
        const owningIds = touchingRelations.map(
          (relation: any) => relation.mappedBy || relation._id,
        );
        const relationIds = allRelations
          .filter(
            (relation: any) =>
              touchingRelations.some(
                (touching: any) =>
                  String(touching._id) === String(relation._id),
              ) ||
              owningIds.some(
                (owningId: any) =>
                  String(owningId) === String(relation._id) ||
                  String(owningId) === String(relation.mappedBy),
              ),
          )
          .map((relation: any) => relation._id);
        const columnIds = columns.map((column: any) => column._id);

        if (
          columnIds.length > 0 &&
          (await this.physicalMigration.mongoCollectionExists(
            SYSTEM_TABLES.columnRule,
          ))
        ) {
          await db
            .collection(SYSTEM_TABLES.columnRule)
            .deleteMany({ column: { $in: columnIds } });
        }
        if (
          await this.physicalMigration.mongoCollectionExists(
            SYSTEM_TABLES.fieldPermission,
          )
        ) {
          if (columnIds.length > 0) {
            await db
              .collection(SYSTEM_TABLES.fieldPermission)
              .deleteMany({ column: { $in: columnIds } });
          }
          if (relationIds.length > 0) {
            await db
              .collection(SYSTEM_TABLES.fieldPermission)
              .deleteMany({ relation: { $in: relationIds } });
          }
        }
        if (
          await this.physicalMigration.mongoCollectionExists(
            SYSTEM_TABLES.route,
          )
        ) {
          await db
            .collection(SYSTEM_TABLES.route)
            .deleteMany({ mainTable: tableId });
        }
        if (
          await this.physicalMigration.mongoCollectionExists(
            SYSTEM_TABLES.graphql,
          )
        ) {
          await db
            .collection(SYSTEM_TABLES.graphql)
            .deleteMany({ table: tableId });
        }
        if (relationIds.length > 0) {
          await db
            .collection(coreNames.relation)
            .deleteMany({ _id: { $in: relationIds } });
        }
        await db.collection(coreNames.column).deleteMany({ table: tableId });
        await db.collection(coreNames.table).deleteOne({ _id: tableId });
      } else {
        const knex = this.queryBuilderService.getKnex();
        const columns = await knex(coreNames.column)
          .where('tableId', tableId)
          .select('id');
        const allRelations = await knex(coreNames.relation).select(
          'id',
          'sourceTableId',
          'targetTableId',
          'mappedById',
        );
        const touchingRelations = allRelations.filter(
          (relation: any) =>
            String(relation.sourceTableId) === String(tableId) ||
            String(relation.targetTableId) === String(tableId),
        );
        const owningIds = touchingRelations.map(
          (relation: any) => relation.mappedById || relation.id,
        );
        const relationIds = allRelations
          .filter(
            (relation: any) =>
              touchingRelations.some(
                (touching: any) => String(touching.id) === String(relation.id),
              ) ||
              owningIds.some(
                (owningId: any) =>
                  String(owningId) === String(relation.id) ||
                  String(owningId) === String(relation.mappedById),
              ),
          )
          .map((relation: any) => relation.id);
        const columnIds = columns.map((column: any) => column.id);

        if (
          columnIds.length > 0 &&
          (await knex.schema.hasTable(SYSTEM_TABLES.columnRule))
        ) {
          await knex(SYSTEM_TABLES.columnRule)
            .whereIn('columnId', columnIds)
            .delete();
        }
        if (await knex.schema.hasTable(SYSTEM_TABLES.fieldPermission)) {
          if (columnIds.length > 0) {
            await knex(SYSTEM_TABLES.fieldPermission)
              .whereIn('columnId', columnIds)
              .delete();
          }
          if (relationIds.length > 0) {
            await knex(SYSTEM_TABLES.fieldPermission)
              .whereIn('relationId', relationIds)
              .delete();
          }
        }
        if (await knex.schema.hasTable(SYSTEM_TABLES.route)) {
          await knex(SYSTEM_TABLES.route)
            .where('mainTableId', tableId)
            .delete();
        }
        if (await knex.schema.hasTable(SYSTEM_TABLES.graphql)) {
          await knex(SYSTEM_TABLES.graphql).where('tableId', tableId).delete();
        }
        if (relationIds.length > 0) {
          await knex(coreNames.relation).whereIn('id', relationIds).delete();
        }
        await knex(coreNames.column).where('tableId', tableId).delete();
        await knex(coreNames.table).where('id', tableId).delete();
      }

      this.verbose(`  Dropped metadata for table: ${tableName}`);
    }
  }

  private async migrateTableMetadata(
    migration: TableMigrationDef,
    isMongoDB: boolean,
  ): Promise<void> {
    const tableName = migration._unique.name._eq;
    this.verbose(`Migrating metadata for table: ${tableName}`);

    const found = await this.findTableId(tableName, isMongoDB);
    if (!found) {
      this.logger.warn(`  Table ${tableName} not found in metadata, skipping`);
      return;
    }

    const { tableId, tableIdField } = found;

    if (
      migration.tableToModify &&
      hasTableMetadataChanges(migration.tableToModify)
    ) {
      await this.modifyTableMetadata(
        tableId,
        isMongoDB,
        migration.tableToModify,
      );
    }

    const columnsToModify = migration.columnsToModify ?? [];
    const columnsToRemove = migration.columnsToRemove ?? [];
    const relationsToModify = migration.relationsToModify ?? [];
    const relationsToRemove = migration.relationsToRemove ?? [];

    if (columnsToModify.length > 0) {
      await this.modifyColumnMetadata(
        tableName,
        tableId,
        tableIdField,
        columnsToModify,
        isMongoDB,
      );
    }

    if (columnsToRemove.length > 0) {
      await this.removeColumnMetadata(
        tableName,
        tableId,
        tableIdField,
        columnsToRemove,
        isMongoDB,
      );
    }

    if (relationsToModify.length > 0) {
      await this.modifyRelationMetadata(tableId, isMongoDB, relationsToModify);
    }

    if (relationsToRemove.length > 0) {
      await this.removeRelationMetadata(tableId, isMongoDB, relationsToRemove);
    }
  }

  private async modifyTableMetadata(
    tableId: any,
    isMongoDB: boolean,
    modification: NonNullable<TableMigrationDef['tableToModify']>,
  ): Promise<void> {
    const coreNames = await this.systemCoreTableResolver.getNames();
    const updateData = buildTableMetadataUpdate(modification);
    if (Object.keys(updateData).length === 0) return;

    if (isMongoDB) {
      updateData.updatedAt = new Date();
      await this.getMongoDb()!
        .collection(coreNames.table)
        .updateOne({ _id: tableId }, { $set: updateData });
      return;
    }

    for (const field of ['uniques', 'indexes', 'metadata']) {
      if (updateData[field] !== undefined) {
        updateData[field] = JSON.stringify(updateData[field] ?? null);
      }
    }
    await this.queryBuilderService
      .getKnex()(coreNames.table)
      .where('id', tableId)
      .update(updateData);
  }

  private async modifyColumnMetadata(
    tableName: string,
    tableId: any,
    tableIdField: string,
    modifications: ColumnModifyDef[],
    isMongoDB: boolean,
  ): Promise<void> {
    for (const mod of modifications) {
      if (!hasColumnMetadataChanges(mod)) {
        continue;
      }

      const oldName = mod.from.name;
      const coreNames = await this.systemCoreTableResolver.getNames();
      let columnId: any;
      let targetColumnId: any;

      if (isMongoDB) {
        const db = this.getMongoDb()!;
        const column = await db.collection(coreNames.column).findOne({
          table: tableId,
          name: oldName,
        });
        const targetColumn = await db.collection(coreNames.column).findOne({
          table: tableId,
          name: mod.to.name,
        });
        columnId = column?._id;
        targetColumnId = targetColumn?._id;

        if (mod.to.name !== mod.from.name) {
          await this.physicalMigration.renameMongoDocumentFieldIfNeeded(
            tableName,
            mod.from.name,
            mod.to.name,
          );
        }
      } else {
        const knex = this.queryBuilderService.getKnex();
        const column = await knex(coreNames.column)
          .where(tableIdField, tableId)
          .where('name', oldName)
          .first();
        const targetColumn = await knex(coreNames.column)
          .where(tableIdField, tableId)
          .where('name', mod.to.name)
          .first();
        columnId = column?.id;
        targetColumnId = targetColumn?.id;
      }

      if (!columnId && !targetColumnId) continue;

      const updateData = buildColumnMetadataUpdate(mod);

      if (mod.to.name !== mod.from.name && !isMongoDB) {
        await this.physicalMigration.renameSqlPhysicalColumnIfNeeded(
          tableName,
          mod.from.name,
          mod.to.name,
        );
      }

      if (Object.keys(updateData).length > 0) {
        if (isMongoDB) {
          const db = this.getMongoDb()!;
          updateData.updatedAt = new Date();
          await db.collection(coreNames.column).updateOne(
            { _id: targetColumnId ?? columnId },
            {
              $set: targetColumnId
                ? { ...updateData, name: mod.to.name }
                : updateData,
            },
          );
        } else {
          const knex = this.queryBuilderService.getKnex();
          if (updateData.defaultValue !== undefined) {
            updateData.defaultValue = JSON.stringify(
              updateData.defaultValue ?? null,
            );
          }
          if (updateData.options !== undefined) {
            updateData.options = JSON.stringify(updateData.options ?? null);
          }
          await knex(coreNames.column)
            .where('id', targetColumnId ?? columnId)
            .update(updateData);
        }
        this.verbose(`  Modified column metadata: ${oldName} → ${mod.to.name}`);
      }

      if (targetColumnId && columnId && targetColumnId !== columnId) {
        if (isMongoDB) {
          const db = this.getMongoDb()!;
          await db.collection(coreNames.column).deleteOne({ _id: columnId });
        } else {
          const knex = this.queryBuilderService.getKnex();
          await knex(coreNames.column).where('id', columnId).delete();
        }
        this.verbose(`  Removed duplicate old column metadata: ${oldName}`);
      }
    }
  }

  private async removeColumnMetadata(
    tableName: string,
    tableId: any,
    tableIdField: string,
    columns: string[],
    isMongoDB: boolean,
  ): Promise<void> {
    for (const colName of columns) {
      const coreNames = await this.systemCoreTableResolver.getNames();
      await this.copyLegacyScriptColumnBeforeRemove(
        tableName,
        colName,
        isMongoDB,
      );

      if (isMongoDB) {
        const db = this.getMongoDb()!;
        const column = await db.collection(coreNames.column).findOne({
          table: tableId,
          name: colName,
        });
        if (column) {
          if (
            await this.physicalMigration.mongoCollectionExists(
              SYSTEM_TABLES.columnRule,
            )
          ) {
            await db
              .collection(SYSTEM_TABLES.columnRule)
              .deleteMany({ column: column._id });
          }
          if (
            await this.physicalMigration.mongoCollectionExists(
              SYSTEM_TABLES.fieldPermission,
            )
          ) {
            await db
              .collection(SYSTEM_TABLES.fieldPermission)
              .deleteMany({ column: column._id });
          }
          await db.collection(coreNames.column).deleteOne({ _id: column._id });
          this.verbose(`  Removed column metadata: ${colName}`);
        }
      } else {
        const knex = this.queryBuilderService.getKnex();
        const column = await knex(coreNames.column)
          .where(tableIdField, tableId)
          .where('name', colName)
          .first();
        if (column) {
          if (await knex.schema.hasTable(SYSTEM_TABLES.columnRule)) {
            await knex(SYSTEM_TABLES.columnRule)
              .where('columnId', column.id)
              .delete();
          }
          if (await knex.schema.hasTable(SYSTEM_TABLES.fieldPermission)) {
            await knex(SYSTEM_TABLES.fieldPermission)
              .where('columnId', column.id)
              .delete();
          }
          await knex(coreNames.column).where('id', column.id).delete();
          this.verbose(`  Removed column metadata: ${colName}`);
        }
      }

      if (!isMongoDB || !(await this.isMongoRelationField(tableId, colName))) {
        await this.physicalMigration.dropPhysicalColumn(
          tableName,
          colName,
          isMongoDB,
        );
      }
    }
  }

  private async isMongoRelationField(
    tableId: any,
    propertyName: string,
  ): Promise<boolean> {
    const db = this.getMongoDb();
    if (!db) return false;
    const coreNames = await this.systemCoreTableResolver.getNames();

    const relation = await db.collection(coreNames.relation).findOne({
      sourceTable: tableId,
      propertyName,
    });
    return !!relation;
  }

  private getLegacyScriptTargetColumn(
    tableName: string,
    colName: string,
  ): string | null {
    return getLegacyScriptTargetColumn(tableName, colName);
  }

  private async copyLegacyScriptColumnBeforeRemove(
    tableName: string,
    colName: string,
    isMongoDB: boolean,
  ): Promise<void> {
    const targetColumn = this.getLegacyScriptTargetColumn(tableName, colName);
    if (!targetColumn) return;

    if (isMongoDB) {
      const db = this.getMongoDb()!;
      await db.collection(tableName).updateMany(
        {
          [colName]: { $exists: true, $ne: null },
          $or: [
            { [targetColumn]: { $exists: false } },
            { [targetColumn]: null },
            { [targetColumn]: '' },
          ],
        },
        [
          {
            $set: {
              [targetColumn]: `$${colName}`,
            },
          },
        ] as any,
      );
      return;
    }

    const knex = this.queryBuilderService.getKnex();
    const [hasSource, hasTarget] = await Promise.all([
      knex.schema.hasColumn(tableName, colName),
      knex.schema.hasColumn(tableName, targetColumn),
    ]);
    if (!hasSource || !hasTarget) return;

    await knex(tableName)
      .whereNotNull(colName)
      .where((qb: any) => {
        qb.whereNull(targetColumn).orWhere(targetColumn, '');
      })
      .update({
        [targetColumn]: knex.ref(colName),
      });
  }

  private async dropPhysicalColumn(
    tableName: string,
    colName: string,
    isMongoDB: boolean,
  ): Promise<void> {
    if (isMongoDB) {
      const db = this.getMongoDb()!;
      await db
        .collection(tableName)
        .updateMany(
          { [colName]: { $exists: true } },
          { $unset: { [colName]: '' } },
        );
      return;
    }

    const knex = this.queryBuilderService.getKnex();
    const hasColumn = await knex.schema.hasColumn(tableName, colName);
    if (!hasColumn) return;
    await knex.schema.alterTable(tableName, (table: any) => {
      table.dropColumn(colName);
    });
    this.verbose(`  Dropped physical column: ${tableName}.${colName}`);
  }

  private async modifyRelationMetadata(
    tableId: any,
    isMongoDB: boolean,
    modifications: RelationModifyDef[],
  ): Promise<void> {
    const sourceTableField = isMongoDB ? 'sourceTable' : 'sourceTableId';

    for (const mod of modifications) {
      if (!hasRelationMetadataChanges(mod)) {
        continue;
      }

      const oldName = mod.from.propertyName;
      const coreNames = await this.systemCoreTableResolver.getNames();
      let relation: any;

      if (isMongoDB) {
        const db = this.getMongoDb()!;
        relation = await db.collection(coreNames.relation).findOne({
          sourceTable: tableId,
          propertyName: oldName,
        });
      } else {
        const knex = this.queryBuilderService.getKnex();
        relation = await knex(coreNames.relation)
          .where(sourceTableField, tableId)
          .where('propertyName', oldName)
          .first();
      }

      if (!relation) {
        continue;
      }

      const relationId = isMongoDB ? relation._id : relation.id;
      const updateData = buildRelationMetadataUpdate(mod);
      if (
        mod.to.targetTable !== undefined &&
        mod.to.targetTable !== mod.from.targetTable
      ) {
        const targetTable = await this.findTableId(
          mod.to.targetTable,
          isMongoDB,
        );
        if (!targetTable) {
          throw new Error(
            `Target table ${mod.to.targetTable} was not found for relation ${oldName}`,
          );
        }
        updateData[isMongoDB ? 'targetTable' : 'targetTableId'] =
          targetTable.tableId;
      }
      if (
        mod.to.mappedBy !== undefined &&
        mod.to.mappedBy !== mod.from.mappedBy
      ) {
        if (mod.to.mappedBy && isMongoDB) {
          const db = this.getMongoDb()!;
          const targetTableId = relation.targetTable;
          const owningRel = await db.collection(coreNames.relation).findOne({
            sourceTable: targetTableId,
            propertyName: mod.to.mappedBy,
          });
          updateData.mappedBy = owningRel?._id || null;
        } else if (mod.to.mappedBy && !isMongoDB) {
          const knex = this.queryBuilderService.getKnex();
          const targetTableId = relation.targetTableId;
          const owningRel = await knex(coreNames.relation)
            .where('sourceTableId', targetTableId)
            .where('propertyName', mod.to.mappedBy)
            .first();
          updateData.mappedById = owningRel?.id || null;
        } else {
          const mappedByField = isMongoDB ? 'mappedBy' : 'mappedById';
          updateData[mappedByField] = null;
        }
      }
      if (Object.keys(updateData).length > 0) {
        if (isMongoDB) {
          const db = this.getMongoDb()!;
          updateData.updatedAt = new Date();
          await db
            .collection(coreNames.relation)
            .updateOne({ _id: relationId }, { $set: updateData });
        } else {
          const knex = this.queryBuilderService.getKnex();
          await knex(coreNames.relation)
            .where('id', relationId)
            .update(updateData);
        }
        this.verbose(
          `  Modified relation metadata: ${oldName} → ${mod.to.propertyName}`,
        );
      }

      await this.updateInverseRelationMetadata(
        relation,
        relationId,
        mod,
        isMongoDB,
      );
    }
  }

  private async updateInverseRelationMetadata(
    relation: any,
    relationId: any,
    mod: RelationModifyDef,
    isMongoDB: boolean,
  ): Promise<void> {
    if (
      mod.to.inversePropertyName === undefined ||
      mod.to.inversePropertyName === mod.from.inversePropertyName
    ) {
      return;
    }

    const coreNames = await this.systemCoreTableResolver.getNames();
    if (isMongoDB) {
      const db = this.getMongoDb()!;
      const counterpart = relation.mappedBy
        ? await db
            .collection(coreNames.relation)
            .findOne({ _id: relation.mappedBy })
        : await db
            .collection(coreNames.relation)
            .findOne({ mappedBy: relationId });
      if (!counterpart) return;

      if (mod.to.inversePropertyName) {
        await db.collection(coreNames.relation).updateOne(
          { _id: counterpart._id },
          {
            $set: {
              propertyName: mod.to.inversePropertyName,
              updatedAt: new Date(),
            },
          },
        );
        return;
      }

      if (
        await this.physicalMigration.mongoCollectionExists(
          SYSTEM_TABLES.fieldPermission,
        )
      ) {
        await db
          .collection(SYSTEM_TABLES.fieldPermission)
          .deleteMany({ relation: counterpart._id });
      }
      await db
        .collection(coreNames.relation)
        .deleteOne({ _id: counterpart._id });
      return;
    }

    const knex = this.queryBuilderService.getKnex();
    const counterpart = relation.mappedById
      ? await knex(coreNames.relation).where('id', relation.mappedById).first()
      : await knex(coreNames.relation).where('mappedById', relationId).first();
    if (!counterpart) return;

    if (mod.to.inversePropertyName) {
      await knex(coreNames.relation)
        .where('id', counterpart.id)
        .update({ propertyName: mod.to.inversePropertyName });
      return;
    }

    if (await knex.schema.hasTable(SYSTEM_TABLES.fieldPermission)) {
      await knex(SYSTEM_TABLES.fieldPermission)
        .where('relationId', counterpart.id)
        .delete();
    }
    await knex(coreNames.relation).where('id', counterpart.id).delete();
  }

  private async removeRelationMetadata(
    tableId: any,
    isMongoDB: boolean,
    relations: string[],
  ): Promise<void> {
    const sourceTableField = isMongoDB ? 'sourceTable' : 'sourceTableId';

    for (const relName of relations) {
      const coreNames = await this.systemCoreTableResolver.getNames();
      if (isMongoDB) {
        const db = this.getMongoDb()!;
        const relation = await db.collection(coreNames.relation).findOne({
          sourceTable: tableId,
          propertyName: relName,
        });
        if (!relation) continue;

        const owningId = relation.mappedBy || relation._id;
        const relationRows = (
          await db.collection(coreNames.relation).find({}).toArray()
        ).filter(
          (row: any) =>
            String(row._id) === String(owningId) ||
            String(row.mappedBy) === String(owningId),
        );
        const relationIds = relationRows.map((row: any) => row._id);
        if (
          !relationIds.some((id: any) => String(id) === String(relation._id))
        ) {
          relationIds.push(relation._id);
        }

        if (
          await this.physicalMigration.mongoCollectionExists(
            SYSTEM_TABLES.fieldPermission,
          )
        ) {
          await db
            .collection(SYSTEM_TABLES.fieldPermission)
            .deleteMany({ relation: { $in: relationIds } });
        }
        await db
          .collection(coreNames.relation)
          .deleteMany({ _id: { $in: relationIds } });
        this.verbose(`  Removed relation metadata pair: ${relName}`);
        continue;
      }

      const knex = this.queryBuilderService.getKnex();
      const relation = await knex(coreNames.relation)
        .where(sourceTableField, tableId)
        .where('propertyName', relName)
        .first();
      if (!relation) continue;

      const owningId = relation.mappedById || relation.id;
      const relationRows = (
        await knex(coreNames.relation).select('id', 'mappedById')
      ).filter(
        (row: any) =>
          String(row.id) === String(owningId) ||
          String(row.mappedById) === String(owningId),
      );
      const relationIds = relationRows.map((row: any) => row.id);
      if (!relationIds.includes(relation.id)) relationIds.push(relation.id);

      if (await knex.schema.hasTable(SYSTEM_TABLES.fieldPermission)) {
        await knex(SYSTEM_TABLES.fieldPermission)
          .whereIn('relationId', relationIds)
          .delete();
      }
      await knex(coreNames.relation).whereIn('id', relationIds).delete();
      this.verbose(`  Removed relation metadata pair: ${relName}`);
    }
  }

  private verbose(message: string): void {
    bootstrapVerboseLog(this.logger, message);
  }
}
