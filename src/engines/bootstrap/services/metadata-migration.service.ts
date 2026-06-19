import { DatabaseConfigService } from '../../../shared/services';
import { Logger } from '../../../shared/logger';
import { QueryBuilderService } from '@enfyra/kernel';
import { Db } from 'mongodb';
import { getErrorMessage } from '../../../shared/utils/error.util';
import {
  SchemaMigrationDef,
  TableMigrationDef,
  ColumnModifyDef,
  RelationModifyDef,
  TableRenameDef,
} from '../../../shared/types/schema-migration.types';
import { bootstrapVerboseLog } from '../utils/bootstrap-logging.util';
import { SystemCoreTableResolver } from './system-core-table-resolver.service';
import {
  buildColumnMetadataUpdate,
  getLegacyScriptTargetColumn,
  getValidTableRenames,
  hasColumnMetadataChanges,
  hasRelationMetadataChanges,
  hasSchemaMigrations,
  loadSnapshotMigrationFile,
} from '../utils/metadata-migration.util';
import { SYSTEM_TABLES } from '../../../shared/utils/system-tables.constants';
import { MetadataPhysicalMigrationHelper } from '../utils/metadata-physical-migration.util';

export class MetadataMigrationService {
  private readonly logger = new Logger(MetadataMigrationService.name);
  private readonly queryBuilderService: QueryBuilderService;
  private readonly systemCoreTableResolver: SystemCoreTableResolver;
  private readonly physicalMigration: MetadataPhysicalMigrationHelper;
  private migrations: SchemaMigrationDef | null = null;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    systemCoreTableResolver: SystemCoreTableResolver;
  }) {
    this.queryBuilderService = deps.queryBuilderService;
    this.systemCoreTableResolver = deps.systemCoreTableResolver;
    this.physicalMigration = new MetadataPhysicalMigrationHelper({
      queryBuilderService: this.queryBuilderService,
      verbose: (message) => this.verbose(message),
    });
    this.loadMigrations();
  }

  private loadMigrations(): void {
    try {
      const migrations = loadSnapshotMigrationFile();
      if (migrations) {
        this.migrations = migrations;
        this.verbose(
          `Loaded snapshot-migration.json with ${migrations.tables?.length || 0} table migration(s)`,
        );
      }
    } catch (error) {
      this.logger.warn(
        `Failed to load snapshot-migration.json: ${getErrorMessage(error)}`,
      );
      this.migrations = null;
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

    this.verbose('Running metadata migrations from snapshot-migration.json...');

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

    await this.physicalMigration.runPhysicalTableRenames(
      this.migrations.physicalTablesToRename ?? [],
      this.queryBuilderService.isMongoDb(),
    );

    await this.physicalMigration.dropPhysicalTables(
      this.migrations.physicalTablesToDrop ?? [],
      this.queryBuilderService.isMongoDb(),
    );
  }

  async runPhysicalMigrationsBeforeMetadataSync(): Promise<void> {
    if (!this.hasMigrations()) return;

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
        throw new Error(
          `Cannot rename core system table ${rename.from} to ${rename.to}: both physical tables exist`,
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
        throw new Error(
          `Cannot rename core system collection ${rename.from} to ${rename.to}: both collections exist`,
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
      await db
        .collection(SYSTEM_TABLES.table)
        .updateOne(
          { name: rename.from },
          { $set: { name: rename.to, updatedAt: new Date() } },
        );
      await this.updateMongoCanonicalRoutePath(rename);
    }
  }

  private async renameSqlTable(rename: TableRenameDef): Promise<void> {
    const knex = this.queryBuilderService.getKnex();
    const oldExists = await knex.schema.hasTable(rename.from);
    const newExists = await knex.schema.hasTable(rename.to);

    if (oldExists && newExists) {
      throw new Error(
        `Cannot rename system table ${rename.from} to ${rename.to}: both physical tables exist`,
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
      throw new Error(
        `Cannot rename system collection ${rename.from} to ${rename.to}: both collections exist`,
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
    await db
      .collection(tableStoreAfter)
      .updateOne(
        tableRecord?._id ? { _id: tableRecord._id } : { name: rename.from },
        { $set: { name: rename.to, updatedAt: new Date() } },
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

  private async renameSqlTableMetadataRow(
    tableStore: string,
    rename: TableRenameDef,
    tableId?: any,
  ): Promise<void> {
    const knex = this.queryBuilderService.getKnex();
    if (!(await knex.schema.hasTable(tableStore))) return;
    const query = tableId
      ? knex(tableStore).where({ id: tableId })
      : knex(tableStore).where({ name: rename.from });
    await query.update({ name: rename.to });
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
      try {
        const found = await this.findTableId(tableName, isMongoDB);
        if (!found) continue;

        const { tableId } = found;
        const coreNames = await this.systemCoreTableResolver.getNames();

        if (isMongoDB) {
          const db = this.getMongoDb()!;
          await db
            .collection(coreNames.relation)
            .deleteMany({ sourceTable: tableId });
          await db.collection(coreNames.column).deleteMany({ table: tableId });
          await db.collection(coreNames.table).deleteOne({ _id: tableId });
        } else {
          const knex = this.queryBuilderService.getKnex();
          await knex(coreNames.relation)
            .where('sourceTableId', tableId)
            .delete();
          await knex(coreNames.column).where('tableId', tableId).delete();
          await knex(coreNames.table).where('id', tableId).delete();
        }

        this.verbose(`  Dropped metadata for table: ${tableName}`);
      } catch (error) {
        this.logger.error(
          `  Failed to drop metadata for ${tableName}: ${getErrorMessage(error)}`,
        );
      }
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

      try {
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
            await knex(coreNames.column)
              .where('id', targetColumnId ?? columnId)
              .update(updateData);
          }
          this.verbose(
            `  Modified column metadata: ${oldName} → ${mod.to.name}`,
          );
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
      } catch (err) {
        this.logger.warn(
          `  Failed to modify column metadata: ${(err as Error).message}`,
        );
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
      try {
        const coreNames = await this.systemCoreTableResolver.getNames();
        await this.copyLegacyScriptColumnBeforeRemove(
          tableName,
          colName,
          isMongoDB,
        );

        if (isMongoDB) {
          const db = this.getMongoDb()!;
          const result = await db
            .collection(coreNames.column)
            .deleteOne({ table: tableId, name: colName });
          if (result.deletedCount > 0) {
            this.verbose(`  Removed column metadata: ${colName}`);
          }
        } else {
          const knex = this.queryBuilderService.getKnex();
          const column = await knex(coreNames.column)
            .where(tableIdField, tableId)
            .where('name', colName)
            .first();
          if (column) {
            await knex(coreNames.column).where('id', column.id).delete();
            this.verbose(`  Removed column metadata: ${colName}`);
          }
        }

        if (
          !isMongoDB ||
          !(await this.isMongoRelationField(tableId, colName))
        ) {
          await this.physicalMigration.dropPhysicalColumn(
            tableName,
            colName,
            isMongoDB,
          );
        }
      } catch (err) {
        this.logger.warn(
          `  Failed to remove column ${colName}: ${(err as Error).message}`,
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

      try {
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

        const relationId = DatabaseConfigService.getRecordId(relation);
        const updateData: any = {};

        if (mod.to.propertyName !== mod.from.propertyName) {
          updateData.propertyName = mod.to.propertyName;
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
        if (mod.to.onDelete !== undefined) {
          updateData.onDelete = mod.to.onDelete;
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
      } catch (err) {
        this.logger.warn(
          `  Failed to modify relation metadata: ${(err as Error).message}`,
        );
      }
    }
  }

  private async removeRelationMetadata(
    tableId: any,
    isMongoDB: boolean,
    relations: string[],
  ): Promise<void> {
    const sourceTableField = isMongoDB ? 'sourceTable' : 'sourceTableId';

    for (const relName of relations) {
      try {
        const coreNames = await this.systemCoreTableResolver.getNames();
        if (isMongoDB) {
          const db = this.getMongoDb()!;
          const result = await db
            .collection(coreNames.relation)
            .deleteOne({ sourceTable: tableId, propertyName: relName });
          if (result.deletedCount > 0) {
            this.verbose(`  Removed relation metadata: ${relName}`);
          }
        } else {
          const knex = this.queryBuilderService.getKnex();
          const relation = await knex(coreNames.relation)
            .where(sourceTableField, tableId)
            .where('propertyName', relName)
            .first();
          if (relation) {
            await knex(coreNames.relation).where('id', relation.id).delete();
            this.verbose(`  Removed relation metadata: ${relName}`);
          }
        }
      } catch (err) {
        this.logger.warn(
          `  Failed to remove relation ${relName}: ${(err as Error).message}`,
        );
      }
    }
  }

  private verbose(message: string): void {
    bootstrapVerboseLog(this.logger, message);
  }
}
