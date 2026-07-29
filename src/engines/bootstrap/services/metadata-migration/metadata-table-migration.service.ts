import { QueryBuilderService } from '@enfyra/kernel';
import type { Db } from 'mongodb';
import { Logger } from '../../../../shared/logger';
import type { TableMigrationDef } from '../../../../shared/types/schema-migration.types';
import { SYSTEM_TABLES } from '../../../../shared/utils/system-tables.constants';
import {
  buildTableMetadataUpdate,
  hasTableMetadataChanges,
} from '../../utils/metadata-migration.util';
import { MetadataPhysicalMigrationHelper } from '../../utils/metadata-physical-migration.util';
import { SystemCoreTableResolver } from '../system-core-table-resolver.service';
import { MetadataColumnMigrationService } from './metadata-column-migration.service';
import { MetadataRelationMigrationService } from './metadata-relation-migration.service';
import { MetadataTableLocator } from './metadata-table-locator.service';

export class MetadataTableMigrationService {
  private readonly logger = new Logger(MetadataTableMigrationService.name);
  private readonly queryBuilderService: QueryBuilderService;
  private readonly systemCoreTableResolver: SystemCoreTableResolver;
  private readonly physicalMigration: MetadataPhysicalMigrationHelper;
  private readonly tableLocator: MetadataTableLocator;
  private readonly columnMigration: MetadataColumnMigrationService;
  private readonly relationMigration: MetadataRelationMigrationService;
  private readonly verbose: (message: string) => void;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    systemCoreTableResolver: SystemCoreTableResolver;
    physicalMigration: MetadataPhysicalMigrationHelper;
    verbose: (message: string) => void;
  }) {
    this.queryBuilderService = deps.queryBuilderService;
    this.systemCoreTableResolver = deps.systemCoreTableResolver;
    this.physicalMigration = deps.physicalMigration;
    this.tableLocator = new MetadataTableLocator(deps);
    this.columnMigration = new MetadataColumnMigrationService(deps);
    this.relationMigration = new MetadataRelationMigrationService(deps);
    this.verbose = deps.verbose;
  }

  private getMongoDb(): Db {
    return this.queryBuilderService.getMongoDb();
  }

  async dropTableMetadata(
    tableNames: string[],
    isMongoDB: boolean,
  ): Promise<void> {
    this.verbose(`Dropping metadata for ${tableNames.length} table(s)...`);

    for (const tableName of tableNames) {
      const found = await this.tableLocator.findTable(tableName, isMongoDB);
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

  async migrateTableMetadata(
    migration: TableMigrationDef,
    isMongoDB: boolean,
  ): Promise<void> {
    const tableName = migration._unique.name._eq;
    this.verbose(`Migrating metadata for table: ${tableName}`);

    const found = await this.tableLocator.findTable(tableName, isMongoDB);
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
      await this.columnMigration.modifyColumnMetadata(
        tableName,
        tableId,
        tableIdField,
        columnsToModify,
        isMongoDB,
      );
    }

    if (columnsToRemove.length > 0) {
      await this.columnMigration.removeColumnMetadata(
        tableName,
        tableId,
        tableIdField,
        columnsToRemove,
        isMongoDB,
      );
    }

    if (relationsToModify.length > 0) {
      await this.relationMigration.modifyRelationMetadata(
        tableId,
        isMongoDB,
        relationsToModify,
      );
    }

    if (relationsToRemove.length > 0) {
      await this.relationMigration.removeRelationMetadata(
        tableId,
        isMongoDB,
        relationsToRemove,
      );
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
}
