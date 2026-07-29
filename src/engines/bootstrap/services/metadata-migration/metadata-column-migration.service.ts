import { QueryBuilderService } from '@enfyra/kernel';
import type { Db } from 'mongodb';
import type { ColumnModifyDef } from '../../../../shared/types/schema-migration.types';
import { SYSTEM_TABLES } from '../../../../shared/utils/system-tables.constants';
import {
  buildColumnMetadataUpdate,
  getLegacyScriptTargetColumn,
  hasColumnMetadataChanges,
} from '../../utils/metadata-migration.util';
import { MetadataPhysicalMigrationHelper } from '../../utils/metadata-physical-migration.util';
import { SystemCoreTableResolver } from '../system-core-table-resolver.service';

export class MetadataColumnMigrationService {
  private readonly queryBuilderService: QueryBuilderService;
  private readonly systemCoreTableResolver: SystemCoreTableResolver;
  private readonly physicalMigration: MetadataPhysicalMigrationHelper;
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
    this.verbose = deps.verbose;
  }

  private getMongoDb(): Db {
    return this.queryBuilderService.getMongoDb();
  }

  async modifyColumnMetadata(
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

  async removeColumnMetadata(
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
}
