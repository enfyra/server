import { DatabaseConfigService } from '../../../shared/services';
import { Logger } from '../../../shared/logger';
import { QueryBuilderService } from '../../../kernel/query';
import { Db } from 'mongodb';
import { getErrorMessage } from '../../../shared/utils/error.util';
import {
  SchemaMigrationDef,
  TableMigrationDef,
  ColumnModifyDef,
  RelationModifyDef,
} from '../../../shared/types/schema-migration.types';
import * as fs from 'fs';
import * as path from 'path';

export class MetadataMigrationService {
  private readonly logger = new Logger(MetadataMigrationService.name);
  private readonly queryBuilderService: QueryBuilderService;
  private migrations: SchemaMigrationDef | null = null;

  constructor(deps: { queryBuilderService: QueryBuilderService }) {
    this.queryBuilderService = deps.queryBuilderService;
    this.loadMigrations();
  }

  private loadMigrations(): void {
    try {
      const filePath = path.join(process.cwd(), 'data/snapshot-migration.json');
      if (fs.existsSync(filePath)) {
        const content = fs.readFileSync(filePath, 'utf8');
        const parsed = JSON.parse(content);
        if (
          parsed &&
          (parsed.tables?.length > 0 || parsed.tablesToDrop?.length > 0)
        ) {
          this.migrations = parsed;
          this.logger.log(
            `Loaded snapshot-migration.json with ${parsed.tables?.length || 0} table migration(s)`,
          );
        }
      }
    } catch (error) {
      this.logger.warn(
        `Failed to load snapshot-migration.json: ${getErrorMessage(error)}`,
      );
      this.migrations = null;
    }
  }

  hasMigrations(): boolean {
    if (!this.migrations) return false;
    return (
      this.migrations.tables?.length > 0 ||
      this.migrations.tablesToDrop?.length > 0
    );
  }

  private getMongoDb(): Db | null {
    if (!this.queryBuilderService.isMongoDb()) return null;
    return this.queryBuilderService.getMongoDb();
  }

  async runMigrations(): Promise<void> {
    if (!this.hasMigrations()) {
      this.logger.log('No metadata migrations to run');
      return;
    }

    this.logger.log(
      'Running metadata migrations from snapshot-migration.json...',
    );

    const isMongoDB = this.queryBuilderService.isMongoDb();

    if (this.migrations!.tablesToDrop?.length > 0) {
      await this.dropTableMetadata(this.migrations!.tablesToDrop, isMongoDB);
    }

    for (const tableMigration of this.migrations!.tables || []) {
      await this.migrateTableMetadata(tableMigration, isMongoDB);
    }

    this.logger.log('Metadata migrations completed');
  }

  private async findTableId(
    tableName: string,
    isMongoDB: boolean,
  ): Promise<{ tableId: any; tableIdField: string } | null> {
    if (isMongoDB) {
      const db = this.getMongoDb()!;
      const table = await db
        .collection('table_definition')
        .findOne({ name: tableName });
      if (!table) return null;
      return { tableId: table._id, tableIdField: 'table' };
    }

    const tableResult = await this.queryBuilderService.find({
      table: 'table_definition',
      filter: { name: { _eq: tableName } },
      limit: 1,
    });
    if (!tableResult.data?.length) return null;
    const table = tableResult.data[0];
    return { tableId: table.id, tableIdField: 'tableId' };
  }

  private async dropTableMetadata(
    tableNames: string[],
    isMongoDB: boolean,
  ): Promise<void> {
    this.logger.log(`Dropping metadata for ${tableNames.length} table(s)...`);

    for (const tableName of tableNames) {
      try {
        const found = await this.findTableId(tableName, isMongoDB);
        if (!found) continue;

        const { tableId } = found;

        if (isMongoDB) {
          const db = this.getMongoDb()!;
          await db
            .collection('relation_definition')
            .deleteMany({ sourceTable: tableId });
          await db
            .collection('column_definition')
            .deleteMany({ table: tableId });
          await db.collection('table_definition').deleteOne({ _id: tableId });
        } else {
          await this.queryBuilderService.delete('relation_definition', {
            where: [{ field: 'sourceTableId', operator: '=', value: tableId }],
          });
          await this.queryBuilderService.delete('column_definition', {
            where: [{ field: 'tableId', operator: '=', value: tableId }],
          });
          await this.queryBuilderService.delete('table_definition', {
            where: [{ field: 'id', operator: '=', value: tableId }],
          });
        }

        this.logger.log(`  Dropped metadata for table: ${tableName}`);
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
    this.logger.log(`Migrating metadata for table: ${tableName}`);

    const found = await this.findTableId(tableName, isMongoDB);
    if (!found) {
      this.logger.warn(`  Table ${tableName} not found in metadata, skipping`);
      return;
    }

    const { tableId, tableIdField } = found;

    if (migration.columnsToModify?.length > 0) {
      await this.modifyColumnMetadata(
        tableId,
        tableIdField,
        migration.columnsToModify,
        isMongoDB,
      );
    }

    if (migration.columnsToRemove?.length > 0) {
      await this.removeColumnMetadata(
        tableName,
        tableId,
        tableIdField,
        migration.columnsToRemove,
        isMongoDB,
      );
    }

    if (migration.relationsToModify?.length > 0) {
      await this.modifyRelationMetadata(
        tableId,
        isMongoDB,
        migration.relationsToModify,
      );
    }

    if (migration.relationsToRemove?.length > 0) {
      await this.removeRelationMetadata(
        tableId,
        isMongoDB,
        migration.relationsToRemove,
      );
    }
  }

  private async modifyColumnMetadata(
    tableId: any,
    tableIdField: string,
    modifications: ColumnModifyDef[],
    isMongoDB: boolean,
  ): Promise<void> {
    for (const mod of modifications) {
      const hasChanges =
        mod.to.name !== mod.from.name ||
        (mod.to.isNullable !== undefined &&
          mod.to.isNullable !== mod.from.isNullable) ||
        (mod.to.isUpdatable !== undefined &&
          mod.to.isUpdatable !== mod.from.isUpdatable) ||
        mod.to.description !== undefined;

      if (!hasChanges) {
        continue;
      }

      const oldName = mod.from.name;

      try {
        let columnId: any;

        if (isMongoDB) {
          const db = this.getMongoDb()!;
          const column = await db.collection('column_definition').findOne({
            table: tableId,
            name: oldName,
          });
          if (!column) continue;
          columnId = column._id;
        } else {
          const columnResult = await this.queryBuilderService.find({
            table: 'column_definition',
            filter: {
              [tableIdField]: { _eq: tableId },
              name: { _eq: oldName },
            },
            limit: 1,
          });
          if (!columnResult.data?.length) continue;
          columnId = columnResult.data[0].id;
        }

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

        if (Object.keys(updateData).length > 0) {
          if (isMongoDB) {
            const db = this.getMongoDb()!;
            updateData.updatedAt = new Date();
            await db
              .collection('column_definition')
              .updateOne({ _id: columnId }, { $set: updateData });
          } else {
            await this.queryBuilderService.update(
              'column_definition',
              { where: [{ field: 'id', operator: '=', value: columnId }] },
              updateData,
            );
          }
          this.logger.log(
            `  Modified column metadata: ${oldName} → ${mod.to.name}`,
          );
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
        await this.copyLegacyScriptColumnBeforeRemove(
          tableName,
          colName,
          isMongoDB,
        );

        if (isMongoDB) {
          const db = this.getMongoDb()!;
          const result = await db
            .collection('column_definition')
            .deleteOne({ table: tableId, name: colName });
          if (result.deletedCount > 0) {
            this.logger.log(`  Removed column metadata: ${colName}`);
          }
        } else {
          const columnResult = await this.queryBuilderService.find({
            table: 'column_definition',
            filter: {
              [tableIdField]: { _eq: tableId },
              name: { _eq: colName },
            },
            limit: 1,
          });
          if (columnResult.data?.length > 0) {
            const columnId = columnResult.data[0].id;
            await this.queryBuilderService.delete('column_definition', {
              where: [{ field: 'id', operator: '=', value: columnId }],
            });
            this.logger.log(`  Removed column metadata: ${colName}`);
          }
        }

        await this.dropPhysicalColumn(tableName, colName, isMongoDB);
      } catch (err) {
        this.logger.warn(
          `  Failed to remove column ${colName}: ${(err as Error).message}`,
        );
      }
    }
  }

  private getLegacyScriptTargetColumn(
    tableName: string,
    colName: string,
  ): string | null {
    const legacyMap: Record<string, string> = {
      route_handler_definition: 'logic',
      pre_hook_definition: 'code',
      post_hook_definition: 'code',
      bootstrap_script_definition: 'logic',
      websocket_definition: 'connectionHandlerScript',
      websocket_event_definition: 'handlerScript',
    };
    return legacyMap[tableName] === colName ? 'sourceCode' : null;
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
        .updateMany({ [colName]: { $exists: true } }, { $unset: { [colName]: '' } });
      return;
    }

    const knex = this.queryBuilderService.getKnex();
    const hasColumn = await knex.schema.hasColumn(tableName, colName);
    if (!hasColumn) return;
    await knex.schema.alterTable(tableName, (table: any) => {
      table.dropColumn(colName);
    });
    this.logger.log(`  Dropped physical column: ${tableName}.${colName}`);
  }

  private async modifyRelationMetadata(
    tableId: any,
    isMongoDB: boolean,
    modifications: RelationModifyDef[],
  ): Promise<void> {
    const sourceTableField = isMongoDB ? 'sourceTable' : 'sourceTableId';

    for (const mod of modifications) {
      const hasChanges =
        mod.to.propertyName !== mod.from.propertyName ||
        (mod.to.mappedBy !== undefined &&
          mod.to.mappedBy !== mod.from.mappedBy) ||
        (mod.to.isNullable !== undefined &&
          mod.to.isNullable !== mod.from.isNullable) ||
        (mod.to.isUpdatable !== undefined &&
          mod.to.isUpdatable !== mod.from.isUpdatable) ||
        mod.to.onDelete !== undefined;

      if (!hasChanges) {
        continue;
      }

      const oldName = mod.from.propertyName;

      try {
        let relation: any;

        if (isMongoDB) {
          const db = this.getMongoDb()!;
          relation = await db.collection('relation_definition').findOne({
            sourceTable: tableId,
            propertyName: oldName,
          });
        } else {
          const relationResult = await this.queryBuilderService.find({
            table: 'relation_definition',
            filter: {
              [sourceTableField]: { _eq: tableId },
              propertyName: { _eq: oldName },
            },
            limit: 1,
          });
          relation = relationResult.data?.[0];
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
            const owningRel = await db
              .collection('relation_definition')
              .findOne({
                sourceTable: targetTableId,
                propertyName: mod.to.mappedBy,
              });
            updateData.mappedBy = owningRel?._id || null;
          } else if (mod.to.mappedBy && !isMongoDB) {
            const targetTableId = relation.targetTableId;
            const owningRels = await this.queryBuilderService.find({
              table: 'relation_definition',
              filter: {
                sourceTableId: { _eq: targetTableId },
                propertyName: { _eq: mod.to.mappedBy },
              },
              limit: 1,
            });
            updateData.mappedById = owningRels.data?.[0]?.id || null;
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
              .collection('relation_definition')
              .updateOne({ _id: relationId }, { $set: updateData });
          } else {
            await this.queryBuilderService.update(
              'relation_definition',
              { where: [{ field: 'id', operator: '=', value: relationId }] },
              updateData,
            );
          }
          this.logger.log(
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
        if (isMongoDB) {
          const db = this.getMongoDb()!;
          const result = await db
            .collection('relation_definition')
            .deleteOne({ sourceTable: tableId, propertyName: relName });
          if (result.deletedCount > 0) {
            this.logger.log(`  Removed relation metadata: ${relName}`);
          }
        } else {
          const relationResult = await this.queryBuilderService.find({
            table: 'relation_definition',
            filter: {
              [sourceTableField]: { _eq: tableId },
              propertyName: { _eq: relName },
            },
            limit: 1,
          });
          if (relationResult.data?.length > 0) {
            const relationId = relationResult.data[0].id;
            await this.queryBuilderService.delete('relation_definition', {
              where: [{ field: 'id', operator: '=', value: relationId }],
            });
            this.logger.log(`  Removed relation metadata: ${relName}`);
          }
        }
      } catch (err) {
        this.logger.warn(
          `  Failed to remove relation ${relName}: ${(err as Error).message}`,
        );
      }
    }
  }
}
