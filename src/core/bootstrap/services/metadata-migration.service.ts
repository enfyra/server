import { Injectable, Logger } from '@nestjs/common';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { SchemaMigrationDef, TableMigrationDef, ColumnModifyDef, RelationModifyDef } from '../../../shared/types/schema-migration.types';
import * as fs from 'fs';
import * as path from 'path';

@Injectable()
export class MetadataMigrationService {
  private readonly logger = new Logger(MetadataMigrationService.name);
  private migrations: SchemaMigrationDef | null = null;

  constructor(private readonly queryBuilder: QueryBuilderService) {
    this.loadMigrations();
  }

  private loadMigrations(): void {
    try {
      const filePath = path.join(process.cwd(), 'data/snapshot-migration.json');
      if (fs.existsSync(filePath)) {
        const content = fs.readFileSync(filePath, 'utf8');
        const parsed = JSON.parse(content);
        if (parsed && (parsed.tables?.length > 0 || parsed.tablesToDrop?.length > 0)) {
          this.migrations = parsed;
          this.logger.log(`Loaded snapshot-migration.json with ${parsed.tables?.length || 0} table migration(s)`);
        }
      }
    } catch (error) {
      this.logger.warn(`Failed to load snapshot-migration.json: ${error.message}`);
      this.migrations = null;
    }
  }

  hasMigrations(): boolean {
    if (!this.migrations) return false;
    return (
      (this.migrations.tables?.length > 0) ||
      (this.migrations.tablesToDrop?.length > 0)
    );
  }

  async runMigrations(): Promise<void> {
    if (!this.hasMigrations()) {
      this.logger.log('No metadata migrations to run');
      return;
    }

    this.logger.log('Running metadata migrations from snapshot-migration.json...');

    const isMongoDB = this.queryBuilder.isMongoDb();

    // Drop table metadata
    if (this.migrations!.tablesToDrop?.length > 0) {
      await this.dropTableMetadata(this.migrations!.tablesToDrop, isMongoDB);
    }

    // Apply table migrations
    for (const tableMigration of this.migrations!.tables || []) {
      await this.migrateTableMetadata(tableMigration, isMongoDB);
    }

    this.logger.log('Metadata migrations completed');
  }

  private async dropTableMetadata(tableNames: string[], isMongoDB: boolean): Promise<void> {
    this.logger.log(`Dropping metadata for ${tableNames.length} table(s)...`);

    for (const tableName of tableNames) {
      try {
        // Find table definition
        const tableResult = await this.queryBuilder.select({
          tableName: 'table_definition',
          filter: { name: { _eq: tableName } },
          limit: 1,
        });

        if (tableResult.data?.length > 0) {
          const table = tableResult.data[0];
          const tableId = isMongoDB ? table._id : table.id;
          const idField = isMongoDB ? '_id' : 'id';

          // Delete relations
          const relationField = isMongoDB ? 'sourceTable' : 'sourceTableId';
          await this.queryBuilder.delete({
            table: 'relation_definition',
            where: [{ field: relationField, operator: '=', value: tableId }],
          });

          // Delete columns
          const columnField = isMongoDB ? 'table' : 'tableId';
          await this.queryBuilder.delete({
            table: 'column_definition',
            where: [{ field: columnField, operator: '=', value: tableId }],
          });

          // Delete table
          await this.queryBuilder.delete({
            table: 'table_definition',
            where: [{ field: idField, operator: '=', value: tableId }],
          });

          this.logger.log(`  Dropped metadata for table: ${tableName}`);
        }
      } catch (error) {
        this.logger.error(`  Failed to drop metadata for ${tableName}: ${error.message}`);
      }
    }
  }

  private async migrateTableMetadata(migration: TableMigrationDef, isMongoDB: boolean): Promise<void> {
    const tableName = migration._unique.name._eq;
    this.logger.log(`Migrating metadata for table: ${tableName}`);

    // Find table definition
    const tableResult = await this.queryBuilder.select({
      tableName: 'table_definition',
      filter: { name: { _eq: tableName } },
      limit: 1,
    });

    if (!tableResult.data?.length) {
      this.logger.warn(`  Table ${tableName} not found in metadata, skipping`);
      return;
    }

    const table = tableResult.data[0];
    const tableId = isMongoDB ? table._id : table.id;
    const tableIdField = isMongoDB ? 'table' : 'tableId';

    // Handle column modifications
    if (migration.columnsToModify?.length > 0) {
      await this.modifyColumnMetadata(tableId, tableIdField, migration.columnsToModify, isMongoDB);
    }

    // Handle column removals
    if (migration.columnsToRemove?.length > 0) {
      await this.removeColumnMetadata(tableId, tableIdField, migration.columnsToRemove, isMongoDB);
    }

    // Handle relation modifications
    if (migration.relationsToModify?.length > 0) {
      await this.modifyRelationMetadata(tableId, isMongoDB, migration.relationsToModify);
    }

    // Handle relation removals
    if (migration.relationsToRemove?.length > 0) {
      await this.removeRelationMetadata(tableId, isMongoDB, migration.relationsToRemove);
    }
  }

  private async modifyColumnMetadata(
    tableId: any,
    tableIdField: string,
    modifications: ColumnModifyDef[],
    isMongoDB: boolean
  ): Promise<void> {
    for (const mod of modifications) {
      // Skip if no actual changes detected (name is same and no property changes)
      const hasChanges = mod.to.name !== mod.from.name ||
        (mod.to.isNullable !== undefined && mod.to.isNullable !== mod.from.isNullable) ||
        (mod.to.isUpdatable !== undefined && mod.to.isUpdatable !== mod.from.isUpdatable) ||
        (mod.to.description !== undefined);

      if (!hasChanges) {
        continue;
      }

      const oldName = mod.from.name;

      try {
        // Find column by old name
        const columnResult = await this.queryBuilder.select({
          tableName: 'column_definition',
          filter: {
            [tableIdField]: { _eq: tableId },
            name: { _eq: oldName },
          },
          limit: 1,
        });

        if (!columnResult.data?.length) {
          // Silently skip if column not found in metadata
          continue;
        }

        const column = columnResult.data[0];
        const columnId = isMongoDB ? column._id : column.id;
        const idField = isMongoDB ? '_id' : 'id';

        // Build update data from "to" object
        const updateData: any = {};

        // Only update fields that differ
        if (mod.to.name !== mod.from.name) {
          updateData.name = mod.to.name;
        }
        if (mod.to.isNullable !== undefined && mod.to.isNullable !== mod.from.isNullable) {
          updateData.isNullable = mod.to.isNullable;
        }
        if (mod.to.isUpdatable !== undefined && mod.to.isUpdatable !== mod.from.isUpdatable) {
          updateData.isUpdatable = mod.to.isUpdatable;
        }
        if (mod.to.description !== undefined) {
          updateData.description = mod.to.description;
        }

        if (Object.keys(updateData).length > 0) {
          await this.queryBuilder.update({
            table: 'column_definition',
            where: [{ field: idField, operator: '=', value: columnId }],
            data: updateData,
          });
          this.logger.log(`  Modified column metadata: ${oldName} → ${mod.to.name}`);
        }
      } catch {
        // Silently skip on error
      }
    }
  }

  private async removeColumnMetadata(
    tableId: any,
    tableIdField: string,
    columns: string[],
    isMongoDB: boolean
  ): Promise<void> {
    for (const colName of columns) {
      try {
        const columnResult = await this.queryBuilder.select({
          tableName: 'column_definition',
          filter: {
            [tableIdField]: { _eq: tableId },
            name: { _eq: colName },
          },
          limit: 1,
        });

        if (columnResult.data?.length > 0) {
          const column = columnResult.data[0];
          const columnId = isMongoDB ? column._id : column.id;
          const idField = isMongoDB ? '_id' : 'id';

          await this.queryBuilder.delete({
            table: 'column_definition',
            where: [{ field: idField, operator: '=', value: columnId }],
          });
          this.logger.log(`  Removed column metadata: ${colName}`);
        }
        // Silently skip if column not found in metadata
      } catch {
        // Silently skip on error
      }
    }
  }

  private async modifyRelationMetadata(
    tableId: any,
    isMongoDB: boolean,
    modifications: RelationModifyDef[]
  ): Promise<void> {
    const sourceTableField = isMongoDB ? 'sourceTable' : 'sourceTableId';

    for (const mod of modifications) {
      const hasChanges = mod.to.propertyName !== mod.from.propertyName ||
        (mod.to.inversePropertyName !== undefined && mod.to.inversePropertyName !== mod.from.inversePropertyName) ||
        (mod.to.isNullable !== undefined && mod.to.isNullable !== mod.from.isNullable) ||
        (mod.to.isUpdatable !== undefined && mod.to.isUpdatable !== mod.from.isUpdatable) ||
        (mod.to.onDelete !== undefined);

      if (!hasChanges) {
        continue;
      }

      const oldName = mod.from.propertyName;

      try {
        const filter: any = {
          [sourceTableField]: { _eq: tableId },
          propertyName: { _eq: oldName },
        };
        if (mod.from.inversePropertyName !== undefined) {
          filter.inversePropertyName = { _eq: mod.from.inversePropertyName };
        }
        const relationResult = await this.queryBuilder.select({
          tableName: 'relation_definition',
          filter,
          limit: 1,
        });

        if (!relationResult.data?.length) {
          continue;
        }

        const relation = relationResult.data[0];
        const relationId = isMongoDB ? relation._id : relation.id;
        const idField = isMongoDB ? '_id' : 'id';

        const updateData: any = {};

        if (mod.to.propertyName !== mod.from.propertyName) {
          updateData.propertyName = mod.to.propertyName;
        }
        if (mod.to.inversePropertyName !== undefined && mod.to.inversePropertyName !== mod.from.inversePropertyName) {
          updateData.inversePropertyName = mod.to.inversePropertyName;
        }
        if (mod.to.isNullable !== undefined && mod.to.isNullable !== mod.from.isNullable) {
          updateData.isNullable = mod.to.isNullable;
        }
        if (mod.to.isUpdatable !== undefined && mod.to.isUpdatable !== mod.from.isUpdatable) {
          updateData.isUpdatable = mod.to.isUpdatable;
        }
        if (mod.to.onDelete !== undefined) {
          updateData.onDelete = mod.to.onDelete;
        }

        if (Object.keys(updateData).length > 0) {
          await this.queryBuilder.update({
            table: 'relation_definition',
            where: [{ field: idField, operator: '=', value: relationId }],
            data: updateData,
          });
          this.logger.log(`  Modified relation metadata: ${oldName} → ${mod.to.propertyName}`);
        }
      } catch {
        // Silently skip on error
      }
    }
  }

  private async removeRelationMetadata(
    tableId: any,
    isMongoDB: boolean,
    relations: string[]
  ): Promise<void> {
    const sourceTableField = isMongoDB ? 'sourceTable' : 'sourceTableId';

    for (const relName of relations) {
      try {
        const relationResult = await this.queryBuilder.select({
          tableName: 'relation_definition',
          filter: {
            [sourceTableField]: { _eq: tableId },
            propertyName: { _eq: relName },
          },
          limit: 1,
        });

        if (relationResult.data?.length > 0) {
          const relation = relationResult.data[0];
          const relationId = isMongoDB ? relation._id : relation.id;
          const idField = isMongoDB ? '_id' : 'id';

          await this.queryBuilder.delete({
            table: 'relation_definition',
            where: [{ field: idField, operator: '=', value: relationId }],
          });
          this.logger.log(`  Removed relation metadata: ${relName}`);
        }
      } catch {
        // Silently skip on error
      }
    }
  }
}