import { Injectable, Logger } from '@nestjs/common';
import { ModuleRef } from '@nestjs/core';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { SchemaMigrationService } from '../../../infrastructure/knex/services/schema-migration.service';
import { MetadataCacheService } from '../../../infrastructure/cache/services/metadata-cache.service';
import { LoggingService } from '../../../core/exceptions/services/logging.service';
import {
  DatabaseException,
  DuplicateResourceException,
  ResourceNotFoundException,
  ValidationException,
} from '../../../core/exceptions/custom-exceptions';
import { validateUniquePropertyNames } from '../utils/duplicate-field-check';
import { getDeletedIds } from '../utils/get-deleted-ids';
import { CreateTableDto } from '../dto/create-table.dto';
import { getForeignKeyColumnName, getJunctionTableName } from '../../../shared/utils/naming-helpers';

/**
 * SqlTableHandlerService - Manages SQL table metadata and physical schema (DDL)
 * 1. Validates and saves metadata to DB
 * 2. Migrates physical schema (CREATE/ALTER/DROP TABLE)
 */
@Injectable()
export class SqlTableHandlerService {
  private logger = new Logger(SqlTableHandlerService.name);

  constructor(
    private queryBuilder: QueryBuilderService,
    private schemaMigrationService: SchemaMigrationService,
    private metadataCacheService: MetadataCacheService,
    private loggingService: LoggingService,
    private moduleRef: ModuleRef,
  ) {}

  private validateRelations(relations: any[]) {
    for (const relation of relations || []) {
      if (relation.type === 'one-to-many' && !relation.inversePropertyName) {
        throw new ValidationException(
          `One-to-many relation '${relation.propertyName}' must have inversePropertyName`,
          {
            relationName: relation.propertyName,
            relationType: relation.type,
            missingField: 'inversePropertyName',
          },
        );
      }
    }
  }

  async createTable(body: any) {
    if (/[A-Z]/.test(body?.name)) {
      throw new ValidationException('Table name must be lowercase (no uppercase letters).', {
        tableName: body?.name,
      });
    }
    if (!/^[a-z0-9_]+$/.test(body?.name)) {
      throw new ValidationException('Table name must be snake_case (a-z, 0-9, _).', {
        tableName: body?.name,
      });
    }

    this.validateRelations(body.relations);

    const knex = this.queryBuilder.getKnex();
    let trx;

    try {
      // Start transaction
      trx = await knex.transaction();

      const hasTable = await knex.schema.hasTable(body.name);
      const existing = await trx('table_definition')
        .where({ name: body.name })
        .first();

      if (hasTable || existing) {
        await trx.rollback();
        throw new DuplicateResourceException(
          'table_definition',
          'name',
          body.name
        );
      }

      const idCol = body.columns.find(
        (col: any) => col.name === 'id' && col.isPrimary,
      );
      if (!idCol) {
        await trx.rollback();
        throw new ValidationException(
          `Table must contain a column named "id" with isPrimary = true.`,
          { tableName: body.name }
        );
      }

      const validTypes = ['int', 'uuid'];
      if (!validTypes.includes(idCol.type)) {
        await trx.rollback();
        throw new ValidationException(
          `The primary column "id" must be of type int or uuid.`,
          { tableName: body.name, idColumnType: idCol.type }
        );
      }

      const primaryCount = body.columns.filter(
        (col: any) => col.isPrimary,
      ).length;
      if (primaryCount !== 1) {
        await trx.rollback();
        throw new ValidationException(
          `Only one column is allowed to have isPrimary = true.`,
          { tableName: body.name, primaryCount }
        );
      }

      try {
        validateUniquePropertyNames(body.columns || [], body.relations || []);
      } catch (error) {
        await trx.rollback();
        throw error;
      }

      body.isSystem = false;

      const [tableId] = await trx('table_definition').insert({
        name: body.name,
        isSystem: body.isSystem,
        alias: body.alias,
        description: body.description,
        uniques: JSON.stringify(body.uniques || []),
        indexes: JSON.stringify(body.indexes || []),
      });

      if (body.columns?.length > 0) {
        const columnsToInsert = body.columns.map((col: any) => ({
          name: col.name,
          type: col.type,
          isPrimary: col.isPrimary || false,
          isGenerated: col.isGenerated || false,
          isNullable: col.isNullable ?? true,
          isSystem: col.isSystem || false,
          isUpdatable: col.isUpdatable ?? true,
          isHidden: col.isHidden || false,
          defaultValue: col.defaultValue ? JSON.stringify(col.defaultValue) : null,
          options: col.options ? JSON.stringify(col.options) : null,
          description: col.description,
          placeholder: col.placeholder,
          tableId: tableId,
        }));
        await trx('column_definition').insert(columnsToInsert);
      }

      if (body.relations?.length > 0) {
        // Load all target tables at once (avoid N+1)
        const targetTableIds = body.relations
          .map((rel: any) => typeof rel.targetTable === 'object' ? rel.targetTable.id : rel.targetTable)
          .filter((id: any) => id != null);
        
        const targetTablesMap = new Map<number, string>();
        if (targetTableIds.length > 0) {
          const targetTables = await trx('table_definition')
            .select('id', 'name')
            .whereIn('id', targetTableIds);
          
          for (const table of targetTables) {
            targetTablesMap.set(table.id, table.name);
          }
        }
        
        const relationsToInsert = [];
        
        for (const rel of body.relations) {
          // Extract targetTableId and targetTableName
          const targetTableId = typeof rel.targetTable === 'object' ? rel.targetTable.id : rel.targetTable;
          
          const insertData: any = {
            propertyName: rel.propertyName,
            type: rel.type,
            targetTableId,
            inversePropertyName: rel.inversePropertyName,
            isNullable: rel.isNullable ?? true,
            isSystem: rel.isSystem || false,
            description: rel.description,
            sourceTableId: tableId,
          };

          // Add junction table info for M2M relations
          if (rel.type === 'many-to-many') {
            const targetTableName = targetTablesMap.get(targetTableId);
            
            if (!targetTableName) {
              throw new Error(`Target table with ID ${targetTableId} not found`);
            }
            
            const junctionTableName = getJunctionTableName(body.name, rel.propertyName, targetTableName);
            insertData.junctionTableName = junctionTableName;
            insertData.junctionSourceColumn = getForeignKeyColumnName(body.name);
            insertData.junctionTargetColumn = getForeignKeyColumnName(targetTableName);
          }

          relationsToInsert.push(insertData);
        }
        
        await trx('relation_definition').insert(relationsToInsert);
      }

      // Check if route already exists before creating
      const existingRoute = await trx('route_definition')
        .where({ path: `/${body.name}` })
        .first();

      if (!existingRoute) {
        await trx('route_definition').insert({
          path: `/${body.name}`,
          mainTableId: tableId,
          isEnabled: true,
          isSystem: false,
          icon: 'lucide:table',
        });
        this.logger.log(`✅ Route /${body.name} created for table ${body.name}`);
      } else {
        this.logger.warn(`Route /${body.name} already exists, skipping route creation`);
      }

      // Commit transaction BEFORE physical schema migration
      await trx.commit();

      // Fetch full table metadata with columns and relations (after commit)
      const fullMetadata = await this.getFullTableMetadata(tableId);

      // Migrate physical schema (after commit)
      await this.schemaMigrationService.createTable(fullMetadata);

      this.logger.log(`✅ Table created: ${body.name} (metadata + physical schema + route)`);
      return fullMetadata;
    } catch (error) {
      // Rollback transaction on error (if not already committed/rolled back)
      if (trx && !trx.isCompleted()) {
        try {
          await trx.rollback();
        } catch (rollbackError) {
          this.logger.error(`Failed to rollback transaction: ${rollbackError.message}`);
        }
      }

      this.loggingService.error('Table creation failed', {
        context: 'createTable',
        error: error.message,
        stack: error.stack,
        tableName: body?.name,
      });

      throw new DatabaseException(
        `Failed to create table: ${error.message}`,
        {
          tableName: body?.name,
          operation: 'create',
        },
      );
    }
  }

  async updateTable(id: string | number, body: any) {
    if (body.name && /[A-Z]/.test(body.name)) {
      throw new ValidationException('Table name must be lowercase.', {
        tableName: body.name,
      });
    }
    if (body.name && !/^[a-z0-9_]+$/.test(body.name)) {
      throw new ValidationException('Table name must be snake_case.', {
        tableName: body.name,
      });
    }

    this.validateRelations(body.relations);

    const knex = this.queryBuilder.getKnex();

    // Wrap entire operation in transaction
    return await knex.transaction(async (trx) => {
      try {
        const exists = await trx('table_definition')
          .where({ id })
          .first();

        if (!exists) {
          throw new ResourceNotFoundException(
            'table_definition',
            String(id)
          );
        }

        if (exists.isSystem) {
          throw new ValidationException(
            'Cannot modify system table',
            { tableId: id, tableName: exists.name }
          );
        }

        validateUniquePropertyNames(body.columns || [], body.relations || []);

        // Skip validation for ID column - it's always present and immutable

        await trx('table_definition')
          .where({ id })
          .update({
            name: body.name,
            alias: body.alias,
            description: body.description,
            uniques: body.uniques ? JSON.stringify(body.uniques) : exists.uniques,
            indexes: body.indexes ? JSON.stringify(body.indexes) : exists.indexes,
          });

        if (body.columns) {
          const existingColumns = await trx('column_definition')
            .where({ tableId: id })
            .select('id');

          const deletedColumnIds = getDeletedIds(
            existingColumns,
            body.columns,
          );

          if (deletedColumnIds.length > 0) {
            await trx('column_definition')
              .whereIn('id', deletedColumnIds)
              .delete();
          }

        for (const col of body.columns) {
          // Skip system columns - they're immutable and always present
          if (col.name === 'id' || col.name === 'createdAt' || col.name === 'updatedAt') {
            continue;
          }

          const columnData = {
            name: col.name,
            type: col.type,
            isPrimary: col.isPrimary || false,
            isGenerated: col.isGenerated || false,
            isNullable: col.isNullable ?? true,
            isSystem: col.isSystem || false,
            isUpdatable: col.isUpdatable ?? true,
            isHidden: col.isHidden || false,
            defaultValue: col.defaultValue ? JSON.stringify(col.defaultValue) : null,
            options: col.options ? JSON.stringify(col.options) : null,
            description: col.description,
            placeholder: col.placeholder,
            tableId: id,
          };

          if (col.id) {
            await trx('column_definition')
              .where({ id: col.id })
              .update(columnData);
          } else {
            await trx('column_definition').insert(columnData);
          }
        }
      }

        if (body.relations) {
          const existingRelations = await trx('relation_definition')
            .where({ sourceTableId: id })
            .select('id');

          const deletedRelationIds = getDeletedIds(
            existingRelations,
            body.relations,
          );

          if (deletedRelationIds.length > 0) {
            await trx('relation_definition')
              .whereIn('id', deletedRelationIds)
              .delete();
          }

        // Load all target tables at once (avoid N+1)
        const targetTableIds = body.relations
          .map((rel: any) => typeof rel.targetTable === 'object' ? rel.targetTable.id : rel.targetTable)
          .filter((id: any) => id != null);
        
        const targetTablesMap = new Map<number, string>();
        if (targetTableIds.length > 0) {
          const targetTables = await trx('table_definition')
            .select('id', 'name')
            .whereIn('id', targetTableIds);
          
          for (const table of targetTables) {
            targetTablesMap.set(table.id, table.name);
          }
        }
        
        for (const rel of body.relations) {
          // Extract targetTableId and targetTableName
          const targetTableId = typeof rel.targetTable === 'object' ? rel.targetTable.id : rel.targetTable;
          
          const relationData: any = {
            propertyName: rel.propertyName,
            type: rel.type,
            targetTableId,
            inversePropertyName: rel.inversePropertyName,
            isNullable: rel.isNullable ?? true,
            isSystem: rel.isSystem || false,
            description: rel.description,
            sourceTableId: id,
          };

          // Add junction table info for M2M relations
          if (rel.type === 'many-to-many') {
            const targetTableName = targetTablesMap.get(targetTableId);
            
            if (!targetTableName) {
              throw new Error(`Target table with ID ${targetTableId} not found`);
            }
            
            const junctionTableName = getJunctionTableName(exists.name, rel.propertyName, targetTableName);
            relationData.junctionTableName = junctionTableName;
            relationData.junctionSourceColumn = getForeignKeyColumnName(exists.name);
            relationData.junctionTargetColumn = getForeignKeyColumnName(targetTableName);
          }

          if (rel.id) {
            await trx('relation_definition')
              .where({ id: rel.id })
              .update(relationData);
          } else {
            await trx('relation_definition').insert(relationData);
          }
        }
      }

        // Get old metadata before migration
        const oldMetadata = await this.metadataCacheService.lookupTableByName(exists.name);

        // Create new metadata from request body (before database update)
        const newMetadata = {
          name: exists.name,
          columns: body.columns || [],
          relations: body.relations || [],
          uniques: body.uniques || [],
          indexes: body.indexes || []
        };

        // Migrate physical schema
        if (oldMetadata && newMetadata) {
          await this.schemaMigrationService.updateTable(exists.name, oldMetadata, newMetadata);
        }

        this.logger.log(`✅ Table updated: ${exists.name} (metadata + physical schema)`);
        
        // Return the table object with id for consistency with other methods
        return {
          id: exists.id,
          name: exists.name,
          ...newMetadata
        };
      } catch (error) {
        this.loggingService.error('Table update failed', {
          context: 'updateTable',
          error: error.message,
          stack: error.stack,
          tableId: id,
          tableName: body?.name,
        });

        throw new DatabaseException(
          `Failed to update table: ${error.message}`,
          {
            tableId: id,
            operation: 'update',
          },
        );
      }
    });
  }

  async delete(id: string | number) {
    const knex = this.queryBuilder.getKnex();

    // Wrap entire operation in transaction
    return await knex.transaction(async (trx) => {
      try {
        const exists = await trx('table_definition')
          .where({ id })
          .first();

        if (!exists) {
          throw new ResourceNotFoundException(
            'table_definition',
            String(id)
          );
        }

        if (exists.isSystem) {
          throw new ValidationException(
            'Cannot delete system table',
            { tableId: id, tableName: exists.name }
          );
        }

        const tableName = exists.name;

        // Delete routes with this table as mainTable
        const deletedRoutes = await trx('route_definition')
          .where({ mainTableId: id })
          .delete();
        this.logger.log(`🗑️ Deleted ${deletedRoutes} routes with mainTableId = ${id}`);
        
        // Delete M2M relations in junction table (route_definition_targetTables_table_definition)
        const junctionTableName = 'route_definition_targetTables_table_definition';
        if (await trx.schema.hasTable(junctionTableName)) {
          const { getForeignKeyColumnName } = await import('../../../shared/utils/naming-helpers');
          const fkColumn = getForeignKeyColumnName('table_definition');
          await trx(junctionTableName)
            .where({ [fkColumn]: id })
            .delete();
          this.logger.log(`🗑️ Deleted junction records for table ${id}`);
        }

        // Delete metadata - remove ALL relations that reference this table
        // 1. Delete relations where this table is the source
        await trx('relation_definition')
          .where({ sourceTableId: id })
          .delete();
        this.logger.log(`🗑️ Deleted source relations for table ${id}`);
        
        // 2. Delete relations where this table is the target AND drop FK columns
        const targetRelations = await trx('relation_definition')
          .where({ targetTableId: id })
          .select('*');
        
        this.logger.log(`🗑️ Found ${targetRelations.length} target relations for table ${tableName}`);
        
        // Drop FK columns from source tables before deleting relations
        for (const rel of targetRelations) {
          if (['one-to-many', 'many-to-one', 'one-to-one'].includes(rel.type)) {
            const sourceTable = await trx('table_definition')
              .where({ id: rel.sourceTableId })
              .first();
            
            if (sourceTable) {
              const { getForeignKeyColumnName } = await import('../../../shared/utils/naming-helpers');
              const fkColumn = getForeignKeyColumnName(tableName); // FK column name in source table
              
              this.logger.log(`🗑️ Dropping FK column ${fkColumn} from table ${sourceTable.name}`);
              
              // Check if column exists before dropping
              const columnExists = await trx.schema.hasColumn(sourceTable.name, fkColumn);
              if (columnExists) {
                // Drop FK constraint first
                try {
                  const fkConstraints = await trx.raw(`
                    SELECT CONSTRAINT_NAME 
                    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
                    WHERE TABLE_SCHEMA = DATABASE() 
                    AND TABLE_NAME = ? 
                    AND COLUMN_NAME = ? 
                    AND REFERENCED_TABLE_NAME IS NOT NULL
                  `, [sourceTable.name, fkColumn]);
                  
                  if (fkConstraints[0] && fkConstraints[0].length > 0) {
                    const actualFkName = fkConstraints[0][0].CONSTRAINT_NAME;
                    await trx.raw(`ALTER TABLE \`${sourceTable.name}\` DROP FOREIGN KEY \`${actualFkName}\``);
                    this.logger.log(`🗑️ Dropped FK constraint: ${actualFkName}`);
                  }
                } catch (error) {
                  this.logger.log(`⚠️ Error dropping FK constraint: ${error.message}`);
                }
                
                // Drop FK column
                await trx.raw(`ALTER TABLE \`${sourceTable.name}\` DROP COLUMN \`${fkColumn}\``);
                this.logger.log(`🗑️ Dropped FK column: ${fkColumn} from ${sourceTable.name}`);
              }
            }
          }
        }

        // 3. CRITICAL: Drop ALL FK constraints referencing this table (from actual DB schema)
        // This handles cases where FK columns exist but metadata is missing
        this.logger.log(`🗑️ Checking for ALL FK constraints referencing table ${tableName}...`);
        
        try {
          const allFkConstraints = await trx.raw(`
            SELECT 
              TABLE_NAME,
              COLUMN_NAME,
              CONSTRAINT_NAME,
              REFERENCED_TABLE_NAME,
              REFERENCED_COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
            WHERE TABLE_SCHEMA = DATABASE() 
            AND REFERENCED_TABLE_NAME = ?
            AND REFERENCED_COLUMN_NAME IS NOT NULL
          `, [tableName]);
          
          if (allFkConstraints[0] && allFkConstraints[0].length > 0) {
            this.logger.log(`🗑️ Found ${allFkConstraints[0].length} FK constraints referencing ${tableName}`);
            
            for (const fk of allFkConstraints[0]) {
              this.logger.log(`🗑️ Dropping FK constraint: ${fk.CONSTRAINT_NAME} from ${fk.TABLE_NAME}.${fk.COLUMN_NAME}`);
              
              // Drop FK constraint
              await trx.raw(`ALTER TABLE \`${fk.TABLE_NAME}\` DROP FOREIGN KEY \`${fk.CONSTRAINT_NAME}\``);
              this.logger.log(`🗑️ Dropped FK constraint: ${fk.CONSTRAINT_NAME}`);
              
              // Drop FK column
              await trx.raw(`ALTER TABLE \`${fk.TABLE_NAME}\` DROP COLUMN \`${fk.COLUMN_NAME}\``);
              this.logger.log(`🗑️ Dropped FK column: ${fk.COLUMN_NAME} from ${fk.TABLE_NAME}`);
            }
          } else {
            this.logger.log(`🗑️ No FK constraints found referencing ${tableName}`);
          }
        } catch (error) {
          this.logger.log(`⚠️ Error checking FK constraints: ${error.message}`);
        }
        
        // Now delete the relations metadata
        await trx('relation_definition')
          .where({ targetTableId: id })
          .delete();
        this.logger.log(`🗑️ Deleted target relations for table ${id}`);

        // 3. Delete columns
        await trx('column_definition')
          .where({ tableId: id })
          .delete();

        await trx('table_definition')
          .where({ id })
          .delete();

        // Drop physical table
        await this.schemaMigrationService.dropTable(tableName);

        this.logger.log(`✅ Table deleted: ${tableName} (metadata + physical schema)`);
        return exists;
      } catch (error) {
        this.loggingService.error('Table deletion failed', {
          context: 'delete',
          error: error.message,
          stack: error.stack,
          tableId: id,
        });

        throw new DatabaseException(
          `Failed to delete table: ${error.message}`,
          {
            tableId: id,
            operation: 'delete',
          },
        );
      }
    });
  }

  /**
   * Get full table metadata with columns and relations
   */
  private async getFullTableMetadata(tableId: string | number): Promise<any> {
    const knex = this.queryBuilder.getKnex();

    const table = await knex('table_definition').where({ id: tableId }).first();
    if (!table) return null;

    // Parse JSON fields
    if (table.uniques && typeof table.uniques === 'string') {
      try {
        table.uniques = JSON.parse(table.uniques);
      } catch (e) {
        table.uniques = [];
      }
    }
    if (table.indexes && typeof table.indexes === 'string') {
      try {
        table.indexes = JSON.parse(table.indexes);
      } catch (e) {
        table.indexes = [];
      }
    }

    // Load columns
    table.columns = await knex('column_definition')
      .where({ tableId })
      .select('*');

    // Parse column JSON fields
    for (const col of table.columns) {
      if (col.defaultValue && typeof col.defaultValue === 'string') {
        try {
          col.defaultValue = JSON.parse(col.defaultValue);
        } catch (e) {
          // Keep as string
        }
      }
      if (col.options && typeof col.options === 'string') {
        try {
          col.options = JSON.parse(col.options);
        } catch (e) {
          // Keep as string
        }
      }
    }

    // Load relations with target table names (use JOIN to avoid N+1)
    const relations = await knex('relation_definition')
      .where({ 'relation_definition.sourceTableId': tableId })
      .leftJoin('table_definition', 'relation_definition.targetTableId', 'table_definition.id')
      .select(
        'relation_definition.*',
        'table_definition.name as targetTableName'
      );

    // Add computed fields
    for (const rel of relations) {
      rel.sourceTableName = table.name;
      
      if (['many-to-one', 'one-to-one'].includes(rel.type)) {
        rel.foreignKeyColumn = getForeignKeyColumnName(rel.propertyName);
      }
      // M2M junction info already stored in DB
    }

    table.relations = relations;

    return table;
  }
}
