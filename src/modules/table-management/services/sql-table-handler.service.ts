import { Injectable, Logger } from '@nestjs/common';
import { ModuleRef } from '@nestjs/core';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { SqlSchemaMigrationService } from '../../../infrastructure/knex/services/sql-schema-migration.service';
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
import { getForeignKeyColumnName, getJunctionTableName, getJunctionColumnNames } from '../../../infrastructure/knex/utils/naming-helpers';

@Injectable()
export class SqlTableHandlerService {
  private logger = new Logger(SqlTableHandlerService.name);

  constructor(
    private queryBuilder: QueryBuilderService,
    private schemaMigrationService: SqlSchemaMigrationService,
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

  /**
   * Validate that all columns (explicit + FK from relations + junction columns) are unique
   */
  private validateAllColumnsUnique(columns: any[], relations: any[], tableName: string, targetTablesMap: Map<number, string>) {
    const allColumnNames = new Set<string>();
    const duplicates: string[] = [];

    // 1. Add explicit columns
    for (const col of columns || []) {
      if (allColumnNames.has(col.name)) {
        duplicates.push(col.name);
      }
      allColumnNames.add(col.name);
    }

    // 2. Add FK columns from M2O and O2O relations
    for (const rel of relations || []) {
      if (['many-to-one', 'one-to-one'].includes(rel.type)) {
        const fkColumn = `${rel.propertyName}Id`;
        if (allColumnNames.has(fkColumn)) {
          duplicates.push(`${fkColumn} (FK for ${rel.propertyName})`);
        }
        allColumnNames.add(fkColumn);
      }
    }

    // 3. Check M2M junction table columns for duplicates (validation only - actual names calculated in getJunctionColumnNames)
    for (const rel of relations || []) {
      if (rel.type === 'many-to-many') {
        const targetTableId = typeof rel.targetTable === 'object' ? rel.targetTable.id : rel.targetTable;
        const targetTableName = targetTablesMap.get(targetTableId);

        if (targetTableName) {
          const { sourceColumn, targetColumn } = getJunctionColumnNames(tableName, rel.propertyName, targetTableName);

          // This should never happen now with the new naming strategy, but keep as safety check
          if (sourceColumn === targetColumn) {
            throw new ValidationException(
              `Many-to-many relation '${rel.propertyName}' in table '${tableName}' creates duplicate junction columns. ` +
              `This should not happen with the current naming strategy. Please report this bug.`,
              {
                tableName,
                relationName: rel.propertyName,
                targetTableName,
                junctionSourceColumn: sourceColumn,
                junctionTargetColumn: targetColumn,
              }
            );
          }
        }
      }
    }

    // 4. Throw error if duplicates found
    if (duplicates.length > 0) {
      throw new ValidationException(
        `Duplicate column names detected in table '${tableName}': ${duplicates.join(', ')}`,
        {
          tableName,
          duplicateColumns: duplicates,
          suggestion: 'Rename columns or relations to ensure all column names are unique.'
        }
      );
    }
  }

  async createTable(body: any) {
    this.logger.log(`\n${'='.repeat(80)}`);
    this.logger.log(`📝 CREATE TABLE: ${body?.name}`);
    this.logger.log(`${'='.repeat(80)}`);
    this.logger.log(`📋 Input Data:`);
    this.logger.log(`   - Columns: ${body.columns?.length || 0}`);
    this.logger.log(`   - Relations: ${body.relations?.length || 0}`);
    this.logger.log(`   - Columns: ${body.columns?.map((c: any) => c.name).join(', ')}`);
    this.logger.log(`   - Relations: ${body.relations?.map((r: any) => `${r.propertyName} (${r.type})`).join(', ')}`);

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
      this.logger.log(`\n🔄 Starting transaction...`);
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

      // Load target table names for M2M validation
      const targetTableIds = body.relations
        ?.filter((rel: any) => rel.type === 'many-to-many')
        ?.map((rel: any) => typeof rel.targetTable === 'object' ? rel.targetTable.id : rel.targetTable)
        ?.filter((id: any) => id != null) || [];

      const targetTablesMap = new Map<number, string>();
      if (targetTableIds.length > 0) {
        const targetTables = await trx('table_definition')
          .select('id', 'name')
          .whereIn('id', targetTableIds);

        for (const table of targetTables) {
          targetTablesMap.set(table.id, table.name);
        }
      }

      // Validate all columns are unique (explicit + FK from relations)
      try {
        this.validateAllColumnsUnique(body.columns || [], body.relations || [], body.name, targetTablesMap);
      } catch (error) {
        await trx.rollback();
        throw error;
      }

      body.isSystem = false;

      this.logger.log(`\n💾 Saving table metadata to DB...`);
      const [tableId] = await trx('table_definition').insert({
        name: body.name,
        isSystem: body.isSystem,
        alias: body.alias,
        description: body.description,
        uniques: JSON.stringify(body.uniques || []),
        indexes: JSON.stringify(body.indexes || []),
      });
      this.logger.log(`   ✅ Table metadata saved (ID: ${tableId})`);

      if (body.columns?.length > 0) {
        this.logger.log(`\n💾 Saving ${body.columns.length} column(s) metadata...`);
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
        this.logger.log(`   ✅ Column metadata saved`);
      }

      if (body.relations?.length > 0) {
        this.logger.log(`\n💾 Saving ${body.relations.length} relation(s) metadata...`);
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
            const { sourceColumn, targetColumn } = getJunctionColumnNames(body.name, rel.propertyName, targetTableName);

            this.logger.log(`   📝 M2M: ${rel.propertyName} → ${targetTableName}`);
            this.logger.log(`      Junction table: ${junctionTableName}`);
            this.logger.log(`      Columns: ${sourceColumn}, ${targetColumn}`);

            // Note: Validation already done in validateAllColumnsUnique()
            insertData.junctionTableName = junctionTableName;
            insertData.junctionSourceColumn = sourceColumn;
            insertData.junctionTargetColumn = targetColumn;
          } else {
            // Explicitly set to null for non-M2M relations
            insertData.junctionTableName = null;
            insertData.junctionSourceColumn = null;
            insertData.junctionTargetColumn = null;
          }

          relationsToInsert.push(insertData);
        }
        
        await trx('relation_definition').insert(relationsToInsert);
        this.logger.log(`   ✅ Relation metadata saved`);
      }

      this.logger.log(`\n🔧 Running physical schema migration...`);
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

      // Fetch full table metadata with columns and relations (before physical migration)
      this.logger.log(`📥 Fetching full table metadata...`);
      const fullMetadata = await this.getFullTableMetadataInTransaction(trx, tableId);

      // Migrate physical schema INSIDE transaction (before commit)
      this.logger.log(`🔨 Calling SqlSchemaMigrationService.createTable()...`);
      await this.schemaMigrationService.createTable(fullMetadata);

      // Commit transaction AFTER physical schema migration succeeds
      this.logger.log(`\n✅ Committing transaction...`);
      await trx.commit();

      this.logger.log(`\n${'='.repeat(80)}`);
      this.logger.log(`✅ TABLE CREATED SUCCESSFULLY: ${body.name}`);
      this.logger.log(`   - Metadata saved to DB`);
      this.logger.log(`   - Physical schema migrated`);
      this.logger.log(`   - Route created`);
      this.logger.log(`${'='.repeat(80)}\n`);
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

        // Load target table names for M2M validation
        const targetTableIds = body.relations
          ?.filter((rel: any) => rel.type === 'many-to-many')
          ?.map((rel: any) => typeof rel.targetTable === 'object' ? rel.targetTable.id : rel.targetTable)
          ?.filter((id: any) => id != null) || [];

        const targetTablesMap = new Map<number, string>();
        if (targetTableIds.length > 0) {
          const targetTables = await trx('table_definition')
            .select('id', 'name')
            .whereIn('id', targetTableIds);

          for (const table of targetTables) {
            targetTablesMap.set(table.id, table.name);
          }
        }

        // Validate all columns are unique (explicit + FK from relations)
        this.validateAllColumnsUnique(body.columns || [], body.relations || [], exists.name, targetTablesMap);

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
            const { sourceColumn, targetColumn } = getJunctionColumnNames(exists.name, rel.propertyName, targetTableName);

            // Note: Validation already done in validateAllColumnsUnique()
            relationData.junctionTableName = junctionTableName;
            relationData.junctionSourceColumn = sourceColumn;
            relationData.junctionTargetColumn = targetColumn;
          } else {
            // Clear junction table metadata if type is NOT M2M
            relationData.junctionTableName = null;
            relationData.junctionSourceColumn = null;
            relationData.junctionTargetColumn = null;
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
        // IMPORTANT: Preserve inverse relations (relations not owned by this table)
        // Frontend doesn't always send inverse relations, so we need to preserve them
        const preserveInverseRelations = (oldRels: any[] = [], newRels: any[] = []) => {
          // Filter inverse relations using isInverse flag (set by metadata cache)
          const inverseRels = (oldRels || []).filter(r => r.isInverse === true);

          this.logger.log(`🔍 Preserving ${inverseRels.length} inverse relations: ${inverseRels.map(r => r.propertyName).join(', ')}`);

          // Merge: keep inverse relations from old + new relations from request
          const newRelIds = new Set(newRels.map(r => r.id).filter(id => id != null));
          const preservedInverse = inverseRels.filter(r => !newRelIds.has(r.id));

          return [...newRels, ...preservedInverse];
        };

        const newMetadata = {
          name: exists.name,
          columns: body.columns !== undefined ? body.columns : (oldMetadata?.columns || []),
          relations: body.relations !== undefined
            ? preserveInverseRelations(oldMetadata?.relations, body.relations)
            : (oldMetadata?.relations || []),
          uniques: body.uniques !== undefined ? body.uniques : (oldMetadata?.uniques || []),
          indexes: body.indexes !== undefined ? body.indexes : (oldMetadata?.indexes || [])
        };

        this.logger.log(`📊 Relations after merge: old=${oldMetadata?.relations?.length || 0}, new=${body.relations?.length || 0}, final=${newMetadata.relations.length}`);

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
          const { getForeignKeyColumnName } = await import('../../../infrastructure/knex/utils/naming-helpers');
          const fkColumn = getForeignKeyColumnName('table_definition');
          await trx(junctionTableName)
            .where({ [fkColumn]: id })
            .delete();
          this.logger.log(`🗑️ Deleted junction records for table ${id}`);
        }

        // IMPORTANT: Fetch relations BEFORE deleting them (needed for junction table cleanup)
        // 1. Fetch all relations involving this table (for physical migration later)
        const allRelations = await trx('relation_definition')
          .where({ sourceTableId: id })
          .orWhere({ targetTableId: id })
          .select('*');

        this.logger.log(`🗑️ Found ${allRelations.length} relations involving table ${tableName}`);

        // 2. Fetch target relations (needed for FK column cleanup)
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
              const { getForeignKeyColumnName } = await import('../../../infrastructure/knex/utils/naming-helpers');
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

        // 4. CRITICAL: Drop ALL FK constraints referencing this table (from actual DB schema)
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

        // Now delete ALL relations metadata (both source and target)
        await trx('relation_definition')
          .where({ sourceTableId: id })
          .orWhere({ targetTableId: id })
          .delete();
        this.logger.log(`🗑️ Deleted all relations for table ${id}`);

        // 5. Delete columns
        await trx('column_definition')
          .where({ tableId: id })
          .delete();

        await trx('table_definition')
          .where({ id })
          .delete();

        // Drop physical table and junction tables INSIDE transaction (before commit)
        // Pass relations so schema migration can drop M2M junction tables
        await this.schemaMigrationService.dropTable(tableName, allRelations);

        // Commit transaction AFTER physical schema migration succeeds
        await trx.commit();

        this.logger.log(`✅ Table deleted: ${tableName} (metadata + physical schema)`);
        return exists;
      } catch (error) {
        // Rollback transaction on error (if not already committed)
        if (trx && !trx.isCompleted()) {
          try {
            await trx.rollback();
            this.logger.log(`🔄 Transaction rolled back due to error`);
          } catch (rollbackError) {
            this.logger.error(`Failed to rollback transaction: ${rollbackError.message}`);
          }
        }

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
   * Get full table metadata with columns and relations (within transaction)
   */
  private async getFullTableMetadataInTransaction(trx: any, tableId: string | number): Promise<any> {
    const table = await trx('table_definition').where({ id: tableId }).first();
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
    table.columns = await trx('column_definition')
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
    const relations = await trx('relation_definition')
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
