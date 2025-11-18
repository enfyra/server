import { Injectable, Logger } from '@nestjs/common';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { SqlSchemaMigrationService } from '../../../infrastructure/knex/services/sql-schema-migration.service';
import { SchemaMigrationLockService } from '../../../infrastructure/knex/services/schema-migration-lock.service';
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
    private schemaMigrationLockService: SchemaMigrationLockService,
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

  private async validateNoDuplicateInverseRelation(
    trx: any,
    sourceTableId: number,
    sourceTableName: string,
    newRelations: any[],
    targetTablesMap: Map<number, string>,
  ): Promise<void> {
    for (const rel of newRelations || []) {
      const targetTableId = typeof rel.targetTable === 'object' ? rel.targetTable.id : rel.targetTable;
      if (!targetTableId) continue;

      const targetTableName = targetTablesMap.get(targetTableId);
      if (!targetTableName) continue;

      let inverseExists = false;
      let inverseRelationInfo = null;

      if (rel.type === 'many-to-one' || rel.type === 'one-to-one') {
        const targetRelations = await trx('relation_definition')
          .where({ sourceTableId: targetTableId })
          .where({ targetTableId: sourceTableId })
          .where((builder: any) => {
            if (rel.inversePropertyName) {
              builder
                .where({ propertyName: rel.inversePropertyName })
                .orWhere({ inversePropertyName: rel.propertyName });
            } else {
              builder.where({ inversePropertyName: rel.propertyName });
            }
          })
          .select('*');

        if (targetRelations.length > 0) {
          inverseExists = true;
          inverseRelationInfo = {
            table: targetTableName,
            propertyName: targetRelations[0].propertyName,
            type: targetRelations[0].type,
          };
        }
      } else if (rel.type === 'one-to-many') {
        if (!rel.inversePropertyName) continue;

        const targetRelations = await trx('relation_definition')
          .where({ sourceTableId: targetTableId })
          .where({ targetTableId: sourceTableId })
          .where({ propertyName: rel.inversePropertyName })
          .whereIn('type', ['many-to-one', 'one-to-one'])
          .select('*');

        if (targetRelations.length > 0) {
          inverseExists = true;
          inverseRelationInfo = {
            table: targetTableName,
            propertyName: targetRelations[0].propertyName,
            type: targetRelations[0].type,
          };
        }
      } else if (rel.type === 'many-to-many') {
        if (!rel.inversePropertyName) continue;

        const targetRelations = await trx('relation_definition')
          .where({ sourceTableId: targetTableId })
          .where({ targetTableId: sourceTableId })
          .where({ propertyName: rel.inversePropertyName })
          .where({ type: 'many-to-many' })
          .select('*');

        if (targetRelations.length > 0) {
          inverseExists = true;
          inverseRelationInfo = {
            table: targetTableName,
            propertyName: targetRelations[0].propertyName,
            type: targetRelations[0].type,
          };
        }
      }

      if (inverseExists && inverseRelationInfo) {
        throw new ValidationException(
          `Cannot create relation '${rel.propertyName}' (${rel.type}) from '${sourceTableName}' to '${targetTableName}': ` +
          `The inverse relation already exists on target table '${targetTableName}' as '${inverseRelationInfo.propertyName}' (${inverseRelationInfo.type}). ` +
          `Relations should be created on ONLY ONE side. System automatically handles the inverse relation. ` +
          `Please remove the relation from '${targetTableName}' or update it instead of creating a duplicate.`,
          {
            sourceTable: sourceTableName,
            targetTable: targetTableName,
            relationName: rel.propertyName,
            relationType: rel.type,
            existingInverseTable: targetTableName,
            existingInverseRelation: inverseRelationInfo.propertyName,
            existingInverseType: inverseRelationInfo.type,
          },
        );
      }
    }
  }

  private validateAllColumnsUnique(columns: any[], relations: any[], tableName: string, targetTablesMap: Map<number, string>) {
    const allColumnNames = new Set<string>();
    const duplicates: string[] = [];

    for (const col of columns || []) {
      if (allColumnNames.has(col.name)) {
        duplicates.push(col.name);
      }
      allColumnNames.add(col.name);
    }

    for (const rel of relations || []) {
      if (['many-to-one', 'one-to-one'].includes(rel.type)) {
        const fkColumn = `${rel.propertyName}Id`;
        if (allColumnNames.has(fkColumn)) {
          duplicates.push(`${fkColumn} (FK for ${rel.propertyName})`);
        }
        allColumnNames.add(fkColumn);
      }
    }

    for (const rel of relations || []) {
      if (rel.type === 'many-to-many') {
        const targetTableId = typeof rel.targetTable === 'object' ? rel.targetTable.id : rel.targetTable;
        const targetTableName = targetTablesMap.get(targetTableId);

        if (targetTableName) {
          const { sourceColumn, targetColumn } = getJunctionColumnNames(tableName, rel.propertyName, targetTableName);

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

  async createTable(body: CreateTableDto) {
    return await this.runWithSchemaLock(`table:create:${body?.name || 'unknown'}`, () => this.createTableInternal(body));
  }

  private async createTableInternal(body: CreateTableDto) {
    this.logger.log(`CREATE TABLE: ${body?.name} (${body.columns?.length || 0} columns, ${body.relations?.length || 0} relations)`);

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

    let schemaCreated = false;
    let createdMetadataSnapshot: any = null;

    try {
      trx = await knex.transaction();

      // Check metadata and physical table SEPARATELY
      const hasTable = await knex.schema.hasTable(body.name);
      const existing = await trx('table_definition')
        .where({ name: body.name })
        .first();

      // If metadata exists, throw error (normal case)
      if (existing) {
        await trx.rollback();
        throw new DuplicateResourceException(
          'table_definition',
          'name',
          body.name
        );
      }

      // If physical table exists but no metadata (mismatch) -> drop physical table first
      if (hasTable && !existing) {
        this.logger.warn(`Mismatch detected: Physical table "${body.name}" exists but no metadata found. Dropping physical table...`);
        try {
          // No metadata exists, so no relations to check - drop physical table with empty relations
          await this.schemaMigrationService.dropTable(body.name, [], trx);
          this.logger.log(`Physical table "${body.name}" dropped successfully`);
        } catch (dropError) {
          await trx.rollback();
          this.logger.error(`Failed to drop physical table "${body.name}": ${dropError.message}`);
          throw new DatabaseException(
            `Failed to drop existing physical table "${body.name}": ${dropError.message}`,
            { tableName: body.name, operation: 'drop_existing_table' }
          );
        }
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

      try {
        this.validateAllColumnsUnique(body.columns || [], body.relations || [], body.name, targetTablesMap);
      } catch (error) {
        await trx.rollback();
        throw error;
      }

      body.isSystem = false;

      this.logger.log(`\nSaving table metadata to DB...`);
      const dbType = this.queryBuilder.getDatabaseType();
      const insertResult = await trx('table_definition').insert({
        name: body.name,
        isSystem: body.isSystem,
        alias: body.alias,
        description: body.description,
        uniques: JSON.stringify(body.uniques || []),
        indexes: JSON.stringify(body.indexes || []),
      }, dbType === 'postgres' ? ['id'] : undefined);
      const tableId = dbType === 'postgres' ? insertResult[0]?.id : insertResult[0];
      this.logger.log(`   Table metadata saved (ID: ${tableId})`);

      if (body.columns?.length > 0) {
        this.logger.log(`\nSaving ${body.columns.length} column(s) metadata...`);
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
        this.logger.log(`   Column metadata saved`);
      }

      if (body.relations?.length > 0) {
        this.logger.log(`\nSaving ${body.relations.length} relation(s) metadata...`);
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

          if (rel.type === 'many-to-many') {
            const targetTableName = targetTablesMap.get(targetTableId);

            if (!targetTableName) {
              throw new Error(`Target table with ID ${targetTableId} not found`);
            }

            const junctionTableName = getJunctionTableName(body.name, rel.propertyName, targetTableName);
            const { sourceColumn, targetColumn } = getJunctionColumnNames(body.name, rel.propertyName, targetTableName);

            this.logger.log(`   M2M: ${rel.propertyName} → ${targetTableName}`);
            this.logger.log(`      Junction table: ${junctionTableName}`);
            this.logger.log(`      Columns: ${sourceColumn}, ${targetColumn}`);

            insertData.junctionTableName = junctionTableName;
            insertData.junctionSourceColumn = sourceColumn;
            insertData.junctionTargetColumn = targetColumn;
          } else {
            insertData.junctionTableName = null;
            insertData.junctionSourceColumn = null;
            insertData.junctionTargetColumn = null;
          }

          relationsToInsert.push(insertData);
        }
        
        await trx('relation_definition').insert(relationsToInsert);
        this.logger.log(`   Relation metadata saved`);
      }

      this.logger.log(`\n🔧 Running physical schema migration...`);
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
        this.logger.log(`Route /${body.name} created for table ${body.name}`);
      } else {
        this.logger.warn(`Route /${body.name} already exists, skipping route creation`);
      }

      this.logger.log(`Fetching full table metadata...`);
      const fullMetadata = await this.getFullTableMetadataInTransaction(trx, tableId);

      if (!fullMetadata) {
        throw new Error(`Failed to fetch metadata for table ${body.name}`);
      }

      if (fullMetadata.relations) {
        for (const rel of fullMetadata.relations) {
          if (['many-to-one', 'one-to-one'].includes(rel.type)) {
            if (!rel.targetTableName) {
              throw new Error(`Relation '${rel.propertyName}' (${rel.type}) from table '${body.name}' has invalid targetTableId: ${rel.targetTableId}. Target table not found. Please verify the target table ID is correct.`);
            }
          }
        }
      }

      this.logger.log(`Calling SqlSchemaMigrationService.createTable()...`);
      await this.schemaMigrationService.createTable(fullMetadata);
      schemaCreated = true;
      createdMetadataSnapshot = fullMetadata;

      this.logger.log(`\nCommitting transaction...`);
      await trx.commit();

      this.logger.log(`\n${'='.repeat(80)}`);
      this.logger.log(`TABLE CREATED SUCCESSFULLY: ${body.name}`);
      this.logger.log(`   - Metadata saved to DB`);
      this.logger.log(`   - Physical schema migrated`);
      this.logger.log(`   - Route created`);
      this.logger.log(`${'='.repeat(80)}\n`);
      return fullMetadata;
    } catch (error) {
      if (trx && !trx.isCompleted()) {
        try {
          await trx.rollback();
        } catch (rollbackError) {
          this.logger.error(`Failed to rollback transaction: ${rollbackError.message}`);
        }
      }

      if (schemaCreated) {
        try {
          await this.schemaMigrationService.dropTable(body.name, createdMetadataSnapshot?.relations || body.relations || []);
          this.logger.warn(`Rolled back physical table ${body.name} after failure`);
        } catch (dropError) {
          this.logger.error(`Failed to rollback physical table ${body.name}: ${dropError.message}`);
        }
      }

      this.loggingService.error('Table creation failed', {
        context: 'createTable',
        error: (error as Error)?.message,
        stack: (error as Error)?.stack,
        tableName: body?.name,
      });

      throw new DatabaseException(
        `Failed to create table: ${(error as Error)?.message}`,
        {
          tableName: body?.name,
          operation: 'create',
        },
      );
    }
  }

  async updateTable(id: string | number, body: CreateTableDto) {
    return await this.runWithSchemaLock(`table:update:${id}`, () => this.updateTableInternal(id, body));
  }

  private async updateTableInternal(id: string | number, body: CreateTableDto) {
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

    try {
      const { oldMetadata, newMetadata, result } = await knex.transaction(async (trx) => {
        const exists = await trx('table_definition')
          .where({ id })
          .first();

        if (!exists) {
          throw new ResourceNotFoundException(
            'table_definition',
            String(id)
          );
        }

        validateUniquePropertyNames(body.columns || [], body.relations || []);

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

        this.validateAllColumnsUnique(body.columns || [], body.relations || [], exists.name, targetTablesMap);

        if (body.relations && body.relations.length > 0) {
          await this.validateNoDuplicateInverseRelation(trx, Number(id), exists.name, body.relations, targetTablesMap);
        }

        const previousTableName = exists.name;
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
          const targetTableId = typeof rel.targetTable === 'object' ? rel.targetTable.id : rel.targetTable;

          // ✅ VALIDATION: Prevent relation type changes (only allow rename)
          if (rel.id) {
            const existingRel = await trx('relation_definition').where({ id: rel.id }).first();
            if (existingRel && existingRel.type !== rel.type) {
              throw new Error(
                `Cannot change relation type from '${existingRel.type}' to '${rel.type}' for property '${rel.propertyName}'. ` +
                `Please delete the old relation and create a new one.`
              );
            }
          }

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

          if (rel.type === 'many-to-many') {
            const targetTableName = targetTablesMap.get(targetTableId);

            if (!targetTableName) {
              throw new Error(`Target table with ID ${targetTableId} not found`);
            }

            if (rel.id) {
              const existingRel = await trx('relation_definition')
                .where({ id: rel.id })
                .first();
              
              if (existingRel && existingRel.junctionTableName) {
                relationData.junctionTableName = existingRel.junctionTableName;
                relationData.junctionSourceColumn = existingRel.junctionSourceColumn;
                relationData.junctionTargetColumn = existingRel.junctionTargetColumn;
              } else {
            const junctionTableName = getJunctionTableName(exists.name, rel.propertyName, targetTableName);
            const { sourceColumn, targetColumn } = getJunctionColumnNames(exists.name, rel.propertyName, targetTableName);
            relationData.junctionTableName = junctionTableName;
            relationData.junctionSourceColumn = sourceColumn;
            relationData.junctionTargetColumn = targetColumn;
              }
            } else {
              const junctionTableName = getJunctionTableName(exists.name, rel.propertyName, targetTableName);
              const { sourceColumn, targetColumn } = getJunctionColumnNames(exists.name, rel.propertyName, targetTableName);
              relationData.junctionTableName = junctionTableName;
              relationData.junctionSourceColumn = sourceColumn;
              relationData.junctionTargetColumn = targetColumn;
            }
          } else {
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

        const oldMetadata = await this.metadataCacheService.lookupTableByName(exists.name);

        const preserveInverseRelations = (oldRels: any[] = [], newRels: any[] = []) => {
          const inverseRels = (oldRels || []).filter(r => r.isInverse === true);

          this.logger.log(`Preserving ${inverseRels.length} inverse relations: ${inverseRels.map(r => r.propertyName).join(', ')}`);

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

        return { oldMetadata, newMetadata, result: { id: exists.id, name: exists.name, ...newMetadata } };
      });

        if (oldMetadata && newMetadata) {
        this.logger.log(`Executing DDL statements outside transaction...`);

        // ✅ FIX: Reload fullMetadata từ DB sau khi transaction commit để lấy IDs mới của relations/columns
        const knex = this.queryBuilder.getKnex();
        const updatedFullMetadata = await this.getFullTableMetadataInTransaction(knex, result.id);

        if (!updatedFullMetadata) {
          throw new Error(`Failed to reload metadata after transaction for table ${result.name}`);
        }

        await this.schemaMigrationService.updateTable(result.name, oldMetadata, updatedFullMetadata);
        }

      this.logger.log(`Table updated: ${result.name} (metadata + physical schema)`);
        
      return result;
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
  }

  async delete(id: string | number) {
    return await this.runWithSchemaLock(`table:delete:${id}`, () => this.deleteTableInternal(id));
  }

  private async deleteTableInternal(id: string | number) {
    const knex = this.queryBuilder.getKnex();

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

        const deletedRoutes = await trx('route_definition')
          .where({ mainTableId: id })
          .delete();
        this.logger.log(`Deleted ${deletedRoutes} routes with mainTableId = ${id}`);
        
        const junctionTableName = 'route_definition_targetTables_table_definition';
        if (await trx.schema.hasTable(junctionTableName)) {
          const { getForeignKeyColumnName } = await import('../../../infrastructure/knex/utils/naming-helpers');
          const fkColumn = getForeignKeyColumnName('table_definition');
          await trx(junctionTableName)
            .where({ [fkColumn]: id })
            .delete();
          this.logger.log(`Deleted junction records for table ${id}`);
        }

        const allRelations = await trx('relation_definition')
          .where({ sourceTableId: id })
          .orWhere({ targetTableId: id })
          .select('*');

        this.logger.log(`Found ${allRelations.length} relations involving table ${tableName}`);

        const targetRelations = await trx('relation_definition')
          .where({ targetTableId: id })
          .select('*');

        this.logger.log(`Found ${targetRelations.length} target relations for table ${tableName}`);

        for (const rel of targetRelations) {
          if (['one-to-many', 'many-to-one', 'one-to-one'].includes(rel.type)) {
            const sourceTable = await trx('table_definition')
              .where({ id: rel.sourceTableId })
              .first();

            if (sourceTable) {
              const { getForeignKeyColumnName } = await import('../../../infrastructure/knex/utils/naming-helpers');
              const fkColumn = getForeignKeyColumnName(tableName);

              this.logger.log(`Dropping FK column ${fkColumn} from table ${sourceTable.name}`);

              const columnExists = await trx.schema.hasColumn(sourceTable.name, fkColumn);
              if (columnExists) {
                try {
                  const dbType = this.queryBuilder.getDatabaseType();
                  let constraintName: string | null = null;

                  if (dbType === 'postgres') {
                    const result = await trx.raw(`
                      SELECT tc.constraint_name
                      FROM information_schema.table_constraints AS tc
                      JOIN information_schema.key_column_usage AS kcu
                        ON tc.constraint_name = kcu.constraint_name
                        AND tc.table_schema = kcu.table_schema
                      WHERE tc.constraint_type = 'FOREIGN KEY'
                        AND tc.table_schema = 'public'
                        AND tc.table_name = ?
                        AND kcu.column_name = ?
                    `, [sourceTable.name, fkColumn]);
                    constraintName = result.rows[0]?.constraint_name || null;
                  } else if (dbType === 'mysql') {
                    const result = await trx.raw(`
                      SELECT CONSTRAINT_NAME as constraint_name
                      FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                      WHERE TABLE_SCHEMA = DATABASE()
                        AND TABLE_NAME = ?
                        AND COLUMN_NAME = ?
                        AND REFERENCED_TABLE_NAME IS NOT NULL
                    `, [sourceTable.name, fkColumn]);
                    constraintName = result[0][0]?.constraint_name || null;
                  }

                  if (constraintName) {
                    const qt = dbType === 'mysql' ? (id: string) => `\`${id}\`` : (id: string) => `"${id}"`;
                    await trx.raw(`ALTER TABLE ${qt(sourceTable.name)} DROP CONSTRAINT ${qt(constraintName)}`);
                    this.logger.log(`Dropped FK constraint: ${constraintName} for column ${fkColumn}`);
                  } else {
                    this.logger.log(`No FK constraint found for column ${fkColumn}, skipping constraint drop`);
                  }
                } catch (error) {
                  this.logger.log(`Error dropping FK constraint: ${error.message}`);
                }

                try {
                  await trx.schema.alterTable(sourceTable.name, (table) => {
                    table.dropColumn(fkColumn);
                  });
                  this.logger.log(`Dropped FK column: ${fkColumn} from ${sourceTable.name}`);
                } catch (error) {
                  this.logger.log(`Error dropping FK column: ${error.message}`);
                }
              }
            }
          }
        }

        this.logger.log(`Checking for ALL FK constraints referencing table ${tableName}...`);

        try {
          const dbType = this.queryBuilder.getDatabaseType();
          let allFkConstraints;

          if (dbType === 'postgres') {
            const result = await trx.raw(`
              SELECT
                tc.table_name,
                kcu.column_name,
                tc.constraint_name,
                ccu.table_name AS referenced_table_name,
                ccu.column_name AS referenced_column_name
              FROM information_schema.table_constraints AS tc
              JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
              JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
              WHERE tc.constraint_type = 'FOREIGN KEY'
                AND tc.table_schema = 'public'
                AND ccu.table_name = ?
            `, [tableName]);

            allFkConstraints = result.rows || [];
          } else {
            const result = await trx.raw(`
              SELECT
                TABLE_NAME as table_name,
                COLUMN_NAME as column_name,
                CONSTRAINT_NAME as constraint_name,
                REFERENCED_TABLE_NAME as referenced_table_name,
                REFERENCED_COLUMN_NAME as referenced_column_name
              FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
              WHERE TABLE_SCHEMA = DATABASE()
              AND REFERENCED_TABLE_NAME = ?
              AND REFERENCED_COLUMN_NAME IS NOT NULL
            `, [tableName]);

            allFkConstraints = result[0] || [];
          }

          if (allFkConstraints && allFkConstraints.length > 0) {
            this.logger.log(`Found ${allFkConstraints.length} FK constraints referencing ${tableName}`);

            for (const fk of allFkConstraints) {
              this.logger.log(`Dropping FK constraint: ${fk.constraint_name} from ${fk.table_name}.${fk.column_name}`);

              try {
                const qt = dbType === 'mysql' ? (id: string) => `\`${id}\`` : (id: string) => `"${id}"`;
                await trx.raw(`ALTER TABLE ${qt(fk.table_name)} DROP CONSTRAINT ${qt(fk.constraint_name)}`);
                this.logger.log(`Dropped FK constraint: ${fk.constraint_name}`);
              } catch (error) {
                this.logger.log(`Error dropping FK constraint: ${error.message}`);
              }

              try {
                await trx.schema.alterTable(fk.table_name, (table: any) => {
                  table.dropColumn(fk.column_name);
                });
                this.logger.log(`Dropped FK column: ${fk.column_name} from ${fk.table_name}`);
              } catch (error) {
                this.logger.log(`Error dropping FK column: ${error.message}`);
              }
            }
          } else {
            this.logger.log(`No FK constraints found referencing ${tableName}`);
          }
        } catch (error) {
          this.logger.log(`Error checking FK constraints: ${error.message}`);
        }

        await trx('relation_definition')
          .where({ sourceTableId: id })
          .orWhere({ targetTableId: id })
          .delete();
        this.logger.log(`Deleted all relations for table ${id}`);

        await trx('column_definition')
          .where({ tableId: id })
          .delete();

        await trx('table_definition')
          .where({ id })
          .delete();

        await this.schemaMigrationService.dropTable(tableName, allRelations, trx);

        await trx.commit();

        this.logger.log(`Table deleted: ${tableName} (metadata + physical schema)`);
        return exists;
      } catch (error) {
        if (trx && !trx.isCompleted()) {
          try {
            await trx.rollback();
            this.logger.log(`Transaction rolled back due to error`);
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

  private async runWithSchemaLock<T>(context: string, handler: () => Promise<T>): Promise<T> {
    const lock = await this.schemaMigrationLockService.acquire(context);
    try {
      return await handler();
    } finally {
      await this.schemaMigrationLockService.release(lock);
    }
  }

  private async getFullTableMetadataInTransaction(trx: any, tableId: string | number): Promise<any> {
    const table = await trx('table_definition').where({ id: tableId }).first();
    if (!table) return null;

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

    table.columns = await trx('column_definition')
      .where({ tableId })
      .select('*');

    for (const col of table.columns) {
      if (col.defaultValue && typeof col.defaultValue === 'string') {
        try {
          col.defaultValue = JSON.parse(col.defaultValue);
        } catch (e) {
        }
      }
      if (col.options && typeof col.options === 'string') {
        try {
          col.options = JSON.parse(col.options);
        } catch (e) {
        }
      }
    }

    const relations = await trx('relation_definition')
      .where({ 'relation_definition.sourceTableId': tableId })
      .leftJoin('table_definition', 'relation_definition.targetTableId', 'table_definition.id')
      .select(
        'relation_definition.*',
        'table_definition.name as targetTableName'
      );

    for (const rel of relations) {
      rel.sourceTableName = table.name;

      if (!rel.targetTableName && rel.targetTableId) {
        const targetTable = await trx('table_definition').where({ id: rel.targetTableId }).first();
        if (targetTable) {
          rel.targetTableName = targetTable.name;
        } else {
          this.logger.error(`Relation ${rel.propertyName} (${rel.type}) has invalid targetTableId: ${rel.targetTableId} - table not found`);
        }
      }

      if (['many-to-one', 'one-to-one'].includes(rel.type)) {
        if (!rel.targetTableName) {
          throw new Error(`Relation '${rel.propertyName}' (${rel.type}) from table '${table.name}' has invalid targetTableId: ${rel.targetTableId}. Target table not found.`);
        }

        rel.foreignKeyColumn = getForeignKeyColumnName(rel.propertyName);
      }
    }

    table.relations = relations;

    return table;
  }
}

