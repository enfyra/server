import { Logger } from '../../../shared/logger';
import type { Knex } from 'knex';
import { getIoAbortSignal } from '@enfyra/kernel';
import {
  QueryBuilderService,
  getForeignKeyColumnName,
} from '@enfyra/kernel';
import {
  SqlSchemaMigrationService,
  SchemaMigrationLockService,
} from '../../../engines/knex';
import { MetadataCacheService } from '../../../engines/cache';
import {
  LoggingService,
  DatabaseException,
  DuplicateResourceException,
  ResourceNotFoundException,
  ValidationException,
} from '../../../domain/exceptions';
import {
  PolicyService,
  isPolicyDeny,
  isPolicyPreview,
} from '../../../domain/policy';
import { TDynamicContext } from '../../../shared/types';
import { validateUniquePropertyNames } from '../utils/duplicate-field-check';
import { TCreateTableBody } from '../types/table-handler.types';
import {
  getRelationTargetTableId,
  relationTargetTableMapKey,
} from '../utils/relation-target-id.util';
import { TableManagementValidationService } from './table-validation.service';
import { SqlTableMetadataBuilderService } from './sql-table-metadata-builder.service';
import { SqlTableMetadataWriterService } from './sql-table-metadata-writer.service';
import { getSqlJunctionPhysicalNames } from '../utils/sql-junction-naming.util';
import { ensureSqlTableRouteArtifacts } from './table-route-artifacts.service';
import {
  ensureSqlM2mJunctionTables,
  ensureSqlSingleRecord,
  syncSqlGqlDefinition,
} from './table-post-migration.service';
import { SqlTableHandlerService } from './sql-table-handler-base.service';

export class SqlTableCreateService extends SqlTableHandlerService {
  async createTable(body: TCreateTableBody, context?: TDynamicContext) {
    const decision = await this.policyService.checkSchemaMigration({
      operation: 'create',
      tableName: 'table_definition',
      data: body,
      currentUser: context?.$user,
    });
    if (isPolicyDeny(decision)) {
      throw new ValidationException(decision.message);
    }
    return await this.runWithSchemaLock(
      `table:create:${body?.name || 'unknown'}`,
      () => this.createTableInternal(body),
    );
  }
  private async createTableInternal(body: TCreateTableBody) {
    if (/[A-Z]/.test(body?.name)) {
      throw new ValidationException(
        'Table name must be lowercase (no uppercase letters).',
        {
          tableName: body?.name,
        },
      );
    }
    if (!/^[a-z0-9_]+$/.test(body?.name)) {
      throw new ValidationException(
        'Table name must be snake_case (a-z, 0-9, _).',
        {
          tableName: body?.name,
        },
      );
    }
    if (
      !body.columns ||
      !Array.isArray(body.columns) ||
      body.columns.length === 0
    ) {
      throw new ValidationException('Table must have at least one column.', {
        tableName: body?.name,
      });
    }
    const bodyRelations = body.relations ?? [];
    this.tableValidationService.validateRelations(bodyRelations);
    const knex = this.queryBuilderService.getKnex();
    let trx!: Knex.Transaction;
    let schemaCreated = false;
    let metadataCommitted = false;
    let createdMetadataSnapshot: any = null;
    try {
      trx = await knex.transaction();
      const abortSignal = getIoAbortSignal();
      if (abortSignal) {
        const onAbort = () => {
          if (trx && !trx.isCompleted()) {
            trx.rollback().catch(() => {});
          }
        };
        if (abortSignal.aborted) {
          await trx.rollback();
          throw new Error('Operation aborted');
        }
        abortSignal.addEventListener('abort', onAbort, { once: true });
      }
      const hasTable = await knex.schema.hasTable(body.name);
      const existing = await trx('table_definition')
        .where({ name: body.name })
        .first();
      if (existing) {
        await trx.rollback();
        throw new DuplicateResourceException(
          'table_definition',
          'name',
          body.name,
        );
      }
      if (hasTable && !existing) {
        this.logger.warn(
          `Mismatch detected: Physical table "${body.name}" exists but no metadata found. Dropping physical table...`,
        );
        try {
          await this.schemaMigrationService.dropTable(body.name, [], trx);
        } catch (dropError: any) {
          await trx.rollback();
          this.logger.error(
            `Failed to drop physical table "${body.name}": ${dropError.message}`,
          );
          throw new DatabaseException(
            `Failed to drop existing physical table "${body.name}": ${dropError.message}`,
            { tableName: body.name, operation: 'drop_existing_table' },
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
          { tableName: body.name },
        );
      }
      const validTypes = ['int', 'uuid'];
      if (!validTypes.includes(idCol.type)) {
        await trx.rollback();
        throw new ValidationException(
          `The primary column "id" must be of type int or uuid.`,
          { tableName: body.name, idColumnType: idCol.type },
        );
      }
      const primaryCount = body.columns.filter(
        (col: any) => col.isPrimary,
      ).length;
      if (primaryCount !== 1) {
        await trx.rollback();
        throw new ValidationException(
          `Only one column is allowed to have isPrimary = true.`,
          { tableName: body.name, primaryCount },
        );
      }
      try {
        validateUniquePropertyNames(body.columns || [], bodyRelations);
      } catch (error: any) {
        await trx.rollback();
        throw error;
      }
      const targetTableIds =
        bodyRelations
          .filter((rel: any) => rel.type === 'many-to-many')
          .map((rel: any) => getRelationTargetTableId(rel))
          ?.filter((id: any) => id != null) || [];
      const targetTablesMap = new Map<string, string>();
      if (targetTableIds.length > 0) {
        const targetTables = await trx('table_definition')
          .select('id', 'name')
          .whereIn('id', targetTableIds);
        for (const table of targetTables) {
          targetTablesMap.set(relationTargetTableMapKey(table.id), table.name);
        }
      }
      try {
        this.validateAllColumnsUnique(
          body.columns || [],
          bodyRelations,
          body.name,
          targetTablesMap,
        );
      } catch (error: any) {
        await trx.rollback();
        throw error;
      }
      body.isSystem = false;
      const dbType = this.queryBuilderService.getDatabaseType();
      const insertResult = await trx('table_definition').insert(
        {
          name: body.name,
          isSystem: body.isSystem,
          ...(body.isSingleRecord && { isSingleRecord: true }),
          alias: body.alias,
          description: body.description,
          uniques: JSON.stringify(body.uniques || []),
          indexes: JSON.stringify(body.indexes || []),
        },
        dbType === 'postgres' ? ['id'] : (undefined as any),
      );
      const tableId =
        dbType === 'postgres' ? insertResult[0]?.id : insertResult[0];
      if (body.columns?.length > 0) {
        const columnsToInsert = body.columns.map((col: any) => ({
          name: col.name,
          type: col.type,
          isPrimary: col.isPrimary || false,
          isGenerated: col.isGenerated || false,
          isNullable: col.isNullable ?? true,
          isSystem: col.isSystem || false,
          isUpdatable: col.isUpdatable ?? true,
          isPublished: col.isPublished ?? true,
          defaultValue:
            col.defaultValue !== undefined
              ? JSON.stringify(col.defaultValue)
              : null,
          options:
            col.options !== undefined ? JSON.stringify(col.options) : null,
          description: col.description,
          placeholder: col.placeholder,
          metadata:
            col.metadata !== undefined ? JSON.stringify(col.metadata) : null,
          tableId: tableId,
        }));
        await trx('column_definition').insert(columnsToInsert);
      }
      if (bodyRelations.length > 0) {
        const targetTableIds = bodyRelations
          .map((rel: any) => getRelationTargetTableId(rel))
          .filter((id: any) => id != null);
        const targetTablesMap = new Map<string, string>();
        if (targetTableIds.length > 0) {
          const targetTables = await trx('table_definition')
            .select('id', 'name')
            .whereIn('id', targetTableIds);
          for (const table of targetTables) {
            targetTablesMap.set(
              relationTargetTableMapKey(table.id),
              table.name,
            );
          }
        }
        const relationsToInsert: Array<{ insertData: any; rel: any }> = [];
        for (const rel of bodyRelations) {
          const targetTableId = getRelationTargetTableId(rel);
          let mappedById: number | null = null;
          if (rel.mappedBy) {
            const owningRel = await trx('relation_definition')
              .where({
                sourceTableId: targetTableId,
                propertyName: rel.mappedBy,
              })
              .select('id')
              .first();
            mappedById = owningRel?.id || null;
          }
          const insertData: any = {
            propertyName: rel.propertyName,
            type: rel.type,
            targetTableId,
            mappedById,
            isNullable: rel.isNullable ?? true,
            isSystem: rel.isSystem || false,
            isUpdatable: rel.isUpdatable ?? true,
            isPublished: rel.isPublished ?? true,
            onDelete: rel.onDelete || 'SET NULL',
            description: rel.description,
            sourceTableId: tableId,
          };
          if (rel.type === 'many-to-many') {
            const targetTableName = targetTablesMap.get(
              relationTargetTableMapKey(targetTableId),
            );
            if (!targetTableName) {
              throw new Error(
                `Target table with ID ${targetTableId} not found`,
              );
            }
            const junction = getSqlJunctionPhysicalNames({
              sourceTable: body.name,
              propertyName: rel.propertyName,
              targetTable: targetTableName,
            });
            insertData.junctionTableName = junction.junctionTableName;
            insertData.junctionSourceColumn = junction.junctionSourceColumn;
            insertData.junctionTargetColumn = junction.junctionTargetColumn;
          } else {
            insertData.junctionTableName = null;
            insertData.junctionSourceColumn = null;
            insertData.junctionTargetColumn = null;
          }
          relationsToInsert.push({ insertData, rel });
        }
        await trx('relation_definition').insert(
          relationsToInsert.map((r: any) => r.insertData),
        );
        for (const { insertData: inserted, rel } of relationsToInsert) {
          if (!rel.inversePropertyName) continue;
          if (rel.mappedBy) {
            throw new ValidationException(
              `Relation '${rel.propertyName}' cannot have both 'mappedBy' and 'inversePropertyName'`,
              { relationName: rel.propertyName },
            );
          }
          const targetTableId = inserted.targetTableId;
          const targetTableName = targetTablesMap.get(
            relationTargetTableMapKey(targetTableId),
          );
          const existingOnTarget = await trx('relation_definition')
            .where({
              sourceTableId: targetTableId,
              propertyName: rel.inversePropertyName,
            })
            .first();
          if (existingOnTarget) {
            throw new ValidationException(
              `Cannot create inverse '${rel.inversePropertyName}' on '${targetTableName}': property name already exists`,
              {
                relationName: rel.inversePropertyName,
                targetTable: targetTableName,
              },
            );
          }
          const owningRel = await trx('relation_definition')
            .where({
              sourceTableId: tableId,
              propertyName: rel.propertyName,
            })
            .first();
          if (!owningRel) continue;
          const existingInverse = await trx('relation_definition')
            .where({ mappedById: owningRel.id })
            .first();
          if (existingInverse) {
            throw new ValidationException(
              `Relation '${rel.propertyName}' already has an inverse '${existingInverse.propertyName}'`,
              { relationName: rel.propertyName },
            );
          }
          let inverseType = rel.type;
          if (rel.type === 'many-to-one') inverseType = 'one-to-many';
          else if (rel.type === 'one-to-many') inverseType = 'many-to-one';
          const inverseData: any = {
            propertyName: rel.inversePropertyName,
            type: inverseType,
            sourceTableId: targetTableId,
            targetTableId: tableId,
            mappedById: owningRel.id,
            isNullable: rel.isNullable ?? true,
            isSystem: rel.isSystem || false,
            isUpdatable: rel.isUpdatable ?? true,
            isPublished: rel.isPublished ?? true,
            onDelete: rel.onDelete || 'SET NULL',
          };
          if (inverseType === 'many-to-many') {
            inverseData.junctionTableName = inserted.junctionTableName;
            inverseData.junctionSourceColumn = inserted.junctionTargetColumn;
            inverseData.junctionTargetColumn = inserted.junctionSourceColumn;
          }
          await trx('relation_definition').insert(inverseData);
          this.logger.log(
            `Auto-created inverse relation '${rel.inversePropertyName}' on '${targetTableName}'`,
          );
        }
      }
      await ensureSqlTableRouteArtifacts({
        trx,
        metadataCacheService: this.metadataCacheService,
        tableName: body.name,
        tableId,
        logger: this.logger,
      });
      const fullMetadata =
        await this.sqlTableMetadataBuilderService.getFullTableMetadataInTransaction(
          trx,
          tableId,
        );
      if (!fullMetadata) {
        throw new Error(`Failed to fetch metadata for table ${body.name}`);
      }
      if (fullMetadata.relations) {
        for (const rel of fullMetadata.relations) {
          if (['many-to-one', 'one-to-one'].includes(rel.type)) {
            if (!rel.targetTableName) {
              throw new Error(
                `Relation '${rel.propertyName}' (${rel.type}) from table '${body.name}' has invalid targetTableId: ${rel.targetTableId}. Target table not found. Please verify the target table ID is correct.`,
              );
            }
          }
        }
      }
      const createResult = await this.schemaMigrationService.createTable(
        fullMetadata,
      );
      const resolvedAutoGeneratedIndexes =
        createResult.autoGeneratedIndexes;
      schemaCreated = true;
      createdMetadataSnapshot = fullMetadata;
      if (!trx.isCompleted()) {
        await trx.commit();
        metadataCommitted = true;
      }

      await this.schemaMigrationService.updateMetadataIndexesForTable(
        body.name,
        fullMetadata.indexes || [],
        resolvedAutoGeneratedIndexes,
      );

      if (body.isSingleRecord) {
        await ensureSqlSingleRecord({
          knex: this.queryBuilderService.getKnex(),
          tableName: body.name,
          columns: fullMetadata.columns || [],
        });
      }
      const affectedTables: string[] = [];
      if (fullMetadata.relations) {
        for (const rel of fullMetadata.relations) {
          if (rel.targetTableName && rel.targetTableName !== body.name) {
            affectedTables.push(rel.targetTableName);
          }
        }
      }
      fullMetadata.affectedTables = [...new Set(affectedTables)];
      return fullMetadata;
    } catch (error: any) {
      if (trx && !trx.isCompleted()) {
        try {
          await trx.rollback();
        } catch (rollbackError: any) {
          this.logger.error(
            `Failed to rollback transaction: ${rollbackError.message}`,
          );
        }
      }
      if (schemaCreated) {
        try {
          await this.schemaMigrationService.dropTable(
            body.name,
            createdMetadataSnapshot?.relations || bodyRelations,
          );
          this.logger.warn(
            `Rolled back physical table ${body.name} after failure`,
          );
        } catch (dropError: any) {
          this.logger.error(
            `Failed to rollback physical table ${body.name}: ${dropError.message}`,
          );
        }
      }
      if (metadataCommitted && !schemaCreated) {
        try {
          await this.queryBuilderService
            .getKnex()('table_definition')
            .where({ name: body.name })
            .delete();
        } catch (metadataCleanupError: any) {
          this.logger.error(
            `Failed to rollback metadata for ${body.name}: ${metadataCleanupError.message}`,
          );
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

}
