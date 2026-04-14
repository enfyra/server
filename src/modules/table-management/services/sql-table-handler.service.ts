import { Injectable, Logger } from '@nestjs/common';
import { getIoAbortSignal } from '../../../infrastructure/executor-engine/services/isolated-executor.service';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { SqlSchemaMigrationService } from '../../../infrastructure/knex/services/sql-schema-migration.service';
import { SchemaMigrationLockService } from '../../../infrastructure/knex/services/schema-migration-lock.service';
import { MetadataCacheService } from '../../../infrastructure/cache/services/metadata-cache.service';
import { LoggingService } from '../../../core/exceptions/services/logging.service';
import { PolicyService } from '../../../core/policy/policy.service';
import { TDynamicContext } from '../../../shared/types';
import {
  isPolicyDeny,
  isPolicyPreview,
} from '../../../core/policy/policy.types';
import {
  DatabaseException,
  DuplicateResourceException,
  ResourceNotFoundException,
  ValidationException,
} from '../../../core/exceptions/custom-exceptions';
import { validateUniquePropertyNames } from '../utils/duplicate-field-check';
import { getDeletedIds } from '../utils/get-deleted-ids';
import { CreateTableDto } from '../dto/create-table.dto';
import {
  getForeignKeyColumnName,
  getJunctionTableName,
  getJunctionColumnNames,
} from '../../../infrastructure/knex/utils/sql-schema-naming.util';
import { generateDefaultRecord } from '../utils/generate-default-record';
import { DEFAULT_REST_HANDLER_LOGIC } from '../../../core/bootstrap/utils/canonical-table-route.util';
@Injectable()
export class SqlTableHandlerService {
  private logger = new Logger(SqlTableHandlerService.name);
  constructor(
    private queryBuilder: QueryBuilderService,
    private schemaMigrationService: SqlSchemaMigrationService,
    private metadataCacheService: MetadataCacheService,
    private loggingService: LoggingService,
    private schemaMigrationLockService: SchemaMigrationLockService,
    private policyService: PolicyService,
  ) {}
  private validateRelations(relations: any[]) {
    for (const relation of relations || []) {
      if (relation.type === 'one-to-many' && !relation.mappedBy) {
        throw new ValidationException(
          `One-to-many relation '${relation.propertyName}' must have mappedBy`,
          {
            relationName: relation.propertyName,
            relationType: relation.type,
            missingField: 'mappedBy',
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
      const targetTableId =
        typeof rel.targetTable === 'object'
          ? rel.targetTable.id
          : rel.targetTable;
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
            if (rel.mappedBy) {
              builder.where({ propertyName: rel.mappedBy });
            }
            builder.orWhereIn(
              'mappedById',
              trx('relation_definition')
                .select('id')
                .where({
                  sourceTableId: sourceTableId,
                  propertyName: rel.propertyName,
                }),
            );
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
        if (!rel.mappedBy) continue;
        const targetRelations = await trx('relation_definition')
          .where({ sourceTableId: targetTableId })
          .where({ targetTableId: sourceTableId })
          .where({ propertyName: rel.mappedBy })
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
        if (!rel.mappedBy) continue;
        const targetRelations = await trx('relation_definition')
          .where({ sourceTableId: targetTableId })
          .where({ targetTableId: sourceTableId })
          .where({ propertyName: rel.mappedBy })
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
  private validateAllColumnsUnique(
    columns: any[],
    relations: any[],
    tableName: string,
    targetTablesMap: Map<number, string>,
  ) {
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
        const fkColumn = getForeignKeyColumnName(rel.propertyName);
        if (allColumnNames.has(fkColumn)) {
          duplicates.push(`${fkColumn} (FK for ${rel.propertyName})`);
        }
        allColumnNames.add(fkColumn);
      }
    }
    for (const rel of relations || []) {
      if (rel.type === 'many-to-many') {
        const targetTableId =
          typeof rel.targetTable === 'object'
            ? rel.targetTable.id
            : rel.targetTable;
        const targetTableName = targetTablesMap.get(targetTableId);
        if (targetTableName) {
          const { sourceColumn, targetColumn } = getJunctionColumnNames(
            tableName,
            rel.propertyName,
            targetTableName,
          );
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
              },
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
          suggestion:
            'Rename columns or relations to ensure all column names are unique.',
        },
      );
    }
  }
  async createTable(body: CreateTableDto, context?: TDynamicContext) {
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
  private async createTableInternal(body: CreateTableDto) {
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
    this.validateRelations(body.relations);
    const knex = this.queryBuilder.getKnex();
    let trx;
    let schemaCreated = false;
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
        } catch (dropError) {
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
        validateUniquePropertyNames(body.columns || [], body.relations || []);
      } catch (error) {
        await trx.rollback();
        throw error;
      }
      const targetTableIds =
        body.relations
          ?.filter((rel: any) => rel.type === 'many-to-many')
          ?.map((rel: any) =>
            typeof rel.targetTable === 'object'
              ? rel.targetTable.id
              : rel.targetTable,
          )
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
        this.validateAllColumnsUnique(
          body.columns || [],
          body.relations || [],
          body.name,
          targetTablesMap,
        );
      } catch (error) {
        await trx.rollback();
        throw error;
      }
      body.isSystem = false;
      const dbType = this.queryBuilder.getDatabaseType();
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
        dbType === 'postgres' ? ['id'] : undefined,
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
      if (body.relations?.length > 0) {
        const targetTableIds = body.relations
          .map((rel: any) =>
            typeof rel.targetTable === 'object'
              ? rel.targetTable.id
              : rel.targetTable,
          )
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
          const targetTableId =
            typeof rel.targetTable === 'object'
              ? rel.targetTable.id
              : rel.targetTable;
          let mappedById: number | null = null;
          if (rel.mappedBy) {
            const owningRel = await trx('relation_definition')
              .where({ sourceTableId: targetTableId, propertyName: rel.mappedBy })
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
            description: rel.description,
            sourceTableId: tableId,
          };
          if (rel.type === 'many-to-many') {
            const targetTableName = targetTablesMap.get(targetTableId);
            if (!targetTableName) {
              throw new Error(
                `Target table with ID ${targetTableId} not found`,
              );
            }
            const junctionTableName = getJunctionTableName(
              body.name,
              rel.propertyName,
              targetTableName,
            );
            const { sourceColumn, targetColumn } = getJunctionColumnNames(
              body.name,
              rel.propertyName,
              targetTableName,
            );
            insertData.junctionTableName = junctionTableName;
            insertData.junctionSourceColumn = sourceColumn;
            insertData.junctionTargetColumn = targetColumn;
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
          const targetTableName = targetTablesMap.get(targetTableId);
          const existingOnTarget = await trx('relation_definition')
            .where({
              sourceTableId: targetTableId,
              propertyName: rel.inversePropertyName,
            })
            .first();
          if (existingOnTarget) {
            throw new ValidationException(
              `Cannot create inverse '${rel.inversePropertyName}' on '${targetTableName}': property name already exists`,
              { relationName: rel.inversePropertyName, targetTable: targetTableName },
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
        const newRoute = await trx('route_definition')
          .where({ path: `/${body.name}` })
          .first();
        if (newRoute?.id) {
          const methods = await trx('method_definition').select('id', 'method');
          const routeTableMeta =
            await this.metadataCacheService.getTableMetadata(
              'route_definition',
            );
          const availableMethodsRel = routeTableMeta?.relations?.find(
            (r: any) => r.propertyName === 'availableMethods',
          );
          if (availableMethodsRel?.junctionTableName && methods?.length > 0) {
            await trx(availableMethodsRel.junctionTableName).insert(
              methods.map((m: any) => ({
                [availableMethodsRel.junctionSourceColumn]: newRoute.id,
                [availableMethodsRel.junctionTargetColumn]: m.id,
              })),
            );
          }
          const httpMethods = methods.filter((m: any) =>
            ['GET', 'POST', 'PATCH', 'DELETE'].includes(m.method),
          );
          if (httpMethods.length > 0) {
            await trx('route_handler_definition').insert(
              httpMethods.map((m: any) => ({
                routeId: newRoute.id,
                methodId: m.id,
                logic: DEFAULT_REST_HANDLER_LOGIC[m.method] || null,
                timeout: 30000,
              })),
            );
          }
        }
      } else {
        this.logger.warn(
          `Route /${body.name} already exists, skipping route creation`,
        );
      }
      const fullMetadata = await this.getFullTableMetadataInTransaction(
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
      const { autoGeneratedIndexes } =
        await this.schemaMigrationService.createTable(fullMetadata);
      schemaCreated = true;
      createdMetadataSnapshot = fullMetadata;
      await trx.commit();

      await this.schemaMigrationService.updateMetadataIndexesForTable(
        body.name,
        fullMetadata.indexes || [],
        autoGeneratedIndexes,
      );

      if (body.isSingleRecord) {
        const knex = this.queryBuilder.getKnex();
        const existingRecord = await knex(body.name).first();
        if (!existingRecord) {
          const defaultRecord = generateDefaultRecord(
            fullMetadata.columns || [],
          );
          await knex(body.name).insert(defaultRecord);
        } else {
        }
      }

      if (body.isSingleRecord) {
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
    } catch (error) {
      if (trx && !trx.isCompleted()) {
        try {
          await trx.rollback();
        } catch (rollbackError) {
          this.logger.error(
            `Failed to rollback transaction: ${rollbackError.message}`,
          );
        }
      }
      if (schemaCreated) {
        try {
          await this.schemaMigrationService.dropTable(
            body.name,
            createdMetadataSnapshot?.relations || body.relations || [],
          );
          this.logger.warn(
            `Rolled back physical table ${body.name} after failure`,
          );
        } catch (dropError) {
          this.logger.error(
            `Failed to rollback physical table ${body.name}: ${dropError.message}`,
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
  async updateTable(
    id: string | number,
    body: CreateTableDto,
    context?: TDynamicContext,
  ) {
    const t0 = Date.now();
    this.logger.log(`[updateTable:${id}] STEP 0 acquiring schema lock`);
    const result = await this.runWithSchemaLock(`table:update:${id}`, () => {
      this.logger.log(
        `[updateTable:${id}] STEP 1 lock acquired (+${Date.now() - t0}ms) → calling updateTableInternal`,
      );
      return this.updateTableInternal(id, body, context);
    });
    this.logger.log(`[updateTable:${id}] STEP DONE total=${Date.now() - t0}ms`);
    return result;
  }
  private async updateTableInternal(
    id: string | number,
    body: CreateTableDto,
    context?: TDynamicContext,
  ) {
    const tag = `[updateTable:${id}]`;
    const stepLog = (msg: string) => this.logger.log(`${tag} ${msg}`);
    let t = Date.now();
    const lap = () => {
      const e = Date.now() - t;
      t = Date.now();
      return e;
    };
    const knex = this.queryBuilder.getKnex();
    const dbType = this.queryBuilder.getDatabaseType();
    const isPostgres = dbType === 'postgres';
    const affectedTableNames = new Set<string>();

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
    stepLog(`STEP 2 name+relation validate done (+${lap()}ms)`);

    try {
      // === VALIDATION PHASE (read-only, no transaction) ===
      const exists = await knex('table_definition').where({ id }).first();
      stepLog(`STEP 3 fetched table_definition row (+${lap()}ms)`);
      if (!exists) {
        throw new ResourceNotFoundException('table_definition', String(id));
      }
      validateUniquePropertyNames(body.columns || [], body.relations || []);

      const m2mTargetTableIds =
        body.relations
          ?.filter((rel: any) => rel.type === 'many-to-many')
          ?.map((rel: any) =>
            typeof rel.targetTable === 'object'
              ? rel.targetTable.id
              : rel.targetTable,
          )
          ?.filter((tid: any) => tid != null) || [];
      const m2mTargetTablesMap = new Map<number, string>();
      if (m2mTargetTableIds.length > 0) {
        const targetTables = await knex('table_definition')
          .select('id', 'name')
          .whereIn('id', m2mTargetTableIds);
        for (const table of targetTables) {
          m2mTargetTablesMap.set(table.id, table.name);
        }
      }
      this.validateAllColumnsUnique(
        body.columns || [],
        body.relations || [],
        exists.name,
        m2mTargetTablesMap,
      );
      const newRelations =
        body.relations?.filter((rel: any) => !rel.id) || [];
      if (newRelations.length > 0) {
        await this.validateNoDuplicateInverseRelation(
          knex,
          Number(id),
          exists.name,
          newRelations,
          m2mTargetTablesMap,
        );
      }
      stepLog(`STEP 4 validators done (+${lap()}ms)`);

      const oldMetadata = await this.getFullTableMetadataInTransaction(
        knex,
        exists.id,
      );
      stepLog(`STEP 5 loaded oldMetadata (+${lap()}ms)`);

      // Compute full target tables map (all relation targets)
      const allTargetTableIds =
        body.relations
          ?.map((rel: any) =>
            typeof rel.targetTable === 'object'
              ? rel.targetTable.id
              : rel.targetTable,
          )
          ?.filter((tid: any) => tid != null) || [];
      const allTargetTablesMap = new Map<number, string>();
      if (allTargetTableIds.length > 0) {
        const targetTables = await knex('table_definition')
          .select('id', 'name')
          .whereIn('id', allTargetTableIds);
        for (const table of targetTables) {
          allTargetTablesMap.set(table.id, table.name);
        }
      }

      if (isPostgres) {
        // === PG PATH: metadata writes + DDL in same transaction ===
        stepLog(`STEP 6 PG: opening transaction...`);
        const trx = await knex.transaction();
        const abortSignal = getIoAbortSignal();
        if (abortSignal) {
          const onAbort = () => {
            if (trx && !trx.isCompleted()) trx.rollback().catch(() => {});
          };
          if (abortSignal.aborted) {
            await trx.rollback();
            throw new Error('Operation aborted');
          }
          abortSignal.addEventListener('abort', onAbort, { once: true });
        }
        try {
          stepLog(`STEP 7 PG: writing metadata in trx...`);
          await this.writeTableMetadataUpdates(
            trx, id, body, exists, affectedTableNames,
          );
          stepLog(`STEP 8 PG: metadata written (+${lap()}ms)`);

          const updatedFullMetadata =
            await this.getFullTableMetadataInTransaction(trx, exists.id);
          stepLog(`STEP 9 PG: reloaded metadata in trx (+${lap()}ms)`);
          if (!updatedFullMetadata) {
            throw new Error(
              `Failed to reload metadata for table ${exists.name}`,
            );
          }

          const decision = await this.policyService.checkSchemaMigration({
            operation: 'update',
            tableName: exists.name,
            data: body,
            currentUser: context?.$user,
            beforeMetadata: oldMetadata,
            afterMetadata: updatedFullMetadata,
            requestContext: context,
          });
          stepLog(`STEP 10 PG: policy checked (+${lap()}ms)`);
          if (isPolicyPreview(decision)) {
            await trx.rollback();
            return { _preview: true, ...decision.details };
          }
          if (isPolicyDeny(decision)) {
            throw new ValidationException(decision.message, decision.details);
          }

          const schemaChanged = decision.details?.schemaChanged === true;
          if (schemaChanged) {
            stepLog(`STEP 11 PG: running DDL inside metadata transaction...`);
            const ddlTimeoutMs = 90 * 1000;
            const pendingUpdate: any = await Promise.race([
              this.schemaMigrationService.updateTable(
                exists.name, oldMetadata, updatedFullMetadata, trx,
              ),
              new Promise<never>((_, reject) =>
                setTimeout(
                  () => reject(new Error(`DDL timed out after ${ddlTimeoutMs}ms`)),
                  ddlTimeoutMs,
                ),
              ),
            ]);
            stepLog(`STEP 11 PG: DDL done (+${lap()}ms)`);
            if (pendingUpdate?.pendingMetadataUpdate) {
              await this.schemaMigrationService.applyPendingMetadataUpdate(
                pendingUpdate.pendingMetadataUpdate, trx,
              );
            }
          }

          stepLog(`STEP 12 PG: committing metadata + DDL transaction...`);
          await trx.commit();
          stepLog(`STEP 12 PG: committed (+${lap()}ms)`);
        } catch (innerError) {
          if (trx && !trx.isCompleted()) {
            try { await trx.rollback(); } catch (_) {}
          }
          throw innerError;
        }
      } else {
        // === MYSQL PATH: DDL first, then metadata writes ===
        stepLog(`STEP 6 ${dbType}: constructing afterMetadata from body...`);
        const afterMetadata = this.constructAfterMetadata(
          exists, body, oldMetadata, allTargetTablesMap,
        );
        stepLog(`STEP 7 ${dbType}: afterMetadata constructed (+${lap()}ms)`);

        const decision = await this.policyService.checkSchemaMigration({
          operation: 'update',
          tableName: exists.name,
          data: body,
          currentUser: context?.$user,
          beforeMetadata: oldMetadata,
          afterMetadata,
          requestContext: context,
        });
        stepLog(`STEP 8 ${dbType}: policy checked (+${lap()}ms)`);
        if (isPolicyPreview(decision)) {
          return { _preview: true, ...decision.details };
        }
        if (isPolicyDeny(decision)) {
          throw new ValidationException(decision.message, decision.details);
        }

        const schemaChanged = decision.details?.schemaChanged === true;

        if (schemaChanged) {
          stepLog(`STEP 9 ${dbType}: running DDL before metadata...`);
          const ddlTimeoutMs = 90 * 1000;
          let pendingUpdate: any;
          try {
            pendingUpdate = await Promise.race([
              this.schemaMigrationService.updateTable(
                exists.name, oldMetadata, afterMetadata,
              ),
              new Promise<never>((_, reject) =>
                setTimeout(
                  () => reject(new Error(`DDL timed out after ${ddlTimeoutMs}ms`)),
                  ddlTimeoutMs,
                ),
              ),
            ]);
            stepLog(`STEP 9 ${dbType}: DDL done (+${lap()}ms)`);
          } catch (ddlError) {
            stepLog(`STEP 9 ${dbType}: DDL FAILED, metadata not saved`);
            throw ddlError;
          }

          stepLog(`STEP 10 ${dbType}: writing metadata after DDL...`);
          const trx = await knex.transaction();
          try {
            await this.writeTableMetadataUpdates(
              trx, id, body, exists, affectedTableNames,
            );
            await trx.commit();
            stepLog(`STEP 10 ${dbType}: metadata committed (+${lap()}ms)`);
          } catch (metadataError) {
            if (trx && !trx.isCompleted()) {
              try { await trx.rollback(); } catch (_) {}
            }
            stepLog(`STEP 10 ${dbType}: metadata write FAILED after DDL succeeded`);
            throw metadataError;
          }

          if (pendingUpdate?.pendingMetadataUpdate) {
            await this.schemaMigrationService.applyPendingMetadataUpdate(
              pendingUpdate.pendingMetadataUpdate,
            );
            stepLog(`STEP 11 ${dbType}: applied pending metadata update (+${lap()}ms)`);
          }
        } else {
          stepLog(`STEP 9 ${dbType}: no schema change, writing metadata only...`);
          const trx = await knex.transaction();
          try {
            await this.writeTableMetadataUpdates(
              trx, id, body, exists, affectedTableNames,
            );
            await trx.commit();
            stepLog(`STEP 9 ${dbType}: metadata committed (+${lap()}ms)`);
          } catch (metadataError) {
            if (trx && !trx.isCompleted()) {
              try { await trx.rollback(); } catch (_) {}
            }
            throw metadataError;
          }
        }
      }

      // === POST-MIGRATION (common) ===
      if (body.isSingleRecord === true && !exists.isSingleRecord) {
        const recordCount = await knex(exists.name)
          .count('* as count')
          .first();
        const count = Number(recordCount?.count || 0);
        if (count === 0) {
          const fullMetadata = await this.getFullTableMetadataInTransaction(
            knex, exists.id,
          );
          const defaultRecord = generateDefaultRecord(fullMetadata?.columns || []);
          await knex(exists.name).insert(defaultRecord);
        } else if (count > 1) {
          const firstRecord = await knex(exists.name)
            .orderBy('id', 'asc')
            .first('id');
          await knex(exists.name).where('id', '!=', firstRecord.id).delete();
        }
      }

      if (body.graphqlEnabled !== undefined) {
        const existingGql = await knex('gql_definition')
          .where({ tableId: exists.id })
          .first();
        if (existingGql) {
          await knex('gql_definition')
            .where({ id: existingGql.id })
            .update({ isEnabled: body.graphqlEnabled === true });
        } else {
          await knex('gql_definition').insert({
            tableId: exists.id,
            isEnabled: body.graphqlEnabled === true,
            isSystem: exists.isSystem || false,
          });
        }
        stepLog(`gql_definition sync done (+${lap()}ms)`);
      }

      return { id: exists.id, name: exists.name, affectedTables: [...affectedTableNames] };
    } catch (error) {
      this.loggingService.error('Table update failed', {
        context: 'updateTable',
        error: error.message,
        stack: error.stack,
        tableId: id,
        tableName: body?.name,
      });
      throw new DatabaseException(`Failed to update table: ${error.message}`, {
        tableId: id,
        operation: 'update',
      });
    }
  }
  async delete(id: string | number, context?: TDynamicContext) {
    return await this.runWithSchemaLock(`table:delete:${id}`, () =>
      this.deleteTableInternal(id, context),
    );
  }
  private async deleteTableInternal(
    id: string | number,
    context?: TDynamicContext,
  ) {
    const knex = this.queryBuilder.getKnex();
    const affectedTableNames = new Set<string>();
    return await knex.transaction(async (trx) => {
      const abortSignal = getIoAbortSignal();
      if (abortSignal) {
        const onAbort = () => {
          if (!trx.isCompleted()) trx.rollback().catch(() => {});
        };
        if (abortSignal.aborted) throw new Error('Operation aborted');
        abortSignal.addEventListener('abort', onAbort, { once: true });
      }
      try {
        const exists = await trx('table_definition').where({ id }).first();
        if (!exists) {
          throw new ResourceNotFoundException('table_definition', String(id));
        }
        if (exists.isSystem) {
          throw new ValidationException('Cannot delete system table', {
            tableId: id,
            tableName: exists.name,
          });
        }
        const tableName = exists.name;
        const decision = await this.policyService.checkSchemaMigration({
          operation: 'delete',
          tableName,
          currentUser: context?.$user,
          requestContext: context,
        });
        if (isPolicyDeny(decision)) {
          throw new ValidationException(decision.message, decision.details);
        }
        const allRelations = await trx('relation_definition')
          .where({ sourceTableId: id })
          .orWhere({ targetTableId: id })
          .select('*');
        const targetRelations = await trx('relation_definition')
          .where({ targetTableId: id })
          .select('*');
        for (const rel of targetRelations) {
          if (['one-to-many', 'many-to-one', 'one-to-one'].includes(rel.type)) {
            const sourceTable = await trx('table_definition')
              .where({ id: rel.sourceTableId })
              .first();
            if (sourceTable) {
              const { getForeignKeyColumnName } =
                await import('../../../infrastructure/knex/utils/sql-schema-naming.util');
              const fkColumn = getForeignKeyColumnName(tableName);
              const columnExists = await trx.schema.hasColumn(
                sourceTable.name,
                fkColumn,
              );
              if (columnExists) {
                try {
                  const dbType = this.queryBuilder.getDatabaseType();
                  let constraintName: string | null = null;
                  if (dbType === 'postgres') {
                    const result = await trx.raw(
                      `
                      SELECT tc.constraint_name
                      FROM information_schema.table_constraints AS tc
                      JOIN information_schema.key_column_usage AS kcu
                        ON tc.constraint_name = kcu.constraint_name
                        AND tc.table_schema = kcu.table_schema
                      WHERE tc.constraint_type = 'FOREIGN KEY'
                        AND tc.table_schema = 'public'
                        AND tc.table_name = ?
                        AND kcu.column_name = ?
                    `,
                      [sourceTable.name, fkColumn],
                    );
                    constraintName = result.rows[0]?.constraint_name || null;
                  } else if (dbType === 'mysql') {
                    const result = await trx.raw(
                      `
                      SELECT CONSTRAINT_NAME as constraint_name
                      FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                      WHERE TABLE_SCHEMA = DATABASE()
                        AND TABLE_NAME = ?
                        AND COLUMN_NAME = ?
                        AND REFERENCED_TABLE_NAME IS NOT NULL
                    `,
                      [sourceTable.name, fkColumn],
                    );
                    constraintName = result[0][0]?.constraint_name || null;
                  }
                  if (constraintName) {
                    const qt =
                      dbType === 'mysql'
                        ? (id: string) => `\`${id}\``
                        : (id: string) => `"${id}"`;
                    await trx.raw(
                      `ALTER TABLE ${qt(sourceTable.name)} DROP CONSTRAINT ${qt(constraintName)}`,
                    );
                  } else {
                  }
                } catch (error) {}
                try {
                  await trx.schema.alterTable(sourceTable.name, (table) => {
                    table.dropColumn(fkColumn);
                  });
                } catch (error) {}
              }
            }
          }
        }
        try {
          const dbType = this.queryBuilder.getDatabaseType();
          let allFkConstraints;
          if (dbType === 'postgres') {
            const result = await trx.raw(
              `
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
            `,
              [tableName],
            );
            allFkConstraints = result.rows || [];
          } else {
            const result = await trx.raw(
              `
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
            `,
              [tableName],
            );
            allFkConstraints = result[0] || [];
          }
          if (allFkConstraints && allFkConstraints.length > 0) {
            for (const fk of allFkConstraints) {
              try {
                const qt =
                  dbType === 'mysql'
                    ? (id: string) => `\`${id}\``
                    : (id: string) => `"${id}"`;
                await trx.raw(
                  `ALTER TABLE ${qt(fk.table_name)} DROP CONSTRAINT ${qt(fk.constraint_name)}`,
                );
              } catch (error) {}
              try {
                await trx.schema.alterTable(fk.table_name, (table: any) => {
                  table.dropColumn(fk.column_name);
                });
              } catch (error) {}
            }
          } else {
          }
        } catch (error) {}
        for (const rel of targetRelations) {
          const sourceTable = await trx('table_definition')
            .where({ id: rel.sourceTableId })
            .select('name')
            .first();
          if (sourceTable?.name) affectedTableNames.add(sourceTable.name);
        }
        await trx('relation_definition').where({ targetTableId: id }).delete();
        await trx('table_definition').where({ id }).delete();
        await this.schemaMigrationService.dropTable(
          tableName,
          allRelations,
          trx,
        );
        await trx.commit();
        exists.affectedTables = [...affectedTableNames];
        return exists;
      } catch (error) {
        if (trx && !trx.isCompleted()) {
          try {
            await trx.rollback();
          } catch (rollbackError) {
            this.logger.error(
              `Failed to rollback transaction: ${rollbackError.message}`,
            );
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
  private async runWithSchemaLock<T>(
    context: string,
    handler: () => Promise<T>,
  ): Promise<T> {
    const lock = await this.schemaMigrationLockService.acquire(context);
    try {
      return await handler();
    } finally {
      await this.schemaMigrationLockService.release(lock);
    }
  }
  private async getFullTableMetadataInTransaction(
    trx: any,
    tableId: string | number,
  ): Promise<any> {
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
        } catch (e) {}
      }
      if (col.options && typeof col.options === 'string') {
        try {
          col.options = JSON.parse(col.options);
        } catch (e) {}
      }
    }
    const relations = await trx('relation_definition')
      .where({ 'relation_definition.sourceTableId': tableId })
      .leftJoin(
        'table_definition',
        'relation_definition.targetTableId',
        'table_definition.id',
      )
      .select(
        'relation_definition.*',
        'table_definition.name as targetTableName',
      );
    for (const rel of relations) {
      rel.sourceTableName = table.name;
      if (!rel.targetTableName && rel.targetTableId) {
        const targetTable = await trx('table_definition')
          .where({ id: rel.targetTableId })
          .first();
        if (targetTable) {
          rel.targetTableName = targetTable.name;
        } else {
          this.logger.error(
            `Relation ${rel.propertyName} (${rel.type}) has invalid targetTableId: ${rel.targetTableId} - table not found`,
          );
        }
      }
      if (['many-to-one', 'one-to-one'].includes(rel.type)) {
        if (!rel.targetTableName) {
          throw new Error(
            `Relation '${rel.propertyName}' (${rel.type}) from table '${table.name}' has invalid targetTableId: ${rel.targetTableId}. Target table not found.`,
          );
        }
        rel.foreignKeyColumn = getForeignKeyColumnName(rel.propertyName);
      }
    }
    table.relations = relations;
    return table;
  }

  private constructAfterMetadata(
    exists: any,
    body: CreateTableDto,
    oldMetadata: any,
    targetTablesMap: Map<number, string>,
  ): any {
    const metadata: any = {
      name: body.name ?? exists.name,
      uniques: body.uniques ?? oldMetadata?.uniques,
      indexes: body.indexes ?? oldMetadata?.indexes,
    };

    metadata.columns = (body.columns ?? oldMetadata?.columns ?? []).map(
      (col: any) => ({
        id: col.id,
        name: col.name,
        type: col.type,
        isPrimary: col.isPrimary || false,
        isGenerated: col.isGenerated || false,
        isNullable: col.isNullable ?? true,
        isSystem: col.isSystem || false,
        isUpdatable: col.isUpdatable ?? true,
        isPublished: col.isPublished ?? true,
        defaultValue: col.defaultValue ?? null,
        tableId: exists.id,
      }),
    );

    metadata.relations = (
      body.relations ?? oldMetadata?.relations ?? []
    ).map((rel: any) => {
      const targetTableId =
        typeof rel.targetTable === 'object' ? rel.targetTable.id : rel.targetTable;
      const targetTableName =
        targetTablesMap.get(targetTableId) ??
        oldMetadata?.relations?.find((r: any) => r.id === rel.id)
          ?.targetTableName ??
        '';
      const relation: any = {
        id: rel.id,
        propertyName: rel.propertyName,
        type: rel.type,
        targetTableId,
        targetTableName,
        sourceTableName: exists.name,
        mappedBy: rel.mappedBy || null,
        mappedById: null,
        isNullable: rel.isNullable ?? true,
        isSystem: rel.isSystem || false,
        isUpdatable: rel.isUpdatable ?? true,
        isPublished: rel.isPublished ?? true,
      };

      if (['many-to-one', 'one-to-one'].includes(rel.type)) {
        relation.foreignKeyColumn = getForeignKeyColumnName(rel.propertyName);
      }

      if (rel.type === 'many-to-many') {
        if (rel.id) {
          const existingRel = oldMetadata?.relations?.find(
            (r: any) => r.id === rel.id,
          );
          if (existingRel?.junctionTableName) {
            relation.junctionTableName = existingRel.junctionTableName;
            relation.junctionSourceColumn = existingRel.junctionSourceColumn;
            relation.junctionTargetColumn = existingRel.junctionTargetColumn;
          }
        }
        if (!relation.junctionTableName && targetTableName) {
          relation.junctionTableName = getJunctionTableName(
            exists.name,
            rel.propertyName,
            targetTableName,
          );
          const { sourceColumn, targetColumn } = getJunctionColumnNames(
            exists.name,
            rel.propertyName,
            targetTableName,
          );
          relation.junctionSourceColumn = sourceColumn;
          relation.junctionTargetColumn = targetColumn;
        }
      }

      return relation;
    });

    return metadata;
  }

  private async writeTableMetadataUpdates(
    queryRunner: any,
    id: string | number,
    body: CreateTableDto,
    exists: any,
    affectedTableNames: Set<string>,
  ): Promise<void> {
    await queryRunner('table_definition')
      .where({ id })
      .update({
        name: body.name,
        alias: body.alias,
        description: body.description,
        uniques: body.uniques
          ? JSON.stringify(body.uniques)
          : exists.uniques,
        indexes: body.indexes
          ? JSON.stringify(body.indexes)
          : exists.indexes,
        ...(body.isSingleRecord !== undefined && {
          isSingleRecord: body.isSingleRecord,
        }),
      });

    if (body.columns) {
      const existingColumns = await queryRunner('column_definition')
        .where({ tableId: id })
        .select('id');
      const deletedColumnIds = getDeletedIds(existingColumns, body.columns);
      if (deletedColumnIds.length > 0) {
        await queryRunner('column_definition')
          .whereIn('id', deletedColumnIds)
          .delete();
      }
      for (const col of body.columns) {
        if (
          col.name === 'id' ||
          col.name === 'createdAt' ||
          col.name === 'updatedAt'
        ) {
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
          tableId: id,
        };
        if (col.id) {
          await queryRunner('column_definition')
            .where({ id: col.id })
            .update(columnData);
        } else {
          await queryRunner('column_definition').insert(columnData);
        }
      }
    }

    if (body.relations) {
      const existingRelations = await queryRunner('relation_definition')
        .where({ sourceTableId: id })
        .select('id');
      const deletedRelationIds = getDeletedIds(existingRelations, body.relations);
      if (deletedRelationIds.length > 0) {
        const inverseRelations = await queryRunner('relation_definition')
          .whereIn('mappedById', deletedRelationIds)
          .select('sourceTableId');
        for (const inv of inverseRelations) {
          const invTable = await queryRunner('table_definition')
            .where({ id: inv.sourceTableId })
            .select('name')
            .first();
          if (invTable?.name) affectedTableNames.add(invTable.name);
        }
        await queryRunner('relation_definition')
          .whereIn('mappedById', deletedRelationIds)
          .delete();
        await queryRunner('relation_definition')
          .whereIn('id', deletedRelationIds)
          .delete();
      }
      for (const rel of body.relations) {
        if (!rel.id) continue;
        const existingRel = await queryRunner('relation_definition')
          .where({ id: rel.id })
          .first();
        if (existingRel?.mappedById) {
          const changed =
            (rel.type !== undefined && rel.type !== existingRel.type) ||
            (rel.targetTable !== undefined &&
              (typeof rel.targetTable === 'object'
                ? rel.targetTable.id
                : rel.targetTable) !== existingRel.targetTableId) ||
            (rel.mappedBy !== undefined && rel.mappedBy !== null) ||
            (rel.isNullable !== undefined &&
              rel.isNullable !== existingRel.isNullable);
          if (changed) {
            throw new ValidationException(
              `Inverse relation '${existingRel.propertyName}' can only have its propertyName modified`,
              { relationName: existingRel.propertyName },
            );
          }
        }
      }
      const targetTableIds = body.relations
        .map((rel: any) =>
          typeof rel.targetTable === 'object'
            ? rel.targetTable.id
            : rel.targetTable,
        )
        .filter((tid: any) => tid != null);
      const targetTablesMap = new Map<number, string>();
      if (targetTableIds.length > 0) {
        const targetTables = await queryRunner('table_definition')
          .select('id', 'name')
          .whereIn('id', targetTableIds);
        for (const table of targetTables) {
          targetTablesMap.set(table.id, table.name);
        }
      }
      for (const rel of body.relations) {
        const targetTableId =
          typeof rel.targetTable === 'object'
            ? rel.targetTable.id
            : rel.targetTable;
        if (rel.id) {
          const existingRel = await queryRunner('relation_definition')
            .where({ id: rel.id })
            .first();
          if (existingRel && existingRel.type !== rel.type) {
            throw new Error(
              `Cannot change relation type from '${existingRel.type}' to '${rel.type}' for property '${rel.propertyName}'. ` +
                `Please delete the old relation and create a new one.`,
            );
          }
        }
        let updateMappedById: number | null = null;
        if (rel.mappedBy) {
          const owningRel = await queryRunner('relation_definition')
            .where({ sourceTableId: targetTableId, propertyName: rel.mappedBy })
            .select('id')
            .first();
          updateMappedById = owningRel?.id || null;
        }
        const relationData: any = {
          propertyName: rel.propertyName,
          type: rel.type,
          targetTableId,
          mappedById: updateMappedById,
          isNullable: rel.isNullable ?? true,
          isSystem: rel.isSystem || false,
          isUpdatable: rel.isUpdatable ?? true,
          isPublished: rel.isPublished ?? true,
          description: rel.description,
          sourceTableId: id,
        };
        if (rel.type === 'many-to-many') {
          const targetTableName = targetTablesMap.get(targetTableId);
          if (!targetTableName) {
            throw new Error(
              `Target table with ID ${targetTableId} not found`,
            );
          }
          if (rel.id) {
            const existingRel = await queryRunner('relation_definition')
              .where({ id: rel.id })
              .first();
            if (existingRel && existingRel.junctionTableName) {
              relationData.junctionTableName = existingRel.junctionTableName;
              relationData.junctionSourceColumn =
                existingRel.junctionSourceColumn;
              relationData.junctionTargetColumn =
                existingRel.junctionTargetColumn;
            } else {
              const junctionTableName = getJunctionTableName(
                exists.name,
                rel.propertyName,
                targetTableName,
              );
              const { sourceColumn, targetColumn } = getJunctionColumnNames(
                exists.name,
                rel.propertyName,
                targetTableName,
              );
              relationData.junctionTableName = junctionTableName;
              relationData.junctionSourceColumn = sourceColumn;
              relationData.junctionTargetColumn = targetColumn;
            }
          } else {
            const junctionTableName = getJunctionTableName(
              exists.name,
              rel.propertyName,
              targetTableName,
            );
            const { sourceColumn, targetColumn } = getJunctionColumnNames(
              exists.name,
              rel.propertyName,
              targetTableName,
            );
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
          await queryRunner('relation_definition')
            .where({ id: rel.id })
            .update(relationData);
        } else {
          await queryRunner('relation_definition').insert(relationData);
        }
        if (rel.inversePropertyName && !rel.id) {
          if (rel.mappedBy) {
            throw new ValidationException(
              `Relation '${rel.propertyName}' cannot have both 'mappedBy' and 'inversePropertyName'`,
              { relationName: rel.propertyName },
            );
          }
          const existingOnTarget = await queryRunner('relation_definition')
            .where({
              sourceTableId: targetTableId,
              propertyName: rel.inversePropertyName,
            })
            .first();
          if (existingOnTarget) {
            throw new ValidationException(
              `Cannot create inverse '${rel.inversePropertyName}' on target table: property name already exists`,
              { relationName: rel.inversePropertyName },
            );
          }
          const insertedRel = await queryRunner('relation_definition')
            .where({ sourceTableId: id, propertyName: rel.propertyName })
            .first();
          if (insertedRel) {
            const existingInverse = await queryRunner('relation_definition')
              .where({ mappedById: insertedRel.id })
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
              targetTableId: id,
              mappedById: insertedRel.id,
              isNullable: rel.isNullable ?? true,
              isSystem: false,
              isUpdatable: rel.isUpdatable ?? true,
              isPublished: rel.isPublished ?? true,
            };
            if (inverseType === 'many-to-many') {
              inverseData.junctionTableName = relationData.junctionTableName;
              inverseData.junctionSourceColumn =
                relationData.junctionTargetColumn;
              inverseData.junctionTargetColumn =
                relationData.junctionSourceColumn;
            }
            await queryRunner('relation_definition').insert(inverseData);
            const targetName = targetTablesMap.get(targetTableId);
            if (targetName) affectedTableNames.add(targetName);
            this.logger.log(
              `Auto-created inverse relation '${rel.inversePropertyName}'`,
            );
          }
        }
      }
    }
  }
}
