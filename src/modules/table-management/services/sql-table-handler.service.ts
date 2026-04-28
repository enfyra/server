import { Logger } from '../../../shared/logger';
import type { Knex } from 'knex';
import {
  getIoAbortSignal,
  compileScriptSource,
} from '../../../kernel/execution';
import {
  QueryBuilderService,
  getForeignKeyColumnName,
  getJunctionTableName,
  getJunctionColumnNames,
} from '../../../kernel/query';
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
import { generateDefaultRecord } from '../utils/generate-default-record';
import { DEFAULT_REST_HANDLER_LOGIC } from '../../../domain/bootstrap';
import { TableManagementValidationService } from './table-validation.service';
import { SqlTableMetadataBuilderService } from './sql-table-metadata-builder.service';
import { SqlTableMetadataWriterService } from './sql-table-metadata-writer.service';
export class SqlTableHandlerService {
  private logger = new Logger(SqlTableHandlerService.name);
  private queryBuilderService: QueryBuilderService;
  private schemaMigrationService: SqlSchemaMigrationService;
  private metadataCacheService: MetadataCacheService;
  private loggingService: LoggingService;
  private schemaMigrationLockService: SchemaMigrationLockService;
  private policyService: PolicyService;
  private tableValidationService: TableManagementValidationService;
  private sqlTableMetadataBuilderService: SqlTableMetadataBuilderService;
  private sqlTableMetadataWriterService: SqlTableMetadataWriterService;
  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    sqlSchemaMigrationService: SqlSchemaMigrationService;
    metadataCacheService: MetadataCacheService;
    loggingService: LoggingService;
    schemaMigrationLockService: SchemaMigrationLockService;
    policyService: PolicyService;
    tableManagementValidationService: TableManagementValidationService;
    sqlTableMetadataBuilderService: SqlTableMetadataBuilderService;
    sqlTableMetadataWriterService: SqlTableMetadataWriterService;
  }) {
    this.queryBuilderService = deps.queryBuilderService;
    this.schemaMigrationService = deps.sqlSchemaMigrationService;
    this.metadataCacheService = deps.metadataCacheService;
    this.loggingService = deps.loggingService;
    this.schemaMigrationLockService = deps.schemaMigrationLockService;
    this.policyService = deps.policyService;
    this.tableValidationService = deps.tableManagementValidationService;
    this.sqlTableMetadataBuilderService = deps.sqlTableMetadataBuilderService;
    this.sqlTableMetadataWriterService = deps.sqlTableMetadataWriterService;
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
          .map((rel: any) =>
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
        const relationsToInsert: Array<{ insertData: any; rel: any }> = [];
        for (const rel of bodyRelations) {
          const targetTableId =
            typeof rel.targetTable === 'object'
              ? rel.targetTable.id
              : rel.targetTable;
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
          if (
            availableMethodsRel?.junctionTableName &&
            availableMethodsRel.junctionSourceColumn &&
            availableMethodsRel.junctionTargetColumn &&
            methods?.length > 0
          ) {
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
                sourceCode: DEFAULT_REST_HANDLER_LOGIC[m.method] || null,
                scriptLanguage: 'typescript',
                compiledCode: compileScriptSource(
                  DEFAULT_REST_HANDLER_LOGIC[m.method] || null,
                  'typescript',
                ),
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
        const knex = this.queryBuilderService.getKnex();
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
    body: TCreateTableBody,
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
    body: TCreateTableBody,
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
    const knex = this.queryBuilderService.getKnex();
    const dbType = this.queryBuilderService.getDatabaseType();
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
    const bodyRelations = body.relations ?? [];
    this.tableValidationService.validateRelations(bodyRelations);
    stepLog(`STEP 2 name+relation validate done (+${lap()}ms)`);

    try {
      // === VALIDATION PHASE (read-only, no transaction) ===
      const exists = await knex('table_definition').where({ id }).first();
      stepLog(`STEP 3 fetched table_definition row (+${lap()}ms)`);
      if (!exists) {
        throw new ResourceNotFoundException('table_definition', String(id));
      }
      validateUniquePropertyNames(body.columns || [], bodyRelations);

      const m2mTargetTableIds =
        bodyRelations
          .filter((rel: any) => rel.type === 'many-to-many')
          .map((rel: any) =>
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
        bodyRelations,
        exists.name,
        m2mTargetTablesMap,
      );
      stepLog(`STEP 4 validators done (+${lap()}ms)`);

      const oldMetadata =
        await this.sqlTableMetadataBuilderService.getFullTableMetadataInTransaction(
          knex,
          exists.id,
        );
      stepLog(`STEP 5 loaded oldMetadata (+${lap()}ms)`);

      // Compute full target tables map (all relation targets)
      const allTargetTableIds =
        bodyRelations
          .map((rel: any) =>
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
          await this.sqlTableMetadataWriterService.writeTableMetadataUpdates(
            trx,
            id,
            body,
            exists,
            affectedTableNames,
          );
          stepLog(`STEP 8 PG: metadata written (+${lap()}ms)`);

          const updatedFullMetadata =
            await this.sqlTableMetadataBuilderService.getFullTableMetadataInTransaction(
              trx,
              exists.id,
            );
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
                exists.name,
                oldMetadata,
                updatedFullMetadata,
                trx,
              ),
              new Promise<never>((_, reject) =>
                setTimeout(
                  () =>
                    reject(new Error(`DDL timed out after ${ddlTimeoutMs}ms`)),
                  ddlTimeoutMs,
                ),
              ),
            ]);
            stepLog(`STEP 11 PG: DDL done (+${lap()}ms)`);
            if (pendingUpdate?.pendingMetadataUpdate) {
              await this.schemaMigrationService.applyPendingMetadataUpdate(
                pendingUpdate.pendingMetadataUpdate,
                trx,
              );
            }
            if (pendingUpdate?.journalUuid) {
              await this.schemaMigrationService
                .markJournalCompleted(pendingUpdate.journalUuid)
                .catch(() => {});
            }
          }

          stepLog(`STEP 12 PG: committing metadata + DDL transaction...`);
          await trx.commit();
          stepLog(`STEP 12 PG: committed (+${lap()}ms)`);
        } catch (innerError: any) {
          if (trx && !trx.isCompleted()) {
            try {
              await trx.rollback();
            } catch (_) {}
          }
          throw innerError;
        }
      } else {
        // === MYSQL PATH: DDL first, then metadata writes ===
        stepLog(`STEP 6 ${dbType}: constructing afterMetadata from body...`);
        const afterMetadata =
          this.sqlTableMetadataBuilderService.constructAfterMetadata(
            exists,
            body,
            oldMetadata,
            allTargetTablesMap,
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
                exists.name,
                oldMetadata,
                afterMetadata,
              ),
              new Promise<never>((_, reject) =>
                setTimeout(
                  () =>
                    reject(new Error(`DDL timed out after ${ddlTimeoutMs}ms`)),
                  ddlTimeoutMs,
                ),
              ),
            ]);
            stepLog(`STEP 9 ${dbType}: DDL done (+${lap()}ms)`);
          } catch (ddlError: any) {
            stepLog(`STEP 9 ${dbType}: DDL FAILED, metadata not saved`);
            throw ddlError;
          }

          stepLog(`STEP 10 ${dbType}: writing metadata after DDL...`);
          const journalUuid = pendingUpdate?.journalUuid;
          let metadataWritten = false;
          const maxRetries = 3;
          for (let attempt = 1; attempt <= maxRetries; attempt++) {
            const trx = await knex.transaction();
            try {
              await this.sqlTableMetadataWriterService.writeTableMetadataUpdates(
                trx,
                id,
                body,
                exists,
                affectedTableNames,
              );
              await trx.commit();
              metadataWritten = true;
              stepLog(
                `STEP 10 ${dbType}: metadata committed (attempt ${attempt}) (+${lap()}ms)`,
              );
              break;
            } catch (metadataError: any) {
              if (trx && !trx.isCompleted()) {
                try {
                  await trx.rollback();
                } catch (_) {}
              }
              stepLog(
                `STEP 10 ${dbType}: metadata write FAILED (attempt ${attempt}/${maxRetries})`,
              );
              if (attempt === maxRetries) {
                stepLog(
                  `STEP 10 ${dbType}: all retries exhausted, rolling back DDL via journal ${journalUuid}`,
                );
                if (journalUuid) {
                  await this.schemaMigrationService
                    .rollbackJournal(journalUuid)
                    .catch((rbErr) => {
                      this.loggingService.error(
                        'DDL rollback after metadata failure also failed',
                        {
                          context: 'updateTable',
                          journalUuid,
                          error: rbErr.message,
                        },
                      );
                    });
                }
                throw metadataError;
              }
              await new Promise((r) => setTimeout(r, 1000));
            }
          }

          if (metadataWritten && journalUuid) {
            await this.schemaMigrationService
              .markJournalCompleted(journalUuid)
              .catch(() => {});
          }

          if (pendingUpdate?.pendingMetadataUpdate) {
            await this.schemaMigrationService.applyPendingMetadataUpdate(
              pendingUpdate.pendingMetadataUpdate,
            );
            stepLog(
              `STEP 11 ${dbType}: applied pending metadata update (+${lap()}ms)`,
            );
          }
        } else {
          stepLog(
            `STEP 9 ${dbType}: no schema change, writing metadata only...`,
          );
          const trx = await knex.transaction();
          try {
            await this.sqlTableMetadataWriterService.writeTableMetadataUpdates(
              trx,
              id,
              body,
              exists,
              affectedTableNames,
            );
            await trx.commit();
            stepLog(`STEP 9 ${dbType}: metadata committed (+${lap()}ms)`);
          } catch (metadataError: any) {
            if (trx && !trx.isCompleted()) {
              try {
                await trx.rollback();
              } catch (_) {}
            }
            throw metadataError;
          }
        }
      }

      // === POST-MIGRATION (common) ===
      if (body.isSingleRecord === true && !exists.isSingleRecord) {
        const recordCount = await knex(exists.name).count('* as count').first();
        const count = Number(recordCount?.count || 0);
        if (count === 0) {
          const fullMetadata =
            await this.sqlTableMetadataBuilderService.getFullTableMetadataInTransaction(
              knex,
              exists.id,
            );
          const defaultRecord = generateDefaultRecord(
            fullMetadata?.columns || [],
          );
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

      return {
        id: exists.id,
        name: exists.name,
        affectedTables: [...affectedTableNames],
      };
    } catch (error: any) {
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
    const knex = this.queryBuilderService.getKnex();
    const affectedTableNames = new Set<string>();
    return await knex.transaction(async (trx: Knex.Transaction) => {
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
                await import('../../../kernel/query');
              const fkColumn = getForeignKeyColumnName(tableName);
              const columnExists = await trx.schema.hasColumn(
                sourceTable.name,
                fkColumn,
              );
              if (columnExists) {
                try {
                  const dbType = this.queryBuilderService.getDatabaseType();
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
                } catch (error: any) {}
                try {
                  await trx.schema.alterTable(sourceTable.name, (table: any) => {
                    table.dropColumn(fkColumn);
                  });
                } catch (error: any) {}
              }
            }
          }
        }
        try {
          const dbType = this.queryBuilderService.getDatabaseType();
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
              } catch (error: any) {}
              try {
                await trx.schema.alterTable(fk.table_name, (table: any) => {
                  table.dropColumn(fk.column_name);
                });
              } catch (error: any) {}
            }
          } else {
          }
        } catch (error: any) {}
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
        exists.affectedTables = [...affectedTableNames];
        return exists;
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
}
