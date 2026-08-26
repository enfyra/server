import type {
  DynamicMutationCreateManyOptions,
  DynamicMutationCreateOptions,
  DynamicMutationDeleteManyOptions,
  DynamicMutationDeleteOptions,
  DynamicMutationLifecycleOptions,
  DynamicMutationManyDeleteResult,
  DynamicMutationManyResult,
  DynamicMutationId,
  DynamicMutationUpdateManyOptions,
  DynamicMutationUpdateOptions,
} from '../types/dynamic-mutation-lifecycle.types';
import { BadRequestException } from '../../../domain/exceptions';
import { isCustomException } from '../../../domain/exceptions/custom-exceptions';
import { classifyDynamicDatabaseError } from '../utils/database-error-classifier.util';
import { logMemory } from '../../../shared/utils/memory-log.util';
import { Logger } from '../../../shared/logger';

export class DynamicMutationLifecycleService {
  private readonly logger = new Logger('DynamicMutationLifecycleService');

  async run<TPersisted, TResult>(
    options: DynamicMutationLifecycleOptions<TPersisted, TResult>,
  ): Promise<TResult> {
    const persisted = await options.persist();
    await options.afterWrite?.(options.context, persisted);
    try {
      const result = await options.buildResult(options.context, persisted);
      await options.reload(options.context);
      await options.afterReload?.(options.context);
      options.emit?.(options.context);
      return result;
    } catch (error) {
      if (options.recover) {
        return options.recover(options.context, persisted, error);
      }
      throw error;
    }
  }

  async create(options: DynamicMutationCreateOptions): Promise<unknown> {
    const {
      runtime,
      routeRouter,
      runtimeMetadataSchemaRouterService,
      queryBuilderService,
      schemaActivationService,
      mutationPreparationService,
      mutationAuthorizationService,
      tableValidationService,
      batchCreationService,
      tableName,
      tableMetadata,
      context,
      data,
      fields,
      batch,
    } = options;
    const startedAt = Date.now();
    const writeMeta = {
      table: tableName,
      operation: 'create',
      batch: batch === true,
    };

    logMemory(this.logger, 'dynamic create start', writeMeta);

    try {
      if (batch) {
        const result = await batchCreationService.createBatch(
          tableName,
          tableMetadata,
          data,
        );
        logMemory(this.logger, 'dynamic create batch done', {
          ...writeMeta,
          durationMs: Date.now() - startedAt,
          count: result.count,
        });
        runtime.emit('create');
        return result;
      }

      const strategy = routeRouter.getStrategy(tableName);
      const body = await mutationPreparationService.prepareCreateBody(
        data,
        tableName,
        tableMetadata,
        mutationAuthorizationService,
        tableValidationService,
        strategy,
      );
      logMemory(this.logger, 'dynamic create body prepared', {
        ...writeMeta,
        bodyKeys: Object.keys(body).length,
      });

      const ctx = {
        tableName,
        id: body.id ?? body._id,
        body,
        existing: null,
      };

      if (strategy.kind === 'schema') {
        const mutation = await runtimeMetadataSchemaRouterService.create({
          tableName: tableName as any,
          data: body,
          context,
        });
        if (mutation.preview) return { data: [mutation.preview] };
        await schemaActivationService.activate(mutation, {
          ids: mutation.recordId == null ? undefined : [mutation.recordId],
        });
        return runtime.find({
          filter: { [runtime.getIdField()]: { _eq: mutation.recordId } },
          fields,
        });
      }

      if (strategy.kind === 'table') {
        body.isSystem = false;
        const mutation = await runtimeMetadataSchemaRouterService.createTable({
          body: body as any,
          context,
        });
        if (mutation.preview) return { data: [mutation.preview] };
        const idValue = mutation.recordId;
        await schemaActivationService.activate(mutation, {
          ids: idValue == null ? undefined : [idValue],
        });
        return runtime.find({
          filter: { [runtime.getIdField()]: { _eq: idValue } },
          fields,
        });
      }

      return this.run({
        context: ctx,
        persist: async () => {
          const inserted = await mutationPreparationService.executeCreateBody(
            body,
            tableName,
            mutationAuthorizationService,
            queryBuilderService,
          );
          logMemory(this.logger, 'dynamic create persisted', {
            ...writeMeta,
            durationMs: Date.now() - startedAt,
          });
          const createdId = inserted.id || inserted._id || body.id;
          ctx.id = createdId;
          return { inserted, createdId };
        },
        afterWrite: async () => {
          await strategy.afterCreateWrite?.(ctx);
        },
        buildResult: async (_context, { createdId }) => {
          const result = await runtime.find({
            filter: { [runtime.getIdField()]: { _eq: createdId } },
            fields,
          });
          logMemory(this.logger, 'dynamic create result loaded', {
            ...writeMeta,
            durationMs: Date.now() - startedAt,
          });
          return result;
        },
        reload: () => runtime.reload({ ids: [ctx.id] }),
        afterReload: async () => {
          logMemory(this.logger, 'dynamic create done', {
            ...writeMeta,
            durationMs: Date.now() - startedAt,
          });
        },
        emit: () => runtime.emit('create', [ctx.id], body),
        recover: async (_context, { inserted, createdId }, error) => {
          const databaseError = classifyDynamicDatabaseError(
            error,
            queryBuilderService.getDatabaseType(),
          );
          if (databaseError.kind !== 'postgres_incompatible_operator') {
            throw error;
          }
          await runtime.reload({ ids: [createdId] });
          logMemory(this.logger, 'dynamic create done', {
            ...writeMeta,
            durationMs: Date.now() - startedAt,
            fallbackResult: true,
          });
          runtime.emit('create', [createdId], body);
          return {
            data: [inserted],
            count: 1,
          };
        },
      });
    } catch (error: any) {
      if (
        error.constructor?.name &&
        [
          'BadRequestException',
          'NotFoundException',
          'ForbiddenException',
        ].includes(error.constructor.name)
      ) {
        throw error;
      }
      if (error.errInfo) {
        const errorMessage = error.errInfo?.details?.details
          ? JSON.stringify(error.errInfo.details.details, null, 2)
          : error.message || 'Document failed validation';
        throw new BadRequestException(errorMessage);
      }
      throw new BadRequestException(
        error.message || 'Document failed validation',
      );
    }
  }

  async createMany(
    options: DynamicMutationCreateManyOptions,
  ): Promise<DynamicMutationManyResult> {
    const {
      runtime,
      routeRouter,
      queryBuilderService,
      mutationPreparationService,
      mutationAuthorizationService,
      tableValidationService,
      tableName,
      tableMetadata,
      data,
      fields,
    } = options;

    try {
      this.assertBulkSupported(routeRouter, tableName);
      const rows = this.requireManyRecords(data, 'data');
      const bodies: Record<string, any>[] = [];
      for (const row of rows) {
        bodies.push(
          await mutationPreparationService.prepareCreateBody(
            row,
            tableName,
            tableMetadata,
            mutationAuthorizationService,
            tableValidationService,
          ),
        );
      }

      const created =
        await mutationAuthorizationService.runWithFieldPermissionCheck(() =>
          mutationAuthorizationService.runWithMutationPolicy(() =>
            (queryBuilderService as any).createMany(tableName, bodies),
          ),
        );
      const ids = this.extractIds(created, runtime.getIdField());
      if (ids.length !== bodies.length) {
        throw new BadRequestException(
          'Bulk create did not return a primary key for every record',
        );
      }

      return this.run({
        context: {
          tableName,
          id: ids[0],
          body: bodies[0],
          existing: null,
        },
        persist: async () => ids,
        buildResult: async () => {
          const result = await runtime.find({
            filter: { [runtime.getIdField()]: { _in: ids } },
            fields,
            limit: -1,
          });
          return {
            data: result.data ?? [],
            count: result.count ?? result.data?.length ?? 0,
          };
        },
        reload: () => runtime.reload({ ids }),
        emit: () => runtime.emit('create', ids),
      });
    } catch (error: any) {
      this.throwCreateError(error);
    }
  }
  async update(options: DynamicMutationUpdateOptions): Promise<unknown> {
    const {
      runtime,
      routeRouter,
      runtimeMetadataSchemaRouterService,
      queryBuilderService,
      mutationPreparationService,
      mutationAuthorizationService,
      tableValidationService,
      tableMetadata,
      tableName,
      context,
      id,
      data,
      fields,
      schemaActivationService,
    } = options;
    const startedAt = Date.now();
    const writeMeta = { table: tableName, operation: 'update', id };

    logMemory(this.logger, 'dynamic update start', writeMeta);

    try {
      const originalBody = data;
      const body = mutationPreparationService.prepareUpdateBody(
        originalBody,
        tableMetadata,
      );
      logMemory(this.logger, 'dynamic update body stripped', {
        ...writeMeta,
        bodyKeys:
          body && typeof body === 'object' ? Object.keys(body).length : 0,
      });

      const existsResult = await runtime.find({
        filter: { [runtime.getIdField()]: { _eq: id } },
      });
      const canonicalExistsResult = await queryBuilderService.find({
        table: tableName,
        fields: '*',
        filter: { [runtime.getIdField()]: { _eq: id } },
        limit: 1,
      });
      const exists =
        canonicalExistsResult?.data?.[0] ?? existsResult?.data?.[0];
      if (!exists) throw new BadRequestException(`id ${id} is not exists!`);
      logMemory(this.logger, 'dynamic update existing loaded', {
        ...writeMeta,
        durationMs: Date.now() - startedAt,
      });

      await mutationAuthorizationService.assertDirectFieldPermission(
        'update',
        body,
        exists,
      );

      await tableValidationService.assertTableValid({
        operation: 'update',
        tableName,
        tableMetadata,
      });
      await mutationAuthorizationService.assertMutationSafety(
        'update',
        body,
        exists,
      );

      const strategy = routeRouter.getStrategy(tableName);
      await strategy.normalizeUpdate?.(body, exists, id);
      Object.assign(
        body,
        mutationPreparationService.normalizeUpdate(tableName, body, exists),
      );
      if (tableName === 'enfyra_flow_step') {
        const normalizedFlowStep = mutationPreparationService.normalizeFlowStep(
          {
            ...exists,
            ...body,
          },
        );
        if ('sourceCode' in normalizedFlowStep) {
          body.sourceCode = normalizedFlowStep.sourceCode;
        }
        if ('scriptLanguage' in normalizedFlowStep) {
          body.scriptLanguage = normalizedFlowStep.scriptLanguage;
        }
        if ('compiledCode' in normalizedFlowStep) {
          body.compiledCode = normalizedFlowStep.compiledCode;
        }
        if ('config' in normalizedFlowStep) {
          body.config = normalizedFlowStep.config;
        }
      }

      const ctx = {
        tableName,
        id,
        body,
        existing: exists,
      };

      if (strategy.kind === 'table') {
        const mutation = await runtimeMetadataSchemaRouterService.updateTable({
          tableId: id,
          body: body as any,
          existing: exists,
          context,
        });
        if (mutation.preview) return { data: [mutation.preview] };
        const tableId = mutation.recordId ?? id;
        await schemaActivationService.activate(mutation, {
          ids: [tableId],
        });
        const result = await runtime.find({
          filter: { [runtime.getIdField()]: { _eq: tableId } },
          fields,
        });
        return result;
      }
      if (strategy.kind === 'schema') {
        const mutation = await runtimeMetadataSchemaRouterService.update({
          tableName: tableName as any,
          recordId: id,
          data: body,
          existing: exists,
          context,
        });
        if (mutation.preview) return { data: [mutation.preview] };
        await schemaActivationService.activate(mutation, { ids: [id] });
        return runtime.find({
          filter: { [runtime.getIdField()]: { _eq: id } },
          fields,
        });
      }

      return this.run({
        context: ctx,
        persist: () =>
          mutationAuthorizationService.runWithFieldPermissionCheck(() =>
            mutationAuthorizationService.runWithMutationPolicy(() =>
              queryBuilderService.update(tableName, id, body),
            ),
          ),
        afterWrite: async () => {
          await strategy.afterUpdateWrite?.(ctx);
          logMemory(this.logger, 'dynamic update persisted', {
            ...writeMeta,
            durationMs: Date.now() - startedAt,
          });
        },
        buildResult: async () => {
          const result = await runtime.find({
            filter: { [runtime.getIdField()]: { _eq: id } },
            fields,
          });
          logMemory(this.logger, 'dynamic update result loaded', {
            ...writeMeta,
            durationMs: Date.now() - startedAt,
          });
          return result;
        },
        reload: () => runtime.reload({ ids: [id] }),
        afterReload: async () => {
          logMemory(this.logger, 'dynamic update done', {
            ...writeMeta,
            durationMs: Date.now() - startedAt,
          });
          await strategy.afterUpdateReload?.(ctx);
        },
        emit: () => runtime.emit('update', [id], body),
      });
    } catch (error: any) {
      if (isCustomException(error)) {
        throw error;
      }
      throw new BadRequestException(error.message);
    }
  }

  async updateMany(
    options: DynamicMutationUpdateManyOptions,
  ): Promise<DynamicMutationManyResult> {
    const {
      runtime,
      routeRouter,
      queryBuilderService,
      mutationPreparationService,
      mutationAuthorizationService,
      tableValidationService,
      tableName,
      tableMetadata,
      ids: requestedIds,
      data,
      fields,
    } = options;

    try {
      this.assertBulkSupported(routeRouter, tableName);
      const ids = this.requireManyIds(requestedIds);
      const body = mutationPreparationService.prepareUpdateBody(
        data,
        tableMetadata,
      );
      this.assertNoRelationPayload(body, tableMetadata);
      const existingRecords = await this.loadExistingRecords(
        queryBuilderService,
        tableName,
        runtime.getIdField(),
        ids,
      );

      await tableValidationService.assertTableValid({
        operation: 'update',
        tableName,
        tableMetadata,
      });
      for (const id of ids) {
        const existing = existingRecords.get(String(id))!;
        await mutationAuthorizationService.assertDirectFieldPermission(
          'update',
          body,
          existing,
        );
        await mutationAuthorizationService.assertMutationSafety(
          'update',
          body,
          existing,
        );
      }

      return this.run({
        context: {
          tableName,
          id: ids[0],
          body,
          existing: existingRecords.get(String(ids[0]))!,
        },
        persist: () =>
          mutationAuthorizationService.runWithFieldPermissionCheck(() =>
            mutationAuthorizationService.runWithMutationPolicy(() =>
              queryBuilderService.updateMany(
                tableName,
                ids,
                body,
                runtime.getIdField(),
              ),
            ),
          ),
        buildResult: async () => {
          const result = await runtime.find({
            filter: { [runtime.getIdField()]: { _in: ids } },
            fields,
            limit: -1,
          });
          return {
            data: result.data ?? [],
            count: result.count ?? result.data?.length ?? 0,
          };
        },
        reload: () => runtime.reload({ ids }),
        emit: () => runtime.emit('update', ids, body),
      });
    } catch (error: any) {
      this.throwMutationError(error);
    }
  }

  async delete(options: DynamicMutationDeleteOptions): Promise<unknown> {
    const {
      runtime,
      routeRouter,
      runtimeMetadataSchemaRouterService,
      queryBuilderService,
      schemaActivationService,
      mutationAuthorizationService,
      tableValidationService,
      tableName,
      tableMetadata,
      context,
      id,
    } = options;
    const startedAt = Date.now();
    const writeMeta = { table: tableName, operation: 'delete', id };

    logMemory(this.logger, 'dynamic delete start', writeMeta);

    try {
      const idField = runtime.getIdField();
      const existsResult = await runtime.find({
        filter: { [idField]: { _eq: id } },
      });
      const exists = existsResult?.data?.[0];
      if (!exists) throw new BadRequestException(`id ${id} is not exists!`);
      logMemory(this.logger, 'dynamic delete existing loaded', {
        ...writeMeta,
        durationMs: Date.now() - startedAt,
      });

      await tableValidationService.assertTableValid({
        operation: 'delete',
        tableName,
        tableMetadata,
      });
      await mutationAuthorizationService.assertMutationSafety(
        'delete',
        {},
        exists,
      );

      const strategy = routeRouter.getStrategy(tableName);
      const ctx = {
        tableName,
        id,
        body: {},
        existing: exists,
      };

      if (strategy.kind === 'table') {
        const mutation = await runtimeMetadataSchemaRouterService.deleteTable({
          tableId: id,
          existing: exists,
          context,
        });
        if (mutation.preview) return { data: [mutation.preview] };
        await schemaActivationService.activate(mutation, { ids: [id] });
        return { message: 'Success', statusCode: 200 };
      }
      if (strategy.kind === 'schema') {
        const mutation = await runtimeMetadataSchemaRouterService.delete({
          tableName: tableName as any,
          recordId: id,
          existing: exists,
          context,
        });
        if (mutation.preview) return { data: [mutation.preview] };
        await schemaActivationService.activate(mutation, { ids: [id] });
        return { message: 'Success', statusCode: 200 };
      }

      return this.run({
        context: ctx,
        persist: () =>
          mutationAuthorizationService.runWithMutationPolicy(() =>
            queryBuilderService.delete(tableName, id),
          ),
        afterWrite: async () => {
          await strategy.afterDeleteWrite?.(ctx);
        },
        buildResult: () => ({
          message: 'Delete successfully!',
          statusCode: 200,
        }),
        reload: () => runtime.reload({ ids: [id] }),
        afterReload: async () => {
          logMemory(this.logger, 'dynamic delete done', {
            ...writeMeta,
            durationMs: Date.now() - startedAt,
          });
          await strategy.afterDeleteReload?.(ctx);
        },
        emit: () => runtime.emit('delete', [id]),
      });
    } catch (error: any) {
      if (isCustomException(error)) {
        throw error;
      }
      throw new BadRequestException(error.message);
    }
  }

  async deleteMany(
    options: DynamicMutationDeleteManyOptions,
  ): Promise<DynamicMutationManyDeleteResult> {
    const {
      runtime,
      routeRouter,
      queryBuilderService,
      mutationAuthorizationService,
      tableValidationService,
      tableName,
      tableMetadata,
      ids: requestedIds,
    } = options;

    try {
      this.assertBulkSupported(routeRouter, tableName);
      const ids = this.requireManyIds(requestedIds);
      const existingRecords = await this.loadExistingRecords(
        queryBuilderService,
        tableName,
        runtime.getIdField(),
        ids,
      );

      await tableValidationService.assertTableValid({
        operation: 'delete',
        tableName,
        tableMetadata,
      });
      for (const id of ids) {
        await mutationAuthorizationService.assertMutationSafety(
          'delete',
          {},
          existingRecords.get(String(id))!,
        );
      }

      return this.run({
        context: {
          tableName,
          id: ids[0],
          body: {},
          existing: existingRecords.get(String(ids[0]))!,
        },
        persist: () =>
          mutationAuthorizationService.runWithMutationPolicy(() =>
            queryBuilderService.deleteMany(
              tableName,
              ids,
              runtime.getIdField(),
            ),
          ),
        buildResult: () => ({
          message: 'Delete successfully!' as const,
          statusCode: 200 as const,
          count: ids.length,
        }),
        reload: () => runtime.reload({ ids }),
        emit: () => runtime.emit('delete', ids),
      });
    } catch (error: any) {
      this.throwMutationError(error);
    }
  }

  private assertBulkSupported(routeRouter: any, tableName: string): void {
    const strategy = routeRouter.getStrategy(tableName);
    if (
      strategy.kind !== 'generic' ||
      Object.keys(strategy).some((key) => key !== 'kind')
    ) {
      throw new BadRequestException(
        `Bulk mutations are not supported for ${tableName}. Use single-record operations.`,
      );
    }
  }

  private requireManyRecords(
    records: Array<Record<string, unknown>>,
    fieldName: string,
  ): Array<Record<string, unknown>> {
    if (!Array.isArray(records) || records.length === 0) {
      throw new BadRequestException(
        `${fieldName} must contain at least one record`,
      );
    }
    return records;
  }

  private requireManyIds(ids: DynamicMutationId[]): DynamicMutationId[] {
    if (!Array.isArray(ids) || ids.length === 0) {
      throw new BadRequestException('ids must contain at least one id');
    }
    const unique = new Set<string>();
    for (const id of ids) {
      if (id === null || id === undefined || id === '') {
        throw new BadRequestException('ids must not contain empty values');
      }
      const key = String(id);
      if (unique.has(key)) {
        throw new BadRequestException('ids must not contain duplicates');
      }
      unique.add(key);
    }
    return ids;
  }

  private async loadExistingRecords(
    queryBuilderService: any,
    tableName: string,
    idField: string,
    ids: DynamicMutationId[],
  ): Promise<Map<string, Record<string, any>>> {
    const result = await queryBuilderService.find({
      table: tableName,
      fields: '*',
      filter: { [idField]: { _in: ids } },
      limit: -1,
    });
    const records = new Map<string, Record<string, any>>();
    for (const record of result?.data ?? []) {
      const id = record?.[idField] ?? record?.id ?? record?._id;
      if (id !== null && id !== undefined) records.set(String(id), record);
    }
    const missing = ids.filter((id) => !records.has(String(id)));
    if (missing.length > 0) {
      throw new BadRequestException(`id ${missing[0]} is not exists!`);
    }
    return records;
  }

  private assertNoRelationPayload(
    body: Record<string, unknown>,
    tableMetadata: any,
  ): void {
    const relationFields = new Set(
      Array.isArray(tableMetadata?.relations)
        ? tableMetadata.relations.map((relation: any) => relation.propertyName)
        : [],
    );
    if (Object.keys(body).some((key) => relationFields.has(key))) {
      throw new BadRequestException(
        'Bulk update does not support relation payloads. Use single-record operations.',
      );
    }
  }

  private extractIds(records: any, idField: string): DynamicMutationId[] {
    if (!Array.isArray(records)) return [];
    const ids: DynamicMutationId[] = [];
    for (const record of records) {
      const id =
        typeof record === 'object' && record !== null
          ? (record[idField] ?? record.id ?? record._id)
          : record;
      if (typeof id !== 'string' && typeof id !== 'number') return [];
      ids.push(id);
    }
    return ids;
  }

  private throwCreateError(error: any): never {
    if (
      error.constructor?.name &&
      [
        'BadRequestException',
        'NotFoundException',
        'ForbiddenException',
      ].includes(error.constructor.name)
    ) {
      throw error;
    }
    if (error.errInfo) {
      const errorMessage = error.errInfo?.details?.details
        ? JSON.stringify(error.errInfo.details.details, null, 2)
        : error.message || 'Document failed validation';
      throw new BadRequestException(errorMessage);
    }
    throw new BadRequestException(
      error.message || 'Document failed validation',
    );
  }

  private throwMutationError(error: any): never {
    if (isCustomException(error)) {
      throw error;
    }
    throw new BadRequestException(error.message);
  }
}
