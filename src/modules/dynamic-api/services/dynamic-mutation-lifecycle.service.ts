import type {
  DynamicMutationCreateOptions,
  DynamicMutationDeleteOptions,
  DynamicMutationLifecycleOptions,
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
}
