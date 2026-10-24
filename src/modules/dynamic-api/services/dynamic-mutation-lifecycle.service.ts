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
import { summarizeMutationError } from '../utils/database-error-sanitizer.util';
import { logMemory } from '../../../shared/utils/memory-log.util';
import {
  DEFAULT_RECORD_VERSION,
  isVersionedTable,
  normalizeCreateVersion,
  normalizeUpdateVersion,
} from '../../../shared/utils/record-version.util';
import { isEnfyraVersionNewer } from '../../../shared/utils/enfyra-version.util';
import {
  projectCodeAuditPreImage,
  recordCodeAuditEntries,
  recordCodeAuditEntry,
  resolveVersionValue,
} from './code-audit.service';
import { Logger } from '../../../shared/logger';
import { deferDynamicTransactionEffect } from '../../../shared/utils/dynamic-transaction-effects.util';
import { markMutationPersistence } from '../../../shared/utils/mutation-outcome.util';

export class DynamicMutationLifecycleService {
  private readonly logger = new Logger('DynamicMutationLifecycleService');

  async run<TPersisted, TResult>(
    options: DynamicMutationLifecycleOptions<TPersisted, TResult>,
  ): Promise<TResult> {
    markMutationPersistence('unknown');
    const persisted = await options.persist();
    markMutationPersistence('persisted');
    const publish = async () => {
      await options.afterCommit?.(options.context);
      await options.reload(options.context);
      await options.afterReload?.(options.context);
      options.emit?.(options.context);
    };
    const deferred = options.dynamicContext
      && deferDynamicTransactionEffect(options.dynamicContext, publish);
    try {
      await options.afterWrite?.(options.context, persisted);
      try {
        return await options.buildResult(options.context, persisted);
      } catch (error) {
        if (options.recover) {
          return await options.recover(options.context, persisted, error);
        }
        throw error;
      }
    } finally {
      if (!deferred) await publish();
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

    markMutationPersistence('not_persisted');
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
      this.applyCreateVersion(tableName, body);

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
          ...(mutation.recordId != null ? { ids: [mutation.recordId] } : {}),
        });
        return runtime.find({
          filter: { [runtime.getIdField()]: { _eq: mutation.recordId } },
          ...(fields !== undefined ? { fields } : {}),
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
          ...(idValue != null ? { ids: [idValue] } : {}),
        });
        return runtime.find({
          filter: { [runtime.getIdField()]: { _eq: idValue } },
          ...(fields !== undefined ? { fields } : {}),
        });
      }

      return this.run({
        dynamicContext: context,
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
          await recordCodeAuditEntry(queryBuilderService, context, tableName, {
            recordId: ctx.id,
            action: 'create',
            toVersion:
              resolveVersionValue(body.version) ?? DEFAULT_RECORD_VERSION,
            previous: null,
          });
          await strategy.afterCreateWrite?.(ctx);
        },
        buildResult: async (_context, { createdId }) => {
          const result = await runtime.find({
            filter: { [runtime.getIdField()]: { _eq: createdId } },
            ...(fields !== undefined ? { fields } : {}),
          });
          logMemory(this.logger, 'dynamic create result loaded', {
            ...writeMeta,
            durationMs: Date.now() - startedAt,
          });
          return result;
        },
        reload: () =>
          runtime.reload({
            ids: [ctx.id],
            ...(strategy.requiresCriticalReload !== undefined
              ? { critical: strategy.requiresCriticalReload }
              : {}),
          }),
        afterReload: async () => {
          logMemory(this.logger, 'dynamic create done', {
            ...writeMeta,
            durationMs: Date.now() - startedAt,
          });
        },
        emit: () => runtime.emit('create', [ctx.id], body),
        recover: async (_context, { inserted }, error) => {
          const databaseError = classifyDynamicDatabaseError(
            error,
            queryBuilderService.getDatabaseType(),
          );
          if (databaseError.kind !== 'postgres_incompatible_operator') {
            throw error;
          }
          logMemory(this.logger, 'dynamic create done', {
            ...writeMeta,
            durationMs: Date.now() - startedAt,
            fallbackResult: true,
          });
          return {
            data: [inserted],
            count: 1,
          };
        },
      });
    } catch (error: any) {
      if (isCustomException(error)) {
        throw error;
      }
      throw new BadRequestException(
        summarizeMutationError(error, queryBuilderService.getDatabaseType())
          .message,
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
      context,
      data,
      fields,
    } = options;

    try {
      this.assertBulkSupported(routeRouter, tableName);
      const rows = this.requireManyRecords(data, 'data');
      const bodies: Record<string, any>[] = [];
      for (const row of rows) {
        const body = await mutationPreparationService.prepareCreateBody(
          row,
          tableName,
          tableMetadata,
          mutationAuthorizationService,
          tableValidationService,
        );
        this.applyCreateVersion(tableName, body);
        bodies.push(body);
      }

      const created =
        await mutationAuthorizationService.runWithFieldPermissionCheck(() =>
          mutationAuthorizationService.runWithMutationPolicy(() =>
            (queryBuilderService as any).createMany(tableName, bodies),
          ),
        );
      const ids = this.extractIds(created, runtime.getIdField());
      const firstId = ids[0];
      const firstBody = bodies[0];
      if (ids.length !== bodies.length || firstId === undefined || !firstBody) {
        throw new BadRequestException(
          'Bulk create did not return a primary key for every record',
        );
      }

      return this.run({
        dynamicContext: context,
        context: {
          tableName,
          id: firstId,
          body: firstBody,
          existing: null,
        },
        persist: async () => ids,
        afterWrite: async () => {
          await recordCodeAuditEntries(
            queryBuilderService,
            context,
            tableName,
            ids.map((id, index) => ({
              recordId: id,
              action: 'create' as const,
              toVersion:
                resolveVersionValue(bodies[index]?.version) ??
                DEFAULT_RECORD_VERSION,
              previous: null,
            })),
          );
        },
        buildResult: async () => {
          const result = await runtime.find({
            filter: { [runtime.getIdField()]: { _in: ids } },
            ...(fields !== undefined ? { fields } : {}),
            limit: 0,
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
      this.throwCreateError(error, queryBuilderService.getDatabaseType());
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

    markMutationPersistence('not_persisted');
    logMemory(this.logger, 'dynamic update start', writeMeta);

    try {
      const originalBody = data;
      let body = mutationPreparationService.prepareUpdateBody(
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
        ...(runtime.readLocked ? { forUpdate: true } : {}),
      });
      const exists =
        canonicalExistsResult?.data?.[0] ?? existsResult?.data?.[0];
      if (!exists) throw new BadRequestException(`id ${id} is not exists!`);
      logMemory(this.logger, 'dynamic update existing loaded', {
        ...writeMeta,
        durationMs: Date.now() - startedAt,
      });

      body = await mutationAuthorizationService.stripUnauthorizedDirectFields(
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
        const existingForNormalization = { ...exists };
        delete existingForNormalization.config;
        const normalizedFlowStep = mutationPreparationService.normalizeFlowStep({
          ...existingForNormalization,
          ...body,
        });
        if ('sourceCode' in normalizedFlowStep) {
          body.sourceCode = normalizedFlowStep.sourceCode;
        }
        if ('scriptLanguage' in normalizedFlowStep) {
          body.scriptLanguage = normalizedFlowStep.scriptLanguage;
        }
        if ('compiledCode' in normalizedFlowStep) {
          body.compiledCode = normalizedFlowStep.compiledCode;
        }
        delete body.config;
      }

      this.applyUpdateVersion(tableName, body, exists);

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
          ...(fields !== undefined ? { fields } : {}),
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
          ...(fields !== undefined ? { fields } : {}),
        });
      }

      return this.run({
        dynamicContext: context,
        context: ctx,
        persist: () =>
          mutationAuthorizationService.runWithFieldPermissionCheck(() =>
            mutationAuthorizationService.runWithMutationPolicy(() =>
              queryBuilderService.update(tableName, id, body),
            ),
          ),
        afterWrite: async () => {
          const revert = context?.$codeAudit;
          await recordCodeAuditEntry(queryBuilderService, context, tableName, {
            recordId: id,
            action: revert?.action === 'revert' ? 'revert' : 'update',
            fromVersion: resolveVersionValue(exists.version),
            toVersion: resolveVersionValue(body.version),
            restoredFrom:
              revert?.action === 'revert'
                ? (revert.restoredFrom ?? null)
                : null,
            previous: projectCodeAuditPreImage(exists, tableMetadata),
          });
          await strategy.afterUpdateWrite?.(ctx);
          logMemory(this.logger, 'dynamic update persisted', {
            ...writeMeta,
            durationMs: Date.now() - startedAt,
          });
        },
        buildResult: async () => {
          const result = await runtime.find({
            filter: { [runtime.getIdField()]: { _eq: id } },
            ...(fields !== undefined ? { fields } : {}),
          });
          logMemory(this.logger, 'dynamic update result loaded', {
            ...writeMeta,
            durationMs: Date.now() - startedAt,
          });
          return result;
        },
        reload: () =>
          runtime.reload({
            ids: [id],
            ...(strategy.requiresCriticalReload !== undefined
              ? { critical: strategy.requiresCriticalReload }
              : {}),
          }),
        afterCommit: () => strategy.afterUpdateCommit?.(ctx),
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
      this.logger.error({ message: 'Dynamic mutation failed', tableName }, error);
      throw new BadRequestException(
        summarizeMutationError(error, queryBuilderService.getDatabaseType())
          .message,
      );
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
      context,
      ids: requestedIds,
      data,
      fields,
    } = options;

    try {
      this.assertBulkSupported(routeRouter, tableName);
      const ids = this.requireManyIds(requestedIds);
      const firstId = ids[0];
      if (firstId === undefined) {
        throw new BadRequestException('ids must contain at least one id');
      }
      let body = mutationPreparationService.prepareUpdateBody(
        data,
        tableMetadata,
      );
      const existingRecords = await this.loadExistingRecords(
        queryBuilderService,
        tableName,
        runtime.getIdField(),
        ids,
      );

      for (const id of ids) {
        body = await mutationAuthorizationService.stripUnauthorizedDirectFields(
          'update',
          body,
          this.requireExistingRecord(existingRecords, id),
        );
      }
      this.assertNoRelationPayload(body, tableMetadata);

      await tableValidationService.assertTableValid({
        operation: 'update',
        tableName,
        tableMetadata,
      });
      for (const id of ids) {
        const existing = this.requireExistingRecord(existingRecords, id);
        await mutationAuthorizationService.assertMutationSafety(
          'update',
          body,
          existing,
        );
      }

      if (isVersionedTable(tableName)) {
        const currentVersions = ids.map(
          (id) => existingRecords.get(String(id))?.version,
        );
        body.version = normalizeUpdateVersion(
          body.version,
          currentVersions.reduce((highest, candidate) =>
            isEnfyraVersionNewer(candidate, highest) ? candidate : highest,
          ),
        );
      }

      return this.run({
        dynamicContext: context,
        context: {
          tableName,
          id: firstId,
          body,
          existing: this.requireExistingRecord(existingRecords, firstId),
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
        afterWrite: async () => {
          await recordCodeAuditEntries(
            queryBuilderService,
            context,
            tableName,
            ids.map((recordId) => ({
              recordId,
              action: 'update' as const,
              fromVersion:
                existingRecords.get(String(recordId))?.version ?? null,
              toVersion: resolveVersionValue(body.version),
              previous: projectCodeAuditPreImage(
                existingRecords.get(String(recordId)),
                tableMetadata,
              ),
            })),
          );
        },
        buildResult: async () => {
          const result = await runtime.find({
            filter: { [runtime.getIdField()]: { _in: ids } },
            ...(fields !== undefined ? { fields } : {}),
            limit: 0,
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
      this.throwMutationError(error, queryBuilderService.getDatabaseType());
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

    markMutationPersistence('not_persisted');
    logMemory(this.logger, 'dynamic delete start', writeMeta);

    try {
      const idField = runtime.getIdField();
      const exists = this.requireExistingRecord(
        await this.loadExistingRecords(
          queryBuilderService,
          tableName,
          idField,
          [id],
        ),
        id,
      );
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

      await strategy.beforeDelete?.(ctx);

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
        dynamicContext: context,
        context: ctx,
        persist: () =>
          mutationAuthorizationService.runWithMutationPolicy(() =>
            queryBuilderService.delete(tableName, id),
          ),
        afterWrite: async () => {
          await recordCodeAuditEntry(queryBuilderService, context, tableName, {
            recordId: id,
            action: 'delete',
            fromVersion: resolveVersionValue(exists.version),
            previous: projectCodeAuditPreImage(exists, tableMetadata),
          });
          await strategy.afterDeleteWrite?.(ctx);
        },
        buildResult: () => ({
          message: 'Delete successfully!',
          statusCode: 200,
        }),
        reload: () =>
          runtime.reload({
            ids: [id],
            ...(strategy.requiresCriticalReload !== undefined
              ? { critical: strategy.requiresCriticalReload }
              : {}),
          }),
        afterCommit: () => strategy.afterDeleteCommit?.(ctx),
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
      this.logger.error({ message: 'Dynamic mutation failed', tableName }, error);
      throw new BadRequestException(
        summarizeMutationError(error, queryBuilderService.getDatabaseType())
          .message,
      );
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
      context,
      ids: requestedIds,
    } = options;

    try {
      this.assertBulkSupported(routeRouter, tableName);
      const ids = this.requireManyIds(requestedIds);
      const firstId = ids[0];
      if (firstId === undefined) {
        throw new BadRequestException('ids must contain at least one id');
      }
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
          this.requireExistingRecord(existingRecords, id),
        );
      }

      return this.run({
        dynamicContext: context,
        context: {
          tableName,
          id: firstId,
          body: {},
          existing: this.requireExistingRecord(existingRecords, firstId),
        },
        persist: () =>
          mutationAuthorizationService.runWithMutationPolicy(() =>
            queryBuilderService.deleteMany(
              tableName,
              ids,
              runtime.getIdField(),
            ),
          ),
        afterWrite: async () => {
          await recordCodeAuditEntries(
            queryBuilderService,
            context,
            tableName,
            ids.map((recordId) => ({
              recordId,
              action: 'delete' as const,
              fromVersion:
                existingRecords.get(String(recordId))?.version ?? null,
              previous: projectCodeAuditPreImage(
                existingRecords.get(String(recordId)),
                tableMetadata,
              ),
            })),
          );
        },
        buildResult: () => ({
          message: 'Delete successfully!' as const,
          statusCode: 200 as const,
          count: ids.length,
        }),
        reload: () => runtime.reload({ ids }),
        emit: () => runtime.emit('delete', ids),
      });
    } catch (error: any) {
      this.throwMutationError(error, queryBuilderService.getDatabaseType());
    }
  }

  private applyCreateVersion(
    tableName: string,
    body: Record<string, any>,
  ): void {
    if (!isVersionedTable(tableName)) return;
    const version = normalizeCreateVersion(body.version);
    if (version === undefined) {
      delete body.version;
      return;
    }
    body.version = version;
  }

  private applyUpdateVersion(
    tableName: string,
    body: Record<string, any>,
    existing: Record<string, any>,
  ): void {
    if (!isVersionedTable(tableName)) return;
    body.version = normalizeUpdateVersion(body.version, existing?.version);
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

  private requireExistingRecord(
    records: Map<string, Record<string, any>>,
    id: DynamicMutationId,
  ): Record<string, any> {
    const record = records.get(String(id));
    if (!record) {
      throw new BadRequestException(`id ${id} is not exists!`);
    }
    return record;
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
      limit: 0,
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

  private throwCreateError(error: any, backend: string): never {
    if (isCustomException(error)) {
      throw error;
    }
    this.logger.error({ message: 'Dynamic create failed' }, error);
    throw new BadRequestException(summarizeMutationError(error, backend).message);
  }

  private throwMutationError(error: any, backend: string): never {
    if (isCustomException(error)) {
      throw error;
    }
    this.logger.error({ message: 'Dynamic mutation failed' }, error);
    throw new BadRequestException(summarizeMutationError(error, backend).message);
  }
}
