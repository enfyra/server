import {
  BadRequestException,
  isCustomException,
} from '../../../domain/exceptions';
import { EventEmitter2 } from 'eventemitter2';
import { Logger } from '../../../shared/logger';
import { QueryBuilderService } from '@enfyra/kernel';
import { RuntimeMetadataSchemaRouterService } from '../../table-management';
import { PolicyService } from '../../../domain/policy';
import { DynamicApiTableValidationService } from '../services/table-validation.service';
import { DynamicReadAuthorizationService } from '../services/dynamic-read-authorization.service';
import { DynamicMutationPreparationService } from '../services/dynamic-mutation-preparation.service';
import { DynamicMutationLifecycleService } from '../services/dynamic-mutation-lifecycle.service';
import { DynamicMutationAuthorizationService } from '../services/dynamic-mutation-authorization.service';
import { DynamicRepositoryReadService } from '../services/dynamic-repository-read.service';
import { DynamicTableRouteHandlerService } from '../services/dynamic-table-route-handler.service';
import type { DynamicReadOptions } from '../types/dynamic-read.types';
import type { GuardValidationService } from '../services/guard-validation.service';
import { TDynamicContext } from '../../../shared/types';
import {
  CACHE_EVENTS,
  DATA_EVENTS,
} from '../../../shared/utils/cache-events.constants';
import { TCacheInvalidationPayload } from '../../../shared/types/cache.types';
import type { BcryptService, UserRevocationService } from '../../../domain/auth';
import type { FlowQueueMaintenanceService } from '../../flow';
import { logMemory } from '../../../shared/utils/memory-log.util';
import type { RuntimeRegistryService } from '../../../engines/cache/services/runtime-registry.service';
import type { RuntimeSchemaActivationGateService } from '../../table-management';
import type {
  RuntimeMetadataSchemaMutationResult,
  RuntimeSchemaMetadataTable,
} from '../../table-management/types/runtime-metadata-schema-router.types';
import { TableRouteRouter } from './table-route.router';
import { classifyDynamicDatabaseError } from '../utils/database-error-classifier.util';
import type {
  MutationContext,
  TableRouteStrategy,
} from '../types/table-route.types';

interface DynamicBatchCreateResult {
  accepted: true;
  batch: true;
  count: number;
}

export class DynamicRepository {
  private readonly logger = new Logger('DynamicRepository');
  public context: TDynamicContext;
  private tableName: string;
  private queryBuilderService: QueryBuilderService;
  private runtimeMetadataSchemaRouterService: RuntimeMetadataSchemaRouterService;
  private tableValidationService: DynamicApiTableValidationService;
  private eventEmitter: EventEmitter2;
  private runtimeRegistryService: RuntimeRegistryService;
  private tableMetadata: any;
  private readonly runtimeSchemaActivationGateService?: RuntimeSchemaActivationGateService;
  private readonly routeRouter: TableRouteRouter;
  private readonly readAuthorizationService: DynamicReadAuthorizationService;
  private readonly mutationPreparationService =
    new DynamicMutationPreparationService();
  private readonly mutationLifecycleService =
    new DynamicMutationLifecycleService();
  private readonly mutationAuthorizationService: DynamicMutationAuthorizationService;
  private readonly readService: DynamicRepositoryReadService;

  constructor({
    context,
    tableName,
    queryBuilderService,
    runtimeMetadataSchemaRouterService,
    policyService,
    tableValidationService,
    guardValidationService,
    eventEmitter,
    userRevocationService,
    bcryptService,
    flowQueueMaintenanceService,
    runtimeRegistryService,
    enforceFieldPermission,
    runtimeSchemaActivationGateService,
  }: {
    context: TDynamicContext;
    tableName: string;
    queryBuilderService: QueryBuilderService;
    runtimeMetadataSchemaRouterService: RuntimeMetadataSchemaRouterService;
    policyService: PolicyService;
    tableValidationService: DynamicApiTableValidationService;
    guardValidationService: GuardValidationService;
    eventEmitter: EventEmitter2;
    fieldPermissionCacheBuilder?: unknown;
    bcryptService: BcryptService;
    flowQueueMaintenanceService?: FlowQueueMaintenanceService;
    userRevocationService?: UserRevocationService;
    runtimeRegistryService: RuntimeRegistryService;
    enforceFieldPermission?: boolean;
    runtimeSchemaActivationGateService?: RuntimeSchemaActivationGateService;
  }) {
    this.context = context;
    this.tableName = tableName;
    this.queryBuilderService = queryBuilderService;
    this.runtimeMetadataSchemaRouterService =
      runtimeMetadataSchemaRouterService;
    this.tableValidationService = tableValidationService;
    this.eventEmitter = eventEmitter;
    this.runtimeRegistryService = runtimeRegistryService;
    this.readAuthorizationService = new DynamicReadAuthorizationService({
      runtimeRegistryService,
    });
    this.readService = new DynamicRepositoryReadService({
      context,
      enforceFieldPermission: enforceFieldPermission === true,
      queryBuilderService,
      readAuthorizationService: this.readAuthorizationService,
      runtimeRegistryService,
      tableName,
    });
    this.mutationAuthorizationService =
      new DynamicMutationAuthorizationService({
        context,
        enforceFieldPermission: enforceFieldPermission === true,
        policyService,
        queryBuilderService,
        runtimeRegistryService,
        tableName,
      });
    this.runtimeSchemaActivationGateService =
      runtimeSchemaActivationGateService;
    this.routeRouter = new TableRouteRouter(
      new DynamicTableRouteHandlerService({
        bcryptService,
        flowQueueMaintenanceService,
        guardValidationService,
        queryBuilderService,
        runtimeMetadataSchemaRouterService,
        userRevocationService,
      }),
    );
  }

  async init() {
    this.tableMetadata = await this.lookupActiveTableByName(this.tableName);
  }

  private async ensureInit() {
    if (!this.tableMetadata) {
      this.tableMetadata = await this.lookupActiveTableByName(this.tableName);
    }
  }

  private async getActiveMetadata(): Promise<any> {
    return this.runtimeRegistryService.requireMetadata();
  }

  private async lookupActiveTableByName(
    tableName: string,
  ): Promise<any | null> {
    return this.runtimeRegistryService.lookupTableByName(tableName);
  }

  private getIdField(): string {
    return this.queryBuilderService.getPkField();
  }

  async find(opt: DynamicReadOptions = {}) {
    return this.readService.find(opt);
  }

  async exists(filter?: unknown): Promise<boolean> {
    return this.readService.exists(filter);
  }

  async create(opt: {
    data: any;
    fields?: string | string[];
    batch?: boolean;
  }): Promise<any | DynamicBatchCreateResult> {
    await this.ensureInit();
    const startedAt = Date.now();
    const writeMeta = {
      table: this.tableName,
      operation: 'create',
      batch: opt.batch === true,
    };
    logMemory(this.logger, 'dynamic create start', writeMeta);
    try {
      const { data, fields, batch } = opt;
      if (batch) {
        const result = await this.createBatch(data);
        logMemory(this.logger, 'dynamic create batch done', {
          ...writeMeta,
          durationMs: Date.now() - startedAt,
          count: result.count,
        });
        this.emitTableMutation('create', undefined, undefined);
        return result;
      }

      const strategy = this.routeRouter.getStrategy(this.tableName);
      const body = await this.prepareCreateBody(data, strategy);
      logMemory(this.logger, 'dynamic create body prepared', {
        ...writeMeta,
        bodyKeys: Object.keys(body).length,
      });

      const ctx: MutationContext = {
        tableName: this.tableName,
        id: body.id ?? body._id,
        body,
        existing: null,
      };

      if (strategy.kind === 'schema') {
        const mutation = await this.runtimeMetadataSchemaRouterService.create({
          tableName: this.tableName as RuntimeSchemaMetadataTable,
          data: body,
          context: this.context,
        });
        if (mutation.preview) return { data: [mutation.preview] };
        await this.activateRuntimeSchemaMutation(mutation, {
          ids: mutation.recordId == null ? undefined : [mutation.recordId],
        });
        return this.find({
          filter: {
            [this.getIdField()]: { _eq: mutation.recordId },
          },
          fields,
        });
      }

      if (strategy.kind === 'table') {
        body.isSystem = false;
        const mutation =
          await this.runtimeMetadataSchemaRouterService.createTable({
            body: body as any,
            context: this.context,
          });
        if (mutation.preview) return { data: [mutation.preview] };
        const idValue = mutation.recordId;
        await this.activateRuntimeSchemaMutation(mutation, {
          ids: idValue == null ? undefined : [idValue],
        });
        const result = await this.find({
          filter: { [this.getIdField()]: { _eq: idValue } },
          fields,
        });
        return result;
      }

      return this.mutationLifecycleService.run({
        context: ctx,
        persist: async () => {
          const inserted = await this.executeCreateBody(body);
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
          const result = await this.find({
            filter: { [this.getIdField()]: { _eq: createdId } },
            fields,
          });
          logMemory(this.logger, 'dynamic create result loaded', {
            ...writeMeta,
            durationMs: Date.now() - startedAt,
          });
          return result;
        },
        reload: () => this.reload({ ids: [ctx.id] }),
        afterReload: async () => {
          logMemory(this.logger, 'dynamic create done', {
            ...writeMeta,
            durationMs: Date.now() - startedAt,
          });
        },
        emit: () => this.emitTableMutation('create', [ctx.id], body),
        recover: async (_context, { inserted, createdId }, error) => {
          const databaseError = classifyDynamicDatabaseError(
            error,
            this.queryBuilderService.getDatabaseType(),
          );
          if (databaseError.kind !== 'postgres_incompatible_operator') {
            throw error;
          }
          await this.reload({ ids: [createdId] });
          logMemory(this.logger, 'dynamic create done', {
            ...writeMeta,
            durationMs: Date.now() - startedAt,
            fallbackResult: true,
          });
          this.emitTableMutation('create', [createdId], body);
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

  private async createBatch(data: any): Promise<DynamicBatchCreateResult> {
    if (this.runtimeMetadataSchemaRouterService.handles(this.tableName)) {
      throw new BadRequestException(
        `Batch create is not supported for ${this.tableName}. Use single-record operations.`,
      );
    }
    if (this.tableName === 'enfyra_table') {
      throw new BadRequestException('Batch create is not supported for tables');
    }

    const rows = Array.isArray(data) ? data : [data];
    if (rows.length === 0) {
      throw new BadRequestException('data must contain at least one record');
    }

    const preparedRows: Record<string, any>[] = [];
    const strategy = this.routeRouter.getStrategy(this.tableName);
    for (const row of rows) {
      preparedRows.push(await this.prepareCreateBody(row, strategy));
    }

    for (const body of preparedRows) {
      await (this.queryBuilderService as any).insert(this.tableName, body, {
        batch: true,
      });
    }

    return {
      accepted: true,
      batch: true,
      count: preparedRows.length,
    };
  }

  private async prepareCreateBody(
    raw: any,
    strategy?: TableRouteStrategy,
  ): Promise<Record<string, any>> {
    if (!raw || typeof raw !== 'object' || Array.isArray(raw)) {
      throw new BadRequestException('data is required and must be an object');
    }

    const body = { ...raw };
    await this.mutationAuthorizationService.assertDirectFieldPermission(
      'create',
      body,
    );

    await this.tableValidationService.assertTableValid({
      operation: 'create',
      tableName: this.tableName,
      tableMetadata: this.tableMetadata,
    });
    await this.mutationAuthorizationService.assertMutationSafety(
      'create',
      body,
      null,
    );
    await strategy?.normalizeCreate?.(body);
    Object.assign(
      body,
      this.mutationPreparationService.normalizeCreate(this.tableName, body),
    );
    if (this.tableName === 'enfyra_flow_step') {
      Object.assign(body, this.mutationPreparationService.normalizeFlowStep(body));
    }
    if (body.id !== undefined) {
      delete body.id;
    }
    if (body._id !== undefined) {
      delete body._id;
    }
    return body;
  }

  private async executeCreateBody(body: Record<string, any>): Promise<any> {
    return this.mutationAuthorizationService.runWithFieldPermissionCheck(() =>
      this.mutationAuthorizationService.runWithMutationPolicy(() =>
        this.queryBuilderService.insert(this.tableName, body),
      ),
    );
  }

  async update(opt: {
    id: string | number;
    data: any;
    fields?: string | string[];
  }) {
    await this.ensureInit();
    const startedAt = Date.now();
    const writeMeta = {
      table: this.tableName,
      operation: 'update',
      id: opt.id,
    };
    logMemory(this.logger, 'dynamic update start', writeMeta);
    try {
      const { id, fields } = opt;
      const originalBody = opt.data;
      const body = this.mutationPreparationService.prepareUpdateBody(
        originalBody,
        this.tableMetadata,
      );
      logMemory(this.logger, 'dynamic update body stripped', {
        ...writeMeta,
        bodyKeys:
          body && typeof body === 'object' ? Object.keys(body).length : 0,
      });
      const existsResult = await this.find({
        filter: { [this.getIdField()]: { _eq: id } },
      });
      const canonicalExistsResult = await this.queryBuilderService.find({
        table: this.tableName,
        fields: '*',
        filter: { [this.getIdField()]: { _eq: id } },
        limit: 1,
      });
      const exists = canonicalExistsResult?.data?.[0] ?? existsResult?.data?.[0];
      if (!exists) throw new BadRequestException(`id ${id} is not exists!`);
      logMemory(this.logger, 'dynamic update existing loaded', {
        ...writeMeta,
        durationMs: Date.now() - startedAt,
      });

      await this.mutationAuthorizationService.assertDirectFieldPermission(
        'update',
        body,
        exists,
      );

      await this.tableValidationService.assertTableValid({
        operation: 'update',
        tableName: this.tableName,
        tableMetadata: this.tableMetadata,
      });
      await this.mutationAuthorizationService.assertMutationSafety(
        'update',
        body,
        exists,
      );
      const strategy = this.routeRouter.getStrategy(this.tableName);
      await strategy.normalizeUpdate?.(body, exists, id);
      Object.assign(
        body,
        this.mutationPreparationService.normalizeUpdate(
          this.tableName,
          body,
          exists,
        ),
      );
      if (this.tableName === 'enfyra_flow_step') {
        const normalizedFlowStep = this.mutationPreparationService.normalizeFlowStep({
          ...exists,
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
        if ('config' in normalizedFlowStep) {
          body.config = normalizedFlowStep.config;
        }
      }

      const ctx: MutationContext = {
        tableName: this.tableName,
        id,
        body,
        existing: exists,
      };

      if (strategy.kind === 'table') {
        const mutation =
          await this.runtimeMetadataSchemaRouterService.updateTable({
            tableId: id,
            body: body as any,
            existing: exists,
            context: this.context,
          });
        if (mutation.preview) return { data: [mutation.preview] };
        const tableId = mutation.recordId ?? id;
        await this.activateRuntimeSchemaMutation(mutation, {
          ids: [tableId],
        });
        const result = await this.find({
          filter: { [this.getIdField()]: { _eq: tableId } },
          fields,
        });
        return result;
      }
      if (strategy.kind === 'schema') {
        const mutation = await this.runtimeMetadataSchemaRouterService.update({
          tableName: this.tableName as RuntimeSchemaMetadataTable,
          recordId: id,
          data: body,
          existing: exists,
          context: this.context,
        });
        if (mutation.preview) return { data: [mutation.preview] };
        await this.activateRuntimeSchemaMutation(mutation, { ids: [id] });
        return this.find({
          filter: { [this.getIdField()]: { _eq: id } },
          fields,
        });
      }
      return this.mutationLifecycleService.run({
        context: ctx,
        persist: () =>
          this.mutationAuthorizationService.runWithFieldPermissionCheck(() =>
            this.mutationAuthorizationService.runWithMutationPolicy(() =>
              this.queryBuilderService.update(this.tableName, id, body),
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
          const result = await this.find({
            filter: { [this.getIdField()]: { _eq: id } },
            fields,
          });
          logMemory(this.logger, 'dynamic update result loaded', {
            ...writeMeta,
            durationMs: Date.now() - startedAt,
          });
          return result;
        },
        reload: () => this.reload({ ids: [id] }),
        afterReload: async () => {
          logMemory(this.logger, 'dynamic update done', {
            ...writeMeta,
            durationMs: Date.now() - startedAt,
          });
          await strategy.afterUpdateReload?.(ctx);
        },
        emit: () => this.emitTableMutation('update', [id], body),
      });
    } catch (error: any) {
      if (isCustomException(error)) {
        throw error;
      }
      throw new BadRequestException(error.message);
    }
  }

  async delete(opt: { id: string | number }) {
    await this.ensureInit();
    const startedAt = Date.now();
    const writeMeta = {
      table: this.tableName,
      operation: 'delete',
      id: opt.id,
    };
    logMemory(this.logger, 'dynamic delete start', writeMeta);
    try {
      const { id } = opt;
      const idField = this.getIdField();
      const existsResult = await this.find({
        filter: { [idField]: { _eq: id } },
      });
      const exists = existsResult?.data?.[0];
      if (!exists) throw new BadRequestException(`id ${id} is not exists!`);
      logMemory(this.logger, 'dynamic delete existing loaded', {
        ...writeMeta,
        durationMs: Date.now() - startedAt,
      });
      await this.tableValidationService.assertTableValid({
        operation: 'delete',
        tableName: this.tableName,
        tableMetadata: this.tableMetadata,
      });
      await this.mutationAuthorizationService.assertMutationSafety(
        'delete',
        {},
        exists,
      );
      const strategy = this.routeRouter.getStrategy(this.tableName);
      const ctx: MutationContext = {
        tableName: this.tableName,
        id,
        body: {},
        existing: exists,
      };
      if (strategy.kind === 'table') {
        const mutation =
          await this.runtimeMetadataSchemaRouterService.deleteTable({
            tableId: id,
            existing: exists,
            context: this.context,
          });
        if (mutation.preview) return { data: [mutation.preview] };
        await this.activateRuntimeSchemaMutation(mutation, { ids: [id] });
        return { message: 'Success', statusCode: 200 };
      }
      if (strategy.kind === 'schema') {
        const mutation = await this.runtimeMetadataSchemaRouterService.delete({
          tableName: this.tableName as RuntimeSchemaMetadataTable,
          recordId: id,
          existing: exists,
          context: this.context,
        });
        if (mutation.preview) return { data: [mutation.preview] };
        await this.activateRuntimeSchemaMutation(mutation, { ids: [id] });
        return { message: 'Success', statusCode: 200 };
      }
      return this.mutationLifecycleService.run({
        context: ctx,
        persist: () =>
          this.mutationAuthorizationService.runWithMutationPolicy(() =>
            this.queryBuilderService.delete(this.tableName, id),
          ),
        afterWrite: async () => {
          await strategy.afterDeleteWrite?.(ctx);
        },
        buildResult: () => ({
          message: 'Delete successfully!',
          statusCode: 200,
        }),
        reload: () => this.reload({ ids: [id] }),
        afterReload: async () => {
          logMemory(this.logger, 'dynamic delete done', {
            ...writeMeta,
            durationMs: Date.now() - startedAt,
          });
          await strategy.afterDeleteReload?.(ctx);
        },
        emit: () => this.emitTableMutation('delete', [id]),
      });
    } catch (error: any) {
      if (isCustomException(error)) {
        throw error;
      }
      throw new BadRequestException(error.message);
    }
  }

  private async activateRuntimeSchemaMutation(
    mutation: RuntimeMetadataSchemaMutationResult,
    opts: { ids?: (string | number)[] },
  ): Promise<void> {
    const mutationId = mutation.mutationId;
    if (!mutationId) {
      await this.reload({
        ids: opts.ids,
        affectedTables: mutation.affectedTables,
        critical: true,
        tableRenames: mutation.tableRenames,
      });
      return;
    }

    this.runtimeSchemaActivationGateService?.begin(mutationId);
    let lastError: unknown;
    for (let attempt = 1; attempt <= 3; attempt += 1) {
      try {
        await this.reload({
          ids: opts.ids,
          affectedTables: mutation.affectedTables,
          critical: true,
          tableRenames: mutation.tableRenames,
        });
        await this.runtimeMetadataSchemaRouterService.markActivated(mutationId);
        this.runtimeSchemaActivationGateService?.complete(mutationId);
        return;
      } catch (error) {
        lastError = error;
        if (attempt < 3) {
          await new Promise((resolve) => setTimeout(resolve, attempt * 100));
        }
      }
    }

    this.runtimeSchemaActivationGateService?.fail(mutationId, lastError);
    const message =
      lastError instanceof Error ? lastError.message : String(lastError);
    throw new Error(
      `Schema mutation committed but cache activation failed; instance fenced: ${message}`,
    );
  }

  private async reload(opts?: {
    ids?: (string | number)[];
    affectedTables?: string[];
    tableRenames?: TCacheInvalidationPayload['tableRenames'];
    critical?: boolean;
  }) {
    const payload: TCacheInvalidationPayload = {
      table: this.tableName,
      action: 'reload',
      timestamp: Date.now(),
      scope: opts?.ids?.length ? 'partial' : 'full',
      ids: opts?.ids,
      affectedTables: opts?.affectedTables,
      critical: opts?.critical,
      tableRenames: opts?.tableRenames,
    };
    if (typeof this.eventEmitter.emitAsync === 'function') {
      await this.eventEmitter.emitAsync(CACHE_EVENTS.INVALIDATE, payload);
      return;
    }
    this.eventEmitter.emit(CACHE_EVENTS.INVALIDATE, payload);
  }

  private emitTableMutation(
    action: 'create' | 'update' | 'delete',
    ids?: (string | number)[],
    data?: any,
  ) {
    this.eventEmitter.emit(DATA_EVENTS.TABLE_MUTATION, {
      table: this.tableName,
      action,
      ids,
      data,
      userId: this.context?.$user?.id ?? null,
    });
  }

}
