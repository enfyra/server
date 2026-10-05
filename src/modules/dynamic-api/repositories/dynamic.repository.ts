import { EventEmitter2 } from 'eventemitter2';
import { QueryBuilderService } from '@enfyra/kernel';
import { RuntimeMetadataSchemaRouterService } from '../../table-management';
import { PolicyService } from '../../../domain/policy';
import { DynamicApiTableValidationService } from '../services/table-validation.service';
import { DynamicReadAuthorizationService } from '../services/dynamic-read-authorization.service';
import { DynamicMutationPreparationService } from '../services/dynamic-mutation-preparation.service';
import { DynamicMutationLifecycleService } from '../services/dynamic-mutation-lifecycle.service';
import { DynamicMutationAuthorizationService } from '../services/dynamic-mutation-authorization.service';
import { DynamicSchemaActivationService } from '../services/dynamic-schema-activation.service';
import { DynamicBatchCreationService } from '../services/dynamic-batch-creation.service';
import { DynamicRepositoryReadService } from '../services/dynamic-repository-read.service';
import { DynamicTableRouteHandlerService } from '../services/dynamic-table-route-handler.service';
import type {
  DynamicBatchCreateResult,
  DynamicMutationManyDeleteResult,
  DynamicMutationManyResult,
  DynamicMutationRuntime,
} from '../types/dynamic-mutation-lifecycle.types';
import type { DynamicReadOptions } from '../types/dynamic-read.types';
import type { DynamicLockedUpdateOptions, DynamicUpdateOptions } from '../types/dynamic-locked-update.types';
import { BadRequestException } from '../../../domain/exceptions';
import type { GuardValidationService } from '../services/guard-validation.service';
import type { TDynamicContext } from '../../../shared/types';
import {
  CACHE_EVENTS,
  DATA_EVENTS,
} from '../../../shared/utils/cache-events.constants';
import { TCacheInvalidationPayload } from '../../../shared/types/cache.types';
import type {
  BcryptService,
  UserRevocationService,
} from '../../../domain/auth';
import type { FlowQueueMaintenanceService } from '../../flow';
import type { RuntimeRegistryService } from '../../../engines/cache/services/runtime-registry.service';
import type { RuntimeSchemaActivationGateService } from '../../table-management';
import { TableRouteRouter } from './table-route.router';
import { deferDynamicTransactionEffect, runWithDeferredDynamicTransactionEffects } from '../../../shared/utils/dynamic-transaction-effects.util';

export class DynamicRepository {
  public context: TDynamicContext;
  private tableName: string;
  private queryBuilderService: QueryBuilderService;
  private runtimeMetadataSchemaRouterService: RuntimeMetadataSchemaRouterService;
  private tableValidationService: DynamicApiTableValidationService;
  private eventEmitter: EventEmitter2;
  private runtimeRegistryService: RuntimeRegistryService;
  private tableMetadata: unknown;
  private readonly runtimeSchemaActivationGateService?: RuntimeSchemaActivationGateService;
  private readonly routeRouter: TableRouteRouter;
  private readonly readAuthorizationService: DynamicReadAuthorizationService;
  private readonly mutationPreparationService =
    new DynamicMutationPreparationService();
  private readonly mutationLifecycleService =
    new DynamicMutationLifecycleService();
  private readonly mutationAuthorizationService: DynamicMutationAuthorizationService;
  private readonly readService: DynamicRepositoryReadService;
  private readonly schemaActivationService: DynamicSchemaActivationService;
  private readonly batchCreationService: DynamicBatchCreationService;

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
    this.mutationAuthorizationService = new DynamicMutationAuthorizationService(
      {
        context,
        enforceFieldPermission: enforceFieldPermission === true,
        policyService,
        queryBuilderService,
        runtimeRegistryService,
        tableName,
      },
    );
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
    this.schemaActivationService = new DynamicSchemaActivationService(
      this.runtimeMetadataSchemaRouterService,
      this.runtimeSchemaActivationGateService,
      this.eventEmitter,
      this.tableName,
    );
    this.batchCreationService = new DynamicBatchCreationService(
      this.mutationPreparationService,
      this.mutationAuthorizationService,
      this.tableValidationService,
      this.routeRouter,
      this.runtimeMetadataSchemaRouterService,
      this.queryBuilderService,
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

  private async lookupActiveTableByName(
    tableName: string,
  ): Promise<unknown | null> {
    return this.runtimeRegistryService.lookupTableByName(tableName);
  }

  private getIdField(): string {
    return this.queryBuilderService.getPkField();
  }

  private getMutationRuntime(): DynamicMutationRuntime {
    return {
      find: (options) => this.find(options),
      getIdField: () => this.getIdField(),
      reload: (options) => this.reload(options),
      emit: (action, ids, data) => this.emitTableMutation(action, ids, data),
    };
  }

  async find(opt: DynamicReadOptions = {}) {
    return this.readService.find(opt);
  }

  async aggregate(opt: Record<string, unknown>) {
    return this.readService.aggregate(opt);
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
    return this.mutationLifecycleService.create({
      runtime: this.getMutationRuntime(),
      routeRouter: this.routeRouter,
      runtimeMetadataSchemaRouterService:
        this.runtimeMetadataSchemaRouterService,
      queryBuilderService: this.queryBuilderService,
      mutationPreparationService: this.mutationPreparationService,
      mutationAuthorizationService: this.mutationAuthorizationService,
      tableValidationService: this.tableValidationService,
      schemaActivationService: this.schemaActivationService,
      batchCreationService: this.batchCreationService,
      tableName: this.tableName,
      tableMetadata: this.tableMetadata,
      context: this.context,
      data: opt.data,
      fields: opt.fields,
      batch: opt.batch,
    });
  }

  async createMany(opt: {
    data: Array<Record<string, unknown>>;
    fields?: string | string[];
  }): Promise<DynamicMutationManyResult> {
    await this.ensureInit();
    return this.mutationLifecycleService.createMany({
      runtime: this.getMutationRuntime(),
      routeRouter: this.routeRouter,
      queryBuilderService: this.queryBuilderService,
      mutationPreparationService: this.mutationPreparationService,
      mutationAuthorizationService: this.mutationAuthorizationService,
      tableValidationService: this.tableValidationService,
      schemaActivationService: this.schemaActivationService,
      runtimeMetadataSchemaRouterService:
        this.runtimeMetadataSchemaRouterService,
      tableName: this.tableName,
      tableMetadata: this.tableMetadata,
      context: this.context,
      data: opt.data,
      fields: opt.fields,
    });
  }

  async update(opt: DynamicUpdateOptions) {
    await this.ensureInit();
    return this.executeUpdate(opt, this.getMutationRuntime());
  }

  private executeUpdate(opt: DynamicUpdateOptions, runtime: DynamicMutationRuntime) {
    return this.mutationLifecycleService.update({
      runtime,
      routeRouter: this.routeRouter,
      runtimeMetadataSchemaRouterService:
        this.runtimeMetadataSchemaRouterService,
      queryBuilderService: this.queryBuilderService,
      mutationPreparationService: this.mutationPreparationService,
      mutationAuthorizationService: this.mutationAuthorizationService,
      tableValidationService: this.tableValidationService,
      tableName: this.tableName,
      tableMetadata: this.tableMetadata,
      context: this.context,
      id: opt.id,
      data: opt.data,
      fields: opt.fields,
      schemaActivationService: this.schemaActivationService,
    });
  }

  async updateLocked(opt: DynamicLockedUpdateOptions): Promise<unknown> {
    await this.ensureInit();
    if (!opt || !['string', 'number'].includes(typeof opt.id) || opt.id === '') {
      throw new BadRequestException('updateLocked requires a record id');
    }
    if (typeof opt.data !== 'function') throw new BadRequestException('updateLocked data must be a callback');
    const strategy = this.routeRouter.getStrategy(this.tableName);
    if (strategy.kind !== 'generic' || strategy.normalizeUpdate || strategy.afterUpdateWrite || strategy.afterUpdateReload) {
      throw new BadRequestException('updateLocked supports plain generic tables only');
    }
    return runWithDeferredDynamicTransactionEffects(this.context, async () => {
      const result = await this.find({ filter: { [this.getIdField()]: { _eq: opt.id } }, fields: '*', deep: {}, limit: 1, page: 1, sort: this.getIdField(), meta: [] });
      const current = result?.data?.[0];
      if (!current) throw new BadRequestException('updateLocked record not found');
      const data = await opt.data(current);
      if (!data || typeof data !== 'object' || Array.isArray(data)) {
        throw new BadRequestException('updateLocked data callback must return a record patch');
      }
      return this.executeUpdate({ id: opt.id, data, fields: opt.fields }, {
        ...this.getMutationRuntime(),
        find: (options) => this.find({ ...options, page: 1, limit: 1 }),
      });
    }, (attempt) => this.queryBuilderService.runWithLockedRecord(this.tableName, opt.id, attempt));
  }

  async updateMany(opt: {
    ids: Array<string | number>;
    data: Record<string, unknown>;
    fields?: string | string[];
  }): Promise<DynamicMutationManyResult> {
    await this.ensureInit();
    return this.mutationLifecycleService.updateMany({
      runtime: this.getMutationRuntime(),
      routeRouter: this.routeRouter,
      queryBuilderService: this.queryBuilderService,
      mutationPreparationService: this.mutationPreparationService,
      mutationAuthorizationService: this.mutationAuthorizationService,
      tableValidationService: this.tableValidationService,
      schemaActivationService: this.schemaActivationService,
      runtimeMetadataSchemaRouterService:
        this.runtimeMetadataSchemaRouterService,
      tableName: this.tableName,
      tableMetadata: this.tableMetadata,
      context: this.context,
      ids: opt.ids,
      data: opt.data,
      fields: opt.fields,
    });
  }

  async delete(opt: { id: string | number }) {
    await this.ensureInit();
    return this.mutationLifecycleService.delete({
      runtime: this.getMutationRuntime(),
      routeRouter: this.routeRouter,
      runtimeMetadataSchemaRouterService:
        this.runtimeMetadataSchemaRouterService,
      queryBuilderService: this.queryBuilderService,
      mutationAuthorizationService: this.mutationAuthorizationService,
      tableValidationService: this.tableValidationService,
      tableName: this.tableName,
      tableMetadata: this.tableMetadata,
      context: this.context,
      id: opt.id,
      schemaActivationService: this.schemaActivationService,
    });
  }

  async deleteMany(opt: {
    ids: Array<string | number>;
  }): Promise<DynamicMutationManyDeleteResult> {
    await this.ensureInit();
    return this.mutationLifecycleService.deleteMany({
      runtime: this.getMutationRuntime(),
      routeRouter: this.routeRouter,
      queryBuilderService: this.queryBuilderService,
      mutationAuthorizationService: this.mutationAuthorizationService,
      tableValidationService: this.tableValidationService,
      schemaActivationService: this.schemaActivationService,
      runtimeMetadataSchemaRouterService:
        this.runtimeMetadataSchemaRouterService,
      tableName: this.tableName,
      tableMetadata: this.tableMetadata,
      context: this.context,
      ids: opt.ids,
    });
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
    const emit = async () => {
      if (typeof this.eventEmitter.emitAsync === 'function') {
        await this.eventEmitter.emitAsync(CACHE_EVENTS.INVALIDATE, payload);
        return;
      }
      this.eventEmitter.emit(CACHE_EVENTS.INVALIDATE, payload);
    };
    if (deferDynamicTransactionEffect(this.context, emit)) return;
    await emit();
  }

  private emitTableMutation(
    action: 'create' | 'update' | 'delete',
    ids?: (string | number)[],
    data?: any,
  ) {
    const payload = {
      table: this.tableName,
      action,
      ids,
      data,
      userId: this.context?.$user?.id ?? this.context?.$user?._id ?? null,
    };
    const emit = () => {
      this.eventEmitter.emit(DATA_EVENTS.TABLE_MUTATION, payload);
    };
    if (deferDynamicTransactionEffect(this.context, emit)) return;
    emit();
  }
}
