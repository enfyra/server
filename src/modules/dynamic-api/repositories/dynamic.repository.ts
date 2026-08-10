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
  DynamicMutationRuntime,
} from '../types/dynamic-mutation-lifecycle.types';
import type { DynamicReadOptions } from '../types/dynamic-read.types';
import type { GuardValidationService } from '../services/guard-validation.service';
import { TDynamicContext } from '../../../shared/types';
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

  async update(opt: {
    id: string | number;
    data: any;
    fields?: string | string[];
  }) {
    await this.ensureInit();
    return this.mutationLifecycleService.update({
      runtime: this.getMutationRuntime(),
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
