import { EventEmitter2 } from 'eventemitter2';
import { DynamicRepository } from './dynamic.repository';
import {
  RuntimeMetadataSchemaRouterService,
  TableHandlerService,
} from '../../table-management';
import { QueryBuilderService } from '@enfyra/kernel';
import { PolicyService } from '../../../domain/policy';
import { DynamicApiTableValidationService } from '../services/table-validation.service';
import { GuardValidationService } from '../services/guard-validation.service';
import { type BcryptService, UserRevocationService } from '../../../domain/auth';
import { TDynamicContext } from '../../../shared/types';
import { FlowQueueMaintenanceService } from '../../flow';
import type { RuntimeRegistryService } from '../../../engines/cache/services/runtime-registry.service';
import type { RuntimeSchemaActivationGateService } from '../../table-management';

export class DynamicRepositoryFactory {
  private readonly tableHandlerService: TableHandlerService;
  private readonly runtimeMetadataSchemaRouterService: RuntimeMetadataSchemaRouterService;
  private readonly queryBuilderService: QueryBuilderService;
  private readonly policyService: PolicyService;
  private readonly tableValidationService: DynamicApiTableValidationService;
  private readonly guardValidationService: GuardValidationService;
  private readonly userRevocationService: UserRevocationService;
  private readonly bcryptService: BcryptService;
  private readonly flowQueueMaintenanceService: FlowQueueMaintenanceService;
  private readonly runtimeRegistryService: RuntimeRegistryService;
  private readonly eventEmitter: EventEmitter2;
  private readonly runtimeSchemaActivationGateService: RuntimeSchemaActivationGateService;

  constructor(deps: {
    tableHandlerService: TableHandlerService;
    runtimeMetadataSchemaRouterService: RuntimeMetadataSchemaRouterService;
    queryBuilderService: QueryBuilderService;
    policyService: PolicyService;
    tableValidationService: DynamicApiTableValidationService;
    guardValidationService: GuardValidationService;
    userRevocationService: UserRevocationService;
    bcryptService: BcryptService;
    flowQueueMaintenanceService: FlowQueueMaintenanceService;
    runtimeRegistryService: RuntimeRegistryService;
    eventEmitter: EventEmitter2;
    runtimeSchemaActivationGateService: RuntimeSchemaActivationGateService;
  }) {
    this.tableHandlerService = deps.tableHandlerService;
    this.runtimeMetadataSchemaRouterService =
      deps.runtimeMetadataSchemaRouterService;
    this.queryBuilderService = deps.queryBuilderService;
    this.policyService = deps.policyService;
    this.tableValidationService = deps.tableValidationService;
    this.guardValidationService = deps.guardValidationService;
    this.userRevocationService = deps.userRevocationService;
    this.bcryptService = deps.bcryptService;
    this.flowQueueMaintenanceService = deps.flowQueueMaintenanceService;
    this.runtimeRegistryService = deps.runtimeRegistryService;
    this.eventEmitter = deps.eventEmitter;
    this.runtimeSchemaActivationGateService =
      deps.runtimeSchemaActivationGateService;
  }

  create(
    tableName: string,
    context: TDynamicContext,
    enforceFieldPermission?: boolean,
  ): DynamicRepository {
    return new DynamicRepository({
      tableName,
      context,
      enforceFieldPermission,
      tableHandlerService: this.tableHandlerService,
      runtimeMetadataSchemaRouterService:
        this.runtimeMetadataSchemaRouterService,
      queryBuilderService: this.queryBuilderService,
      policyService: this.policyService,
      tableValidationService: this.tableValidationService,
      guardValidationService: this.guardValidationService,
      userRevocationService: this.userRevocationService,
      bcryptService: this.bcryptService,
      flowQueueMaintenanceService: this.flowQueueMaintenanceService,
      runtimeRegistryService: this.runtimeRegistryService,
      eventEmitter: this.eventEmitter,
      runtimeSchemaActivationGateService:
        this.runtimeSchemaActivationGateService,
    });
  }
}
