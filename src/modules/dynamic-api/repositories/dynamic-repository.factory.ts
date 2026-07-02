import { EventEmitter2 } from 'eventemitter2';
import { DynamicRepository } from './dynamic.repository';
import { TableHandlerService } from '../../table-management';
import { QueryBuilderService } from '@enfyra/kernel';
import { PolicyService } from '../../../domain/policy';
import { DynamicApiTableValidationService } from '../services/table-validation.service';
import { UserRevocationService } from '../../../domain/auth';
import { TDynamicContext } from '../../../shared/types';
import { FlowQueueMaintenanceService } from '../../flow';
import type { RuntimeRegistryService } from '../../../engines/cache/services/runtime-registry.service';

export class DynamicRepositoryFactory {
  private readonly tableHandlerService: TableHandlerService;
  private readonly queryBuilderService: QueryBuilderService;
  private readonly policyService: PolicyService;
  private readonly tableValidationService: DynamicApiTableValidationService;
  private readonly userRevocationService: UserRevocationService;
  private readonly flowQueueMaintenanceService: FlowQueueMaintenanceService;
  private readonly runtimeRegistryService: RuntimeRegistryService;
  private readonly eventEmitter: EventEmitter2;

  constructor(deps: {
    tableHandlerService: TableHandlerService;
    queryBuilderService: QueryBuilderService;
    policyService: PolicyService;
    tableValidationService: DynamicApiTableValidationService;
    userRevocationService: UserRevocationService;
    flowQueueMaintenanceService: FlowQueueMaintenanceService;
    runtimeRegistryService: RuntimeRegistryService;
    eventEmitter: EventEmitter2;
  }) {
    this.tableHandlerService = deps.tableHandlerService;
    this.queryBuilderService = deps.queryBuilderService;
    this.policyService = deps.policyService;
    this.tableValidationService = deps.tableValidationService;
    this.userRevocationService = deps.userRevocationService;
    this.flowQueueMaintenanceService = deps.flowQueueMaintenanceService;
    this.runtimeRegistryService = deps.runtimeRegistryService;
    this.eventEmitter = deps.eventEmitter;
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
      queryBuilderService: this.queryBuilderService,
      policyService: this.policyService,
      tableValidationService: this.tableValidationService,
      userRevocationService: this.userRevocationService,
      flowQueueMaintenanceService: this.flowQueueMaintenanceService,
      runtimeRegistryService: this.runtimeRegistryService,
      eventEmitter: this.eventEmitter,
    });
  }
}
