import { EventEmitter2 } from 'eventemitter2';
import { DynamicRepository } from './dynamic.repository';
import { TableHandlerService } from '../../table-management';
import { QueryBuilderService } from '../../../kernel/query';
import {
  MetadataCacheService,
  SettingCacheService,
  FieldPermissionCacheService,
} from '../../../engines/cache';
import { PolicyService } from '../../../domain/policy';
import { DynamicApiTableValidationService } from '../services/table-validation.service';
import { UserRevocationService } from '../../../domain/auth';
import { TDynamicContext } from '../../../shared/types';
import { FlowQueueMaintenanceService } from '../../flow';

export class DynamicRepositoryFactory {
  private readonly tableHandlerService: TableHandlerService;
  private readonly queryBuilderService: QueryBuilderService;
  private readonly metadataCacheService: MetadataCacheService;
  private readonly policyService: PolicyService;
  private readonly tableValidationService: DynamicApiTableValidationService;
  private readonly settingCacheService: SettingCacheService;
  private readonly fieldPermissionCacheService: FieldPermissionCacheService;
  private readonly userRevocationService: UserRevocationService;
  private readonly flowQueueMaintenanceService: FlowQueueMaintenanceService;
  private readonly eventEmitter: EventEmitter2;

  constructor(deps: {
    tableHandlerService: TableHandlerService;
    queryBuilderService: QueryBuilderService;
    metadataCacheService: MetadataCacheService;
    policyService: PolicyService;
    tableValidationService: DynamicApiTableValidationService;
    settingCacheService: SettingCacheService;
    fieldPermissionCacheService: FieldPermissionCacheService;
    userRevocationService: UserRevocationService;
    flowQueueMaintenanceService: FlowQueueMaintenanceService;
    eventEmitter: EventEmitter2;
  }) {
    this.tableHandlerService = deps.tableHandlerService;
    this.queryBuilderService = deps.queryBuilderService;
    this.metadataCacheService = deps.metadataCacheService;
    this.policyService = deps.policyService;
    this.tableValidationService = deps.tableValidationService;
    this.settingCacheService = deps.settingCacheService;
    this.fieldPermissionCacheService = deps.fieldPermissionCacheService;
    this.userRevocationService = deps.userRevocationService;
    this.flowQueueMaintenanceService = deps.flowQueueMaintenanceService;
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
      metadataCacheService: this.metadataCacheService,
      policyService: this.policyService,
      tableValidationService: this.tableValidationService,
      settingCacheService: this.settingCacheService,
      fieldPermissionCacheService: this.fieldPermissionCacheService,
      userRevocationService: this.userRevocationService,
      flowQueueMaintenanceService: this.flowQueueMaintenanceService,
      eventEmitter: this.eventEmitter,
    });
  }
}
