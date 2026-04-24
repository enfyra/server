import { EventEmitter2 } from 'eventemitter2';
import { DynamicRepository } from './dynamic.repository';
import { TableHandlerService } from '../../table-management/services/table-handler.service';
import { QueryBuilderService } from '../../../engine/query-builder/query-builder.service';
import { QueryEngine } from '../../../engine/query-engine/services/query-engine.service';
import { MetadataCacheService } from '../../../engine/cache/services/metadata-cache.service';
import { PolicyService } from '../../../domain/policy/policy.service';
import { DynamicApiTableValidationService } from '../services/table-validation.service';
import { SettingCacheService } from '../../../engine/cache/services/setting-cache.service';
import { FieldPermissionCacheService } from '../../../engine/cache/services/field-permission-cache.service';
import { UserRevocationService } from '../../../domain/auth/services/user-revocation.service';
import { TDynamicContext } from '../../../shared/types';

export class DynamicRepositoryFactory {
  private readonly tableHandlerService: TableHandlerService;
  private readonly queryBuilderService: QueryBuilderService;
  private readonly queryEngine: QueryEngine;
  private readonly metadataCacheService: MetadataCacheService;
  private readonly policyService: PolicyService;
  private readonly tableValidationService: DynamicApiTableValidationService;
  private readonly settingCacheService: SettingCacheService;
  private readonly fieldPermissionCacheService: FieldPermissionCacheService;
  private readonly userRevocationService: UserRevocationService;
  private readonly eventEmitter: EventEmitter2;

  constructor(deps: {
    tableHandlerService: TableHandlerService;
    queryBuilderService: QueryBuilderService;
    queryEngine: QueryEngine;
    metadataCacheService: MetadataCacheService;
    policyService: PolicyService;
    tableValidationService: DynamicApiTableValidationService;
    settingCacheService: SettingCacheService;
    fieldPermissionCacheService: FieldPermissionCacheService;
    userRevocationService: UserRevocationService;
    eventEmitter: EventEmitter2;
  }) {
    this.tableHandlerService = deps.tableHandlerService;
    this.queryBuilderService = deps.queryBuilderService;
    this.queryEngine = deps.queryEngine;
    this.metadataCacheService = deps.metadataCacheService;
    this.policyService = deps.policyService;
    this.tableValidationService = deps.tableValidationService;
    this.settingCacheService = deps.settingCacheService;
    this.fieldPermissionCacheService = deps.fieldPermissionCacheService;
    this.userRevocationService = deps.userRevocationService;
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
      queryEngine: this.queryEngine,
      metadataCacheService: this.metadataCacheService,
      policyService: this.policyService,
      tableValidationService: this.tableValidationService,
      settingCacheService: this.settingCacheService,
      fieldPermissionCacheService: this.fieldPermissionCacheService,
      userRevocationService: this.userRevocationService,
      eventEmitter: this.eventEmitter,
    });
  }
}
