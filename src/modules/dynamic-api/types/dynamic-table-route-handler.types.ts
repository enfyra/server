import type { QueryBuilderService } from '@enfyra/kernel';
import type { BcryptService, UserRevocationService } from '../../../domain/auth';
import type { RuntimeMetadataSchemaRouterService } from '../../table-management';
import type { FlowQueueMaintenanceService } from '../../flow';
import type { GuardValidationService } from '../services/guard-validation.service';

export interface DynamicTableRouteHandlerDependencies {
  bcryptService: BcryptService;
  flowQueueMaintenanceService?: FlowQueueMaintenanceService;
  guardValidationService: GuardValidationService;
  queryBuilderService: QueryBuilderService;
  runtimeMetadataSchemaRouterService: RuntimeMetadataSchemaRouterService;
  userRevocationService?: UserRevocationService;
}
