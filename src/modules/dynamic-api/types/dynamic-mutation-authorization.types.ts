import type { PolicyService } from '../../../domain/policy';
import type { QueryBuilderService } from '@enfyra/kernel';
import type { RuntimeRegistryService } from '../../../engines/cache/services/runtime-registry.service';
import type { TDynamicContext } from '../../../shared/types';

export interface DynamicMutationAuthorizationDependencies {
  context: TDynamicContext;
  enforceFieldPermission: boolean;
  policyService: PolicyService;
  queryBuilderService: QueryBuilderService;
  runtimeRegistryService: RuntimeRegistryService;
  tableName: string;
}
