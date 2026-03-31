import { MetadataCacheService } from '../../../infrastructure/cache/services/metadata-cache.service';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { TableHandlerService } from '../../table-management/services/table-handler.service';
import { QueryEngine } from '../../../infrastructure/query-engine/services/query-engine.service';
import { RouteCacheService } from '../../../infrastructure/cache/services/route-cache.service';
import { PolicyService } from '../../../core/policy/policy.service';
import { TableValidationService } from '../../dynamic-api/services/table-validation.service';
import { EventEmitter2 } from '@nestjs/event-emitter';
import { ConversationService } from '../services/conversation.service';

// Base dependencies for permission checking
export interface CheckPermissionExecutorDependencies {
  queryBuilder: QueryBuilderService;
  routeCacheService: RouteCacheService;
}

// Simple dependencies
export interface GetHintExecutorDependencies {
  queryBuilder: QueryBuilderService;
}

export interface UpdateTaskExecutorDependencies {
  conversationService: ConversationService;
}

// Full table operation dependencies (shared by multiple executors)
export interface TableOperationDependencies extends CheckPermissionExecutorDependencies {
  metadataCacheService: MetadataCacheService;
  queryBuilder: QueryBuilderService;
  tableHandlerService: TableHandlerService;
  queryEngine: QueryEngine;
  policyService: PolicyService;
  tableValidationService: TableValidationService;
  eventEmitter: EventEmitter2;
}

// Specific executor dependencies (interfaces for proper extension)
export interface GetTableDetailsExecutorDependencies extends TableOperationDependencies {}
export interface DynamicRepositoryExecutorDependencies extends TableOperationDependencies {}
export interface BatchDynamicRepositoryExecutorDependencies extends TableOperationDependencies {}
export interface CreateTablesExecutorDependencies extends TableOperationDependencies {}
export interface UpdateTablesExecutorDependencies extends TableOperationDependencies {}
export interface DeleteTablesExecutorDependencies extends TableOperationDependencies {}
