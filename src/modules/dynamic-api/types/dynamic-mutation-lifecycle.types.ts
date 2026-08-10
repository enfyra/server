import type { MutationContext } from './table-route.types';
import type { DynamicReadOptions } from './dynamic-read.types';
import type { QueryBuilderService } from '@enfyra/kernel';
import type { TDynamicContext } from '../../../shared/types';
import type { TCacheInvalidationPayload } from '../../../shared/types/cache.types';
import type { RuntimeMetadataSchemaRouterService } from '../../table-management/services/runtime-metadata-schema-router.service';
import type { TableRouteRouter } from '../repositories/table-route.router';
import type { DynamicMutationPreparationService } from '../services/dynamic-mutation-preparation.service';
import type { DynamicMutationAuthorizationService } from '../services/dynamic-mutation-authorization.service';
import type { DynamicApiTableValidationService } from '../services/table-validation.service';
import type { DynamicSchemaActivationService } from '../services/dynamic-schema-activation.service';
import type { DynamicBatchCreationService } from '../services/dynamic-batch-creation.service';

export type DynamicMutationId = string | number;

export type DynamicMutationAction = 'create' | 'update' | 'delete';

export interface DynamicMutationReadResult {
  data?: Array<Record<string, unknown>>;
  count?: number;
}

export interface DynamicMutationReloadOptions {
  ids?: DynamicMutationId[];
  affectedTables?: string[];
  tableRenames?: TCacheInvalidationPayload['tableRenames'];
  critical?: boolean;
}

export interface DynamicMutationRuntime {
  find(options: DynamicReadOptions): Promise<DynamicMutationReadResult>;
  getIdField(): string;
  reload(options?: DynamicMutationReloadOptions): Promise<void>;
  emit(
    action: DynamicMutationAction,
    ids?: DynamicMutationId[],
    data?: Record<string, unknown>,
  ): void;
}

export interface DynamicBatchCreateResult {
  accepted: true;
  batch: true;
  count: number;
}

interface DynamicMutationOperationOptions {
  runtime: DynamicMutationRuntime;
  routeRouter: TableRouteRouter;
  runtimeMetadataSchemaRouterService: RuntimeMetadataSchemaRouterService;
  queryBuilderService: QueryBuilderService;
  schemaActivationService: DynamicSchemaActivationService;
  tableName: string;
  tableMetadata: unknown;
  context: TDynamicContext;
}

export interface DynamicMutationCreateOptions extends DynamicMutationOperationOptions {
  mutationPreparationService: DynamicMutationPreparationService;
  mutationAuthorizationService: DynamicMutationAuthorizationService;
  tableValidationService: DynamicApiTableValidationService;
  batchCreationService: DynamicBatchCreationService;
  data: Record<string, unknown>;
  fields?: string | string[];
  batch?: boolean;
}

export interface DynamicMutationUpdateOptions extends DynamicMutationOperationOptions {
  mutationPreparationService: DynamicMutationPreparationService;
  mutationAuthorizationService: DynamicMutationAuthorizationService;
  tableValidationService: DynamicApiTableValidationService;
  id: DynamicMutationId;
  data: Record<string, unknown>;
  fields?: string | string[];
}

export interface DynamicMutationDeleteOptions extends DynamicMutationOperationOptions {
  mutationAuthorizationService: DynamicMutationAuthorizationService;
  tableValidationService: DynamicApiTableValidationService;
  id: DynamicMutationId;
}

export interface DynamicMutationLifecycleOptions<TPersisted, TResult> {
  context: MutationContext;
  persist: () => Promise<TPersisted>;
  afterWrite?: (
    context: MutationContext,
    persisted: TPersisted,
  ) => Promise<void> | void;
  buildResult: (
    context: MutationContext,
    persisted: TPersisted,
  ) => Promise<TResult> | TResult;
  reload: (context: MutationContext) => Promise<void>;
  afterReload?: (context: MutationContext) => Promise<void> | void;
  emit?: (context: MutationContext) => void;
  recover?: (
    context: MutationContext,
    persisted: TPersisted,
    error: unknown,
  ) => Promise<TResult>;
}
