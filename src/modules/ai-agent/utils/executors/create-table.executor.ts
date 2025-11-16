import { Logger } from '@nestjs/common';
import { MetadataCacheService } from '../../../../infrastructure/cache/services/metadata-cache.service';
import { QueryBuilderService } from '../../../../infrastructure/query-builder/query-builder.service';
import { TableHandlerService } from '../../../table-management/services/table-handler.service';
import { QueryEngine } from '../../../../infrastructure/query-engine/services/query-engine.service';
import { RouteCacheService } from '../../../../infrastructure/cache/services/route-cache.service';
import { StorageConfigCacheService } from '../../../../infrastructure/cache/services/storage-config-cache.service';
import { AiConfigCacheService } from '../../../../infrastructure/cache/services/ai-config-cache.service';
import { SystemProtectionService } from '../../../dynamic-api/services/system-protection.service';
import { TableValidationService } from '../../../dynamic-api/services/table-validation.service';
import { SwaggerService } from '../../../../infrastructure/swagger/services/swagger.service';
import { GraphqlService } from '../../../graphql/services/graphql.service';
import { TDynamicContext } from '../../../../shared/interfaces/dynamic-context.interface';
import { TableCreationWorkflow } from '../table-creation-workflow';
import { executeCheckPermission, CheckPermissionExecutorDependencies } from './check-permission.executor';

const logger = new Logger('CreateTableExecutor');

export interface CreateTableExecutorDependencies extends CheckPermissionExecutorDependencies {
  metadataCacheService: MetadataCacheService;
  queryBuilder: QueryBuilderService;
  tableHandlerService: TableHandlerService;
  queryEngine: QueryEngine;
  routeCacheService: RouteCacheService;
  storageConfigCacheService: StorageConfigCacheService;
  aiConfigCacheService: AiConfigCacheService;
  systemProtectionService: SystemProtectionService;
  tableValidationService: TableValidationService;
  swaggerService: SwaggerService;
  graphqlService: GraphqlService;
}

export async function executeCreateTable(
  args: {
    name: string;
    description?: string;
    columns: any[];
    relations?: any[];
    uniques?: any[][];
    indexes?: any[];
  },
  context: TDynamicContext,
  abortSignal: AbortSignal | undefined,
  deps: CreateTableExecutorDependencies,
): Promise<any> {
  logger.debug(`[create_table] Called with name=${args.name}`, {
    name: args.name,
    columnsCount: args.columns?.length || 0,
    relationsCount: args.relations?.length || 0,
    uniquesCount: args.uniques?.length || 0,
    indexesCount: args.indexes?.length || 0,
  });

  if (abortSignal?.aborted) {
    logger.debug(`[create_table] Request aborted`);
    return {
      error: true,
      errorCode: 'REQUEST_ABORTED',
      message: 'Request aborted by client',
    };
  }

  const permissionCache: Map<string, any> =
    ((context as any).__permissionCache as Map<string, any>) ||
    (((context as any).__permissionCache = new Map<string, any>()) as Map<string, any>);

  const userId = context.$user?.id;
  const cacheKey = `${userId || 'anon'}|create|table_definition|`;

  if (!permissionCache.has(cacheKey)) {
    const permissionResult = await executeCheckPermission(
      { table: 'table_definition', operation: 'create' },
      context,
      deps,
    );
    if (!permissionResult?.allowed) {
      return {
        error: true,
        errorCode: 'PERMISSION_DENIED',
        message: `Permission denied for create operation on table_definition. Reason: ${permissionResult?.reason || 'unknown'}.`,
        userMessage: `❌ **Permission Denied**: You do not have permission to create tables.\n\n📋 **Reason**: ${permissionResult?.reason || 'unknown'}\n\n💡 **Note**: This operation cannot proceed. Please check your access rights or contact an administrator.`,
        suggestion: `You MUST inform the user: "You do not have permission to create tables. Please check your access rights or contact an administrator." Then STOP - do NOT retry this operation or call any other tools.`,
        reason: permissionResult?.reason || 'unknown',
      };
    }
  } else {
    const permissionResult = permissionCache.get(cacheKey);
    if (!permissionResult?.allowed) {
      return {
        error: true,
        errorCode: 'PERMISSION_DENIED',
        message: `Permission denied for create operation on table_definition. Reason: ${permissionResult?.reason || 'unknown'}.`,
        userMessage: `❌ **Permission Denied**: You do not have permission to create tables.\n\n📋 **Reason**: ${permissionResult?.reason || 'unknown'}\n\n💡 **Note**: This operation cannot proceed. Please check your access rights or contact an administrator.`,
        suggestion: `You MUST inform the user: "You do not have permission to create tables. Please check your access rights or contact an administrator." Then STOP - do NOT retry this operation or call any other tools.`,
        reason: permissionResult?.reason || 'unknown',
      };
    }
  }

  const {
    metadataCacheService,
    queryBuilder,
    tableHandlerService,
    queryEngine,
    routeCacheService,
    storageConfigCacheService,
    aiConfigCacheService,
    systemProtectionService,
    tableValidationService,
    swaggerService,
    graphqlService,
  } = deps;

  const workflow = new TableCreationWorkflow(
    metadataCacheService,
    queryBuilder,
    tableHandlerService,
    queryEngine,
    routeCacheService,
    storageConfigCacheService,
    aiConfigCacheService,
    systemProtectionService,
    tableValidationService,
    swaggerService,
    graphqlService,
  );

  logger.debug(`[create_table] Executing workflow for table ${args.name}`);
  const workflowResult = await workflow.execute({
    tableName: args.name,
    tableData: {
      name: args.name,
      description: args.description,
      columns: args.columns,
      relations: args.relations,
      uniques: args.uniques,
      indexes: args.indexes,
    },
    context,
    maxRetries: 3,
  });

  if (!workflowResult.success) {
    const errorMessage = workflowResult.stopReason || 'Table creation workflow failed';
    const errorDetails = workflowResult.errors?.map(e => e.error).join('; ') || errorMessage;
    logger.error(`[create_table] Workflow failed for ${args.name}: ${errorDetails}`);
    return {
      error: true,
      errorCode: 'WORKFLOW_ERROR',
      message: errorDetails,
      errors: workflowResult.errors,
      stopReason: workflowResult.stopReason,
    };
  }

  logger.debug(`[create_table] Successfully created table ${args.name}`, {
    tableId: workflowResult.result?.id,
    tableName: workflowResult.result?.name,
  });
  return {
    ...workflowResult.result,
    reloadAdminUI: true,
    message: workflowResult.result?.message || `Table "${args.name}" has been created successfully. ⚠️ **Important**: Please reload the admin UI to see the changes.`,
  };
}

