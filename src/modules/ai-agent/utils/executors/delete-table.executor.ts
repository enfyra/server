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
import { executeCheckPermission, CheckPermissionExecutorDependencies } from './check-permission.executor';
import { DynamicRepository } from '../../../dynamic-api/repositories/dynamic.repository';

const logger = new Logger('DeleteTableExecutor');

export interface DeleteTableExecutorDependencies extends CheckPermissionExecutorDependencies {
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

export async function executeDeleteTable(
  args: {
    id: number;
  },
  context: TDynamicContext,
  abortSignal: AbortSignal | undefined,
  deps: DeleteTableExecutorDependencies,
): Promise<any> {
  logger.debug(`[delete_table] Called with id=${args.id}`);

  if (abortSignal?.aborted) {
    logger.debug(`[delete_table] Request aborted`);
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
  const cacheKey = `${userId || 'anon'}|delete|table_definition|`;

  if (!permissionCache.has(cacheKey)) {
    const permissionResult = await executeCheckPermission(
      { table: 'table_definition', operation: 'delete' },
      context,
      deps,
    );
    if (!permissionResult?.allowed) {
      return {
        error: true,
        errorCode: 'PERMISSION_DENIED',
        message: permissionResult?.reason || 'Permission denied',
        userMessage: `❌ **Permission Denied**: You do not have permission to delete tables.\n\n💡 **Note**: Please check your access rights or contact an administrator.`,
        suggestion: `You MUST inform the user: "You do not have permission to delete tables. Please check your access rights or contact an administrator." Then STOP - do NOT retry this operation.`,
      };
    }
    permissionCache.set(cacheKey, permissionResult);
  } else {
    const cachedPermission = permissionCache.get(cacheKey);
    if (!cachedPermission?.allowed) {
      return {
        error: true,
        errorCode: 'PERMISSION_DENIED',
        message: cachedPermission?.reason || 'Permission denied',
        userMessage: `❌ **Permission Denied**: You do not have permission to delete tables.\n\n💡 **Note**: Please check your access rights or contact an administrator.`,
        suggestion: `You MUST inform the user: "You do not have permission to delete tables. Please check your access rights or contact an administrator." Then STOP - do NOT retry this operation.`,
      };
    }
  }

  try {
    // Get table info before deletion
    const repo = new DynamicRepository({
      context,
      tableName: 'table_definition',
      queryBuilder: deps.queryBuilder,
      tableHandlerService: deps.tableHandlerService,
      queryEngine: deps.queryEngine,
      routeCacheService: deps.routeCacheService,
      storageConfigCacheService: deps.storageConfigCacheService,
      aiConfigCacheService: deps.aiConfigCacheService,
      systemProtectionService: deps.systemProtectionService,
      tableValidationService: deps.tableValidationService,
      bootstrapScriptService: undefined,
      redisPubSubService: undefined,
      metadataCacheService: deps.metadataCacheService,
      swaggerService: deps.swaggerService,
      graphqlService: deps.graphqlService,
    });
    await repo.init();

    const tableInfo = await repo.find({
      where: { id: { _eq: args.id } },
      fields: 'id,name,description',
      limit: 1,
    });

    if (!tableInfo?.data || tableInfo.data.length === 0) {
      logger.error(`[delete_table] Table with id=${args.id} not found`);
      return {
        error: true,
        errorCode: 'TABLE_NOT_FOUND',
        message: `Table with id ${args.id} not found`,
        userMessage: `❌ **Table Not Found**: Table with id ${args.id} does not exist.\n\n💡 **Note**: Please verify the table id is correct. Use get_table_details or dynamic_repository to find the correct table id.`,
        suggestion: `First find the table using: {"table":"table_definition","operation":"find","where":{"name":{"_eq":"<table_name>"}},"fields":"id,name"}. Then use the id from the result.`,
      };
    }

    const tableName = tableInfo.data[0].name;

    logger.debug(`[delete_table] Deleting table ${tableName} (id=${args.id})`);

    // Delete the table
    await repo.delete({ id: args.id });

    logger.debug(`[delete_table] Successfully deleted table ${tableName} (id=${args.id})`);

    return {
      success: true,
      id: args.id,
      name: tableName,
      message: `Table "${tableName}" (id: ${args.id}) has been deleted successfully.`,
    };
  } catch (error: any) {
    const errorMessage = error?.message || String(error);
    logger.error(`[delete_table] Error deleting table with id=${args.id}: ${errorMessage}`, error?.stack);

    return {
      error: true,
      errorCode: 'DELETE_TABLE_FAILED',
      message: errorMessage,
      userMessage: `❌ **Error Deleting Table**: ${errorMessage}\n\n💡 **Note**: Please check the error message and try again.`,
      suggestion: `Review the error message and verify the table id is correct. If the error persists, inform the user about the issue.`,
    };
  }
}

