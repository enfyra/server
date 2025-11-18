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
import { executeCheckPermission } from './check-permission.executor';
import { DynamicRepository } from '../../../dynamic-api/repositories/dynamic.repository';
import { DeleteTablesExecutorDependencies } from '../types';

const logger = new Logger('DeleteTablesExecutor');

async function executeDeleteSingleTable(
  id: number,
  context: TDynamicContext,
  abortSignal: AbortSignal | undefined,
  deps: DeleteTablesExecutorDependencies,
): Promise<any> {
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
    where: { id: { _eq: id } },
    fields: 'id,name,description',
    limit: 1,
  });

  if (!tableInfo?.data || tableInfo.data.length === 0) {
    return {
      error: true,
      errorCode: 'TABLE_NOT_FOUND',
      message: `Table with id ${id} not found`,
    };
  }

  const tableName = tableInfo.data[0].name;
  await repo.delete({ id });

  return {
    success: true,
    id,
    name: tableName,
  };
}

export async function executeDeleteTables(
  args: {
    ids: number[];
  },
  context: TDynamicContext,
  abortSignal: AbortSignal | undefined,
  deps: DeleteTablesExecutorDependencies,
): Promise<any> {
  if (abortSignal?.aborted) {
    return {
      error: true,
      errorCode: 'REQUEST_ABORTED',
      message: 'Request aborted by client',
    };
  }

  if (!args.ids || args.ids.length === 0) {
    return {
      error: true,
      errorCode: 'INVALID_INPUT',
      message: 'ids array is required and must not be empty',
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

  const results: any[] = [];
  const errors: any[] = [];

  for (let i = 0; i < args.ids.length; i++) {
    const id = args.ids[i];
    
    if (abortSignal?.aborted) {
      break;
    }

    try {
      const result = await executeDeleteSingleTable(id, context, abortSignal, deps);
      
      if (result.error) {
        errors.push({
          index: i,
          id,
          error: result.errorCode || 'UNKNOWN_ERROR',
          message: result.message || 'Unknown error',
        });
        results.push({
          success: false,
          id,
          error: result.errorCode || 'UNKNOWN_ERROR',
          message: result.message || 'Unknown error',
        });
      } else {
        results.push({
          success: true,
          id: result.id,
          name: result.name,
        });
      }
    } catch (error: any) {
      logger.error(`[delete_tables] Error deleting table id=${id}: ${error.message}`);
      errors.push({
        index: i,
        id,
        error: 'EXECUTION_ERROR',
        message: error.message || String(error),
      });
      results.push({
        success: false,
        id,
        error: 'EXECUTION_ERROR',
        message: error.message || String(error),
      });
    }
  }

  const successCount = results.filter(r => r.success).length;
  const failureCount = results.filter(r => !r.success).length;

  const succeededTableNames = results.filter(r => r.success).map(r => r.name).join(', ');
  const failedTableIds = results.filter(r => !r.success).map(r => r.id).join(', ');

  const summary = failureCount === 0
    ? `Successfully deleted ${successCount} table(s): ${succeededTableNames}.`
    : `Deleted ${successCount} table(s), ${failureCount} failed. ${succeededTableNames ? `Succeeded: ${succeededTableNames}. ` : ''}${failedTableIds ? `Failed IDs: ${failedTableIds}.` : ''}`;

  const message = failureCount === 0
    ? `✅ Deleted ${successCount} table(s): ${succeededTableNames}. Please reload admin UI.`
    : `⚠️ Deleted ${successCount}, ${failureCount} failed. ${succeededTableNames ? `Succeeded: ${succeededTableNames}. ` : ''}${failedTableIds ? `Failed: ${failedTableIds}.` : ''} Please reload admin UI.`;

  return {
    success: failureCount === 0,
    total: args.ids.length,
    succeeded: successCount,
    failed: failureCount,
    results,
    errors: errors.length > 0 ? errors : undefined,
    reloadAdminUI: successCount > 0,
    summary,
    message,
  };
}

