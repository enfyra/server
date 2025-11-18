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
import { executeCheckPermission } from './check-permission.executor';
import { CreateTablesExecutorDependencies } from '../types';

const logger = new Logger('CreateTablesExecutor');

async function executeCreateSingleTable(
  table: {
    name: string;
    description?: string;
    columns: any[];
    relations?: any[];
    uniques?: any[][];
    indexes?: any[];
  },
  context: TDynamicContext,
  abortSignal: AbortSignal | undefined,
  deps: CreateTablesExecutorDependencies,
): Promise<any> {
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

  const workflowResult = await workflow.execute({
    tableName: table.name,
    tableData: {
      name: table.name,
      description: table.description,
      columns: table.columns,
      relations: table.relations,
      uniques: table.uniques,
      indexes: table.indexes,
    },
    context,
    maxRetries: 3,
  });

  if (!workflowResult.success) {
    const errorMessage = workflowResult.stopReason || 'Table creation workflow failed';
    const errorDetails = workflowResult.errors?.map(e => e.error).join('; ') || errorMessage;
    return {
      error: true,
      errorCode: 'WORKFLOW_ERROR',
      message: errorDetails,
      errors: workflowResult.errors,
      stopReason: workflowResult.stopReason,
    };
  }

  return {
    success: true,
    tableName: table.name,
    tableId: workflowResult.result?.id,
    result: workflowResult.result,
  };
}

export async function executeCreateTables(
  args: {
    tables: Array<{
      name: string;
      description?: string;
      columns: any[];
      relations?: any[];
      uniques?: any[][];
      indexes?: any[];
    }>;
  },
  context: TDynamicContext,
  abortSignal: AbortSignal | undefined,
  deps: CreateTablesExecutorDependencies,
): Promise<any> {
  if (abortSignal?.aborted) {
    return {
      error: true,
      errorCode: 'REQUEST_ABORTED',
      message: 'Request aborted by client',
    };
  }

  if (!args.tables || args.tables.length === 0) {
    return {
      error: true,
      errorCode: 'INVALID_INPUT',
      message: 'tables array is required and must not be empty',
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
    permissionCache.set(cacheKey, permissionResult);
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

  const results: any[] = [];
  const errors: any[] = [];

  for (let i = 0; i < args.tables.length; i++) {
    const table = args.tables[i];
    
    if (abortSignal?.aborted) {
      break;
    }

    try {
      const result = await executeCreateSingleTable(table, context, abortSignal, deps);
      
      if (result.error) {
        errors.push({
          index: i,
          tableName: table.name,
          error: result.errorCode || 'UNKNOWN_ERROR',
          message: result.message || 'Unknown error',
        });
        results.push({
          success: false,
          tableName: table.name,
          error: result.errorCode || 'UNKNOWN_ERROR',
          message: result.message || 'Unknown error',
        });
      } else {
        results.push({
          success: true,
          tableName: table.name,
          tableId: result.tableId,
          result: result.result,
        });
      }
    } catch (error: any) {
      logger.error(`[create_tables] Error creating table ${table.name}: ${error.message}`);
      errors.push({
        index: i,
        tableName: table.name,
        error: 'EXECUTION_ERROR',
        message: error.message || String(error),
      });
      results.push({
        success: false,
        tableName: table.name,
        error: 'EXECUTION_ERROR',
        message: error.message || String(error),
      });
    }
  }

  const successCount = results.filter(r => r.success).length;
  const failureCount = results.filter(r => !r.success).length;

  const succeededTables = results.filter(r => r.success).map(r => `"${r.tableName}" (ID: ${r.tableId || 'N/A'})`).join(', ');
  const failedTables = results.filter(r => !r.success).map(r => `"${r.tableName}"`).join(', ');

  const summary = failureCount === 0
    ? `Successfully created ${successCount} table(s): ${succeededTables}.`
    : `Created ${successCount} table(s), ${failureCount} failed. ${succeededTables ? `Succeeded: ${succeededTables}. ` : ''}${failedTables ? `Failed: ${failedTables}.` : ''}`;

  const detailedMessage = failureCount === 0
    ? `✅ Successfully created ${successCount} table(s):\n\n${results.filter(r => r.success).map((r, idx) => `${idx + 1}. "${r.tableName}" (ID: ${r.tableId || 'N/A'})`).join('\n')}\n\n⚠️ **Important**: Please reload the admin UI to see the changes.`
    : `⚠️ Created ${successCount} table(s), ${failureCount} failed:\n\n${succeededTables ? `✅ **Succeeded (${successCount}):**\n${results.filter(r => r.success).map((r, idx) => `${idx + 1}. "${r.tableName}" (ID: ${r.tableId || 'N/A'})`).join('\n')}\n\n` : ''}${failedTables ? `❌ **Failed (${failureCount}):**\n${results.filter(r => !r.success).map((r, idx) => `${idx + 1}. "${r.tableName}" - ${r.error}: ${r.message}`).join('\n')}\n\n` : ''}⚠️ **Important**: Please reload the admin UI to see the changes.`;

  return {
    success: failureCount === 0,
    total: args.tables.length,
    succeeded: successCount,
    failed: failureCount,
    results,
    errors: errors.length > 0 ? errors : undefined,
    reloadAdminUI: successCount > 0,
    summary,
    message: detailedMessage,
  };
}

