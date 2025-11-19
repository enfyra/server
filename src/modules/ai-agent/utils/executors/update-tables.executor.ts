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
import { TableUpdateWorkflow } from '../table-update-workflow';
import { executeCheckPermission } from './check-permission.executor';
import { UpdateTablesExecutorDependencies } from '../types';

const logger = new Logger('UpdateTablesExecutor');

async function executeUpdateSingleTable(
  table: {
    tableName: string;
    tableId?: number;
    description?: string;
    columns?: any[];
    relations?: any[];
    uniques?: any[][];
    indexes?: any[];
  },
  context: TDynamicContext,
  abortSignal: AbortSignal | undefined,
  deps: UpdateTablesExecutorDependencies,
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

  try {
    const workflow = new TableUpdateWorkflow(
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

    const updateData: any = {};
    if (table.description !== undefined) updateData.description = table.description;
    if (table.columns) updateData.columns = table.columns;
    if (table.relations) updateData.relations = table.relations;
    if (table.uniques) updateData.uniques = table.uniques;
    if (table.indexes) updateData.indexes = table.indexes;

    const workflowResult = await workflow.execute({
      tableName: table.tableName,
      tableId: table.tableId,
      updateData,
      context,
    });

    if (workflowResult.success) {
      const result = workflowResult.result;
      const updatedFields: string[] = [];
      if (table.description !== undefined) updatedFields.push('description');
      if (table.columns && table.columns.length > 0) updatedFields.push(`${table.columns.length} column(s)`);
      if (table.relations !== undefined) {
        if (table.relations.length === 0) {
          updatedFields.push('all relations deleted');
        } else {
          updatedFields.push(`${table.relations.length} relation(s)`);
        }
      }
      if (table.uniques !== undefined) {
        if (table.uniques.length === 0) {
          updatedFields.push('all unique constraints deleted');
        } else {
          updatedFields.push(`${table.uniques.length} unique constraint(s)`);
        }
      }
      if (table.indexes !== undefined) {
        if (table.indexes.length === 0) {
          updatedFields.push('all indexes deleted');
        } else {
          updatedFields.push(`${table.indexes.length} index(es)`);
        }
      }

      return {
        success: true,
        tableName: table.tableName,
        tableId: result?.id || table.tableId,
        updated: updatedFields.length > 0 ? updatedFields.join(', ') : 'table metadata',
        result,
      };
    }

    const errorMessage = workflowResult.stopReason || 'Table update workflow failed';
    return {
      error: true,
      errorCode: 'TABLE_UPDATE_FAILED',
      message: errorMessage,
      errors: workflowResult.errors,
    };
  } catch (error: any) {
    logger.error(`[update_tables] Workflow failed for ${table.tableName}: ${error.message}`);
    return {
      error: true,
      errorCode: 'TABLE_UPDATE_EXCEPTION',
      message: error.message,
    };
  }
}

export async function executeUpdateTables(
  args: {
    tables: Array<{
      tableName: string;
      tableId?: number;
      description?: string;
      columns?: any[];
      relations?: any[];
      uniques?: any[][];
      indexes?: any[];
    }>;
  },
  context: TDynamicContext,
  abortSignal: AbortSignal | undefined,
  deps: UpdateTablesExecutorDependencies,
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
  const cacheKey = `${userId || 'anon'}|update|table_definition|`;

  if (!permissionCache.has(cacheKey)) {
    const permissionResult = await executeCheckPermission(
      { table: 'table_definition', operation: 'update' },
      context,
      deps,
    );
    if (!permissionResult?.allowed) {
      return {
        error: true,
        errorCode: 'PERMISSION_DENIED',
        message: `Permission denied for update operation on table_definition. Reason: ${permissionResult?.reason || 'unknown'}.`,
        userMessage: `❌ **Permission Denied**: You do not have permission to update tables.\n\n📋 **Reason**: ${permissionResult?.reason || 'unknown'}\n\n💡 **Note**: This operation cannot proceed. Please check your access rights or contact an administrator.`,
        suggestion: `You MUST inform the user: "You do not have permission to update tables. Please check your access rights or contact an administrator." Then STOP - do NOT retry this operation or call any other tools.`,
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
        message: `Permission denied for update operation on table_definition. Reason: ${permissionResult?.reason || 'unknown'}.`,
        userMessage: `❌ **Permission Denied**: You do not have permission to update tables.\n\n📋 **Reason**: ${permissionResult?.reason || 'unknown'}\n\n💡 **Note**: This operation cannot proceed. Please check your access rights or contact an administrator.`,
        suggestion: `You MUST inform the user: "You do not have permission to update tables. Please check your access rights or contact an administrator." Then STOP - do NOT retry this operation or call any other tools.`,
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

    if (!table.tableName && !table.tableId) {
      errors.push({
        index: i,
        tableName: undefined,
        tableId: undefined,
        error: 'INVALID_INPUT',
        message: 'Either tableName or tableId is required',
      });
      results.push({
        success: false,
        tableName: undefined,
        tableId: undefined,
        error: 'INVALID_INPUT',
        message: 'Either tableName or tableId is required',
      });
      continue;
    }

    try {
      const updateArgs: any = { ...table };
      if (!updateArgs.tableName && updateArgs.tableId) {
        updateArgs.tableName = `table_${updateArgs.tableId}`;
      }
      const result = await executeUpdateSingleTable(updateArgs, context, abortSignal, deps);
      
      if (result.error) {
        errors.push({
          index: i,
          tableName: table.tableName,
          tableId: table.tableId,
          error: result.errorCode || 'UNKNOWN_ERROR',
          message: result.message || 'Unknown error',
        });
        results.push({
          success: false,
          tableName: table.tableName,
          tableId: table.tableId,
          error: result.errorCode || 'UNKNOWN_ERROR',
          message: result.message || 'Unknown error',
        });
      } else {
        results.push({
          success: true,
          tableName: result.tableName || table.tableName,
          tableId: result.tableId || table.tableId,
          updated: result.updated,
          result: result.result,
        });
      }
    } catch (error: any) {
      logger.error(`[update_tables] Error updating table ${table.tableName || `id=${table.tableId}`}: ${error.message}`);
      errors.push({
        index: i,
        tableName: table.tableName,
        tableId: table.tableId,
        error: 'EXECUTION_ERROR',
        message: error.message || String(error),
      });
      results.push({
        success: false,
        tableName: table.tableName,
        tableId: table.tableId,
        error: 'EXECUTION_ERROR',
        message: error.message || String(error),
      });
    }
  }

  const successCount = results.filter(r => r.success).length;
  const failureCount = results.filter(r => !r.success).length;

  const succeededTables = results.filter(r => r.success).map(r => `"${r.tableName}" (ID: ${r.tableId || 'N/A'})`).join(', ');
  const failedTables = results.filter(r => !r.success).map(r => `"${r.tableName || `ID ${r.tableId}`}"`).join(', ');

  const summary = failureCount === 0
    ? `Successfully updated ${successCount} table(s): ${succeededTables}.`
    : `Updated ${successCount} table(s), ${failureCount} failed. ${succeededTables ? `Succeeded: ${succeededTables}. ` : ''}${failedTables ? `Failed: ${failedTables}.` : ''}`;

  const detailedMessage = failureCount === 0
    ? `✅ Successfully updated ${successCount} table(s):\n\n${results.filter(r => r.success).map((r, idx) => `${idx + 1}. "${r.tableName}" (ID: ${r.tableId || 'N/A'})${r.updated ? ` - Updated: ${r.updated}` : ''}`).join('\n')}\n\n⚠️ **Important**: Please reload the admin UI to see the changes.`
    : `⚠️ Updated ${successCount} table(s), ${failureCount} failed:\n\n${succeededTables ? `✅ **Succeeded (${successCount}):**\n${results.filter(r => r.success).map((r, idx) => `${idx + 1}. "${r.tableName}" (ID: ${r.tableId || 'N/A'})${r.updated ? ` - Updated: ${r.updated}` : ''}`).join('\n')}\n\n` : ''}${failedTables ? `❌ **Failed (${failureCount}):**\n${results.filter(r => !r.success).map((r, idx) => `${idx + 1}. "${r.tableName || `ID ${r.tableId}`}" - ${r.error}: ${r.message}`).join('\n')}\n\n` : ''}⚠️ **Important**: Please reload the admin UI to see the changes.`;

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

