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
import { executeCheckPermission, CheckPermissionExecutorDependencies } from './check-permission.executor';

const logger = new Logger('UpdateTableExecutor');

export interface UpdateTableExecutorDependencies extends CheckPermissionExecutorDependencies {
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

export async function executeUpdateTable(
  args: {
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
  deps: UpdateTableExecutorDependencies,
): Promise<any> {
  logger.debug(`[update_table] Called with tableName=${args.tableName}`, {
    tableName: args.tableName,
    tableId: args.tableId,
    columnsCount: args.columns?.length || 0,
    relationsCount: args.relations?.length || 0,
    uniquesCount: args.uniques?.length || 0,
    indexesCount: args.indexes?.length || 0,
  });

  if (abortSignal?.aborted) {
    logger.debug(`[update_table] Request aborted`);
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
    logger.debug(`[update_table] Executing workflow for table ${args.tableName}`);
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
    if (args.description !== undefined) updateData.description = args.description;
    if (args.columns) updateData.columns = args.columns;
    if (args.relations) updateData.relations = args.relations;
    if (args.uniques) updateData.uniques = args.uniques;
    if (args.indexes) updateData.indexes = args.indexes;

    const workflowResult = await workflow.execute({
      tableName: args.tableName,
      tableId: args.tableId,
      updateData,
      context,
    });

    if (workflowResult.success) {
      const result = workflowResult.result;
      const updatedFields: string[] = [];
      if (args.description !== undefined) updatedFields.push('description');
      if (args.columns && args.columns.length > 0) updatedFields.push(`${args.columns.length} column(s)`);
      if (args.relations !== undefined) {
        if (args.relations.length === 0) {
          updatedFields.push('all relations deleted');
        } else {
          updatedFields.push(`${args.relations.length} relation(s)`);
        }
      }
      if (args.uniques !== undefined) {
        if (args.uniques.length === 0) {
          updatedFields.push('all unique constraints deleted');
        } else {
          updatedFields.push(`${args.uniques.length} unique constraint(s)`);
        }
      }
      if (args.indexes !== undefined) {
        if (args.indexes.length === 0) {
          updatedFields.push('all indexes deleted');
        } else {
          updatedFields.push(`${args.indexes.length} index(es)`);
        }
      }

      logger.debug(`[update_table] Successfully updated table ${args.tableName}`, {
        tableId: result?.id || args.tableId,
        updatedFields: updatedFields,
      });
      return {
        success: true,
        tableName: args.tableName,
        tableId: result?.id || args.tableId,
        updated: updatedFields.length > 0 ? updatedFields.join(', ') : 'table metadata',
        result,
        reloadAdminUI: true,
        message: `Table "${args.tableName}" has been updated successfully. ⚠️ **Important**: Please reload the admin UI to see the changes.`,
      };
    }

    const errorMessage = workflowResult.stopReason || 'Table update workflow failed';
    logger.error(`[update_table] Workflow failed for ${args.tableName}: ${errorMessage}`);

    return {
      error: true,
      errorCode: 'TABLE_UPDATE_FAILED',
      message: errorMessage,
      errors: workflowResult.errors,
      suggestion: 'Check the errors array for details. Ensure table exists and update data is valid.',
    };
  } catch (error: any) {
    logger.error(`[UpdateTableExecutor] update_table → EXCEPTION: ${error.message}`);
    return {
      error: true,
      errorCode: 'TABLE_UPDATE_EXCEPTION',
      message: error.message,
      suggestion: 'An unexpected error occurred. Please try again or check table name and update data.',
    };
  }
}

