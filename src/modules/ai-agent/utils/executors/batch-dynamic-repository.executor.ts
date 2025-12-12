import { Logger } from '@nestjs/common';
import { DynamicRepository } from '../../../dynamic-api/repositories/dynamic.repository';
import { QueryBuilderService } from '../../../../infrastructure/query-builder/query-builder.service';
import { TableHandlerService } from '../../../table-management/services/table-handler.service';
import { QueryEngine } from '../../../../infrastructure/query-engine/services/query-engine.service';
import { RouteCacheService } from '../../../../infrastructure/cache/services/route-cache.service';
import { StorageConfigCacheService } from '../../../../infrastructure/cache/services/storage-config-cache.service';
import { AiConfigCacheService } from '../../../../infrastructure/cache/services/ai-config-cache.service';
import { MetadataCacheService } from '../../../../infrastructure/cache/services/metadata-cache.service';
import { SystemProtectionService } from '../../../dynamic-api/services/system-protection.service';
import { TableValidationService } from '../../../dynamic-api/services/table-validation.service';
import { SwaggerService } from '../../../../infrastructure/swagger/services/swagger.service';
import { GraphqlService } from '../../../graphql/services/graphql.service';
import { TDynamicContext } from '../../../../shared/interfaces/dynamic-context.interface';
import {
  formatErrorForUser,
  shouldEscalateToHuman,
  formatEscalationMessage,
  getRecoveryStrategy,
} from '../error-recovery.helper';
import { executeCheckPermission } from './check-permission.executor';
import { BatchDynamicRepositoryExecutorDependencies } from '../types';

const logger = new Logger('BatchDynamicRepositoryExecutor');

export async function executeBatchDynamicRepository(
  args: {
    table: string;
    operation: 'batch_create' | 'batch_update' | 'batch_delete';
    fields?: string;
    dataArray?: any[];
    updates?: Array<{ id: string | number; data: any }>;
    ids?: Array<string | number>;
  },
  context: TDynamicContext,
  abortSignal: AbortSignal | undefined,
  deps: BatchDynamicRepositoryExecutorDependencies,
): Promise<any> {
  if (abortSignal?.aborted) {
    return {
      error: true,
      errorCode: 'REQUEST_ABORTED',
      message: 'Request aborted by client',
    };
  }

  const {
    queryBuilder,
    tableHandlerService,
    queryEngine,
    routeCacheService,
    storageConfigCacheService,
    aiConfigCacheService,
    metadataCacheService,
    systemProtectionService,
    tableValidationService,
    swaggerService,
    graphqlService,
  } = deps;

  const repo = new DynamicRepository({
    context,
    tableName: args.table,
    queryBuilder,
    tableHandlerService,
    queryEngine,
    routeCacheService,
    storageConfigCacheService,
    aiConfigCacheService,
    metadataCacheService,
    systemProtectionService,
    tableValidationService,
    swaggerService,
    graphqlService,
  });

  // Permission check
  const needsPermissionCheck =
    ['batch_create', 'batch_update', 'batch_delete'].includes(args.operation);

  if (needsPermissionCheck) {
    const permissionCache: Map<string, any> =
      ((context as any).__permissionCache as Map<string, any>) ||
      (((context as any).__permissionCache = new Map<string, any>()) as Map<string, any>);

    const userId = context.$user?.id;
    const operation = args.operation === 'batch_create' ? 'create' : args.operation === 'batch_update' ? 'update' : 'delete';
    const cacheKey = `${userId || 'anon'}|${operation}|${args.table || ''}|`;

    if (!permissionCache.has(cacheKey)) {
      const isMetadataTable = args.table?.endsWith('_definition');
      if (!isMetadataTable) {
        const permissionResult = await executeCheckPermission(
          { table: args.table, operation: operation as 'read' | 'create' | 'update' | 'delete' },
          context,
          deps,
        );
        if (!permissionResult?.allowed) {
          return {
            error: true,
            errorCode: 'PERMISSION_DENIED',
            message: `Permission denied for ${operation} operation on ${args.table}. Reason: ${permissionResult?.reason || 'unknown'}.`,
            userMessage: `❌ **Permission Denied**: You do not have permission to perform ${operation} operation on table "${args.table}".\n\n📋 **Reason**: ${permissionResult?.reason || 'unknown'}\n\n💡 **Note**: This operation cannot proceed. Please check your access rights or contact an administrator.`,
            suggestion: `You MUST inform the user: "You do not have permission to ${operation} on table ${args.table}. Please check your access rights or contact an administrator." Then STOP - do NOT retry this operation or call any other tools.`,
            reason: permissionResult?.reason || 'unknown',
          };
        }
        permissionCache.set(cacheKey, permissionResult);
      }
    } else {
      const permissionResult = permissionCache.get(cacheKey);
      if (!permissionResult?.allowed) {
        return {
          error: true,
          errorCode: 'PERMISSION_DENIED',
          message: `Permission denied for ${operation} operation on ${args.table}. Reason: ${permissionResult?.reason || 'unknown'}.`,
          userMessage: `❌ **Permission Denied**: You do not have permission to perform ${operation} operation on table "${args.table}".\n\n📋 **Reason**: ${permissionResult?.reason || 'unknown'}\n\n💡 **Note**: This operation cannot proceed. Please check your access rights or contact an administrator.`,
          suggestion: `You MUST inform the user: "You do not have permission to ${operation} on table ${args.table}. Please check your access rights or contact an administrator." Then STOP - do NOT retry this operation or call any other tools.`,
          reason: permissionResult?.reason || 'unknown',
        };
      }
    }
  }

  try {
    const safeFields = args.fields && args.fields.trim() ? args.fields : 'id';

    const validOperations = ['batch_create', 'batch_update', 'batch_delete'];
    if (!validOperations.includes(args.operation)) {
      throw new Error(`Invalid operation: "${args.operation}". Valid operations are: ${validOperations.join(', ')}. For finding records, use find_records tool instead.`);
    }

    const results: any[] = [];
    const errors: any[] = [];
    let result: any;
    const batchSize = 5;

    switch (args.operation) {
      case 'batch_create':
        if (!args.dataArray || !Array.isArray(args.dataArray)) {
          throw new Error('dataArray (array) is required for batch_create operation');
        }
        const itemsWithId = args.dataArray.filter((item: any) => item.id !== undefined);
        if (itemsWithId.length > 0) {
          throw new Error(`CRITICAL: Do NOT include "id" field in batch_create operations. The database will automatically generate the id. Found "id" field in ${itemsWithId.length} item(s). Remove "id" from all data objects and try again.`);
        }
        
        for (let start = 0; start < args.dataArray.length; start += batchSize) {
          if (abortSignal?.aborted) break;
          const chunk = args.dataArray.slice(start, start + batchSize);
          await Promise.all(chunk.map(async (data, offset) => {
            const i = start + offset;
            if (abortSignal?.aborted) return;
          try {
            const createResult = await repo.create({ data, fields: safeFields });
            const createdId = createResult?.data?.[0]?.id || createResult?.id || 'unknown';
            results.push({
              success: true,
              index: i,
              id: createdId,
              data: createResult?.data?.[0] || createResult,
            });
          } catch (error: any) {
            const errorMsg = error?.message || String(error);
            logger.error(`[batch_dynamic_repository] Error creating record ${i + 1}/${args.dataArray.length}: ${errorMsg}`);
            errors.push({
              index: i,
              error: 'CREATE_FAILED',
              message: errorMsg,
              data: data,
            });
            results.push({
              success: false,
              index: i,
              error: 'CREATE_FAILED',
              message: errorMsg,
            });
          }
          }));
        }
        
        const successCount = results.filter(r => r.success).length;
        const failureCount = results.filter(r => !r.success).length;
        
        const succeededItems = results.filter(r => r.success).map(r => `ID ${r.id}`).join(', ');
        const failedItems = results.filter(r => !r.success).map((r, idx) => `Item ${r.index + 1}`).join(', ');
        
        return {
          success: failureCount === 0,
          total: args.dataArray.length,
          succeeded: successCount,
          failed: failureCount,
          results,
          errors: errors.length > 0 ? errors : undefined,
          summary: failureCount === 0
            ? `Successfully created ${successCount} record(s) in table "${args.table}". Created IDs: ${succeededItems}.`
            : `Created ${successCount} record(s), ${failureCount} failed in table "${args.table}". ${succeededItems ? `Succeeded: ${succeededItems}. ` : ''}${failedItems ? `Failed: ${failedItems}.` : ''}`,
          message: failureCount === 0
            ? `✅ Successfully created ${successCount} record(s) in table "${args.table}".\n\n📋 Created IDs: ${succeededItems}`
            : `⚠️ Created ${successCount} record(s), ${failureCount} failed in table "${args.table}".\n\n${succeededItems ? `✅ Succeeded (${successCount}): ${succeededItems}\n\n` : ''}${failedItems ? `❌ Failed (${failureCount}): ${failedItems}\n\nCheck errors array for details.` : ''}`,
        };

      case 'batch_update':
        if (!args.updates || !Array.isArray(args.updates)) {
          throw new Error('updates (array of {id, data}) is required for batch_update operation');
        }
        
        for (let start = 0; start < args.updates.length; start += batchSize) {
          if (abortSignal?.aborted) break;
          const chunk = args.updates.slice(start, start + batchSize);
          await Promise.all(chunk.map(async (update, offset) => {
            const i = start + offset;
            if (abortSignal?.aborted) return;
          try {
            const updateResult = await repo.update({ id: update.id, data: update.data, fields: safeFields });
            results.push({
              success: true,
              index: i,
              id: update.id,
              data: updateResult?.data?.[0] || updateResult,
            });
          } catch (error: any) {
            const errorMsg = error?.message || String(error);
            logger.error(`[batch_dynamic_repository] Error updating record ${i + 1}/${args.updates.length} (ID: ${update.id}): ${errorMsg}`);
            errors.push({
              index: i,
              id: update.id,
              error: 'UPDATE_FAILED',
              message: errorMsg,
              data: update.data,
            });
            results.push({
              success: false,
              index: i,
              id: update.id,
              error: 'UPDATE_FAILED',
              message: errorMsg,
            });
          }
          }));
        }
        
        const updateSuccessCount = results.filter(r => r.success).length;
        const updateFailureCount = results.filter(r => !r.success).length;
        
        const succeededUpdateIds = results.filter(r => r.success).map(r => `ID ${r.id}`).join(', ');
        const failedUpdateIds = results.filter(r => !r.success).map(r => `ID ${r.id}`).join(', ');
        
        return {
          success: updateFailureCount === 0,
          total: args.updates.length,
          succeeded: updateSuccessCount,
          failed: updateFailureCount,
          results,
          errors: errors.length > 0 ? errors : undefined,
          summary: updateFailureCount === 0
            ? `Successfully updated ${updateSuccessCount} record(s) in table "${args.table}". Updated IDs: ${succeededUpdateIds}.`
            : `Updated ${updateSuccessCount} record(s), ${updateFailureCount} failed in table "${args.table}". ${succeededUpdateIds ? `Succeeded: ${succeededUpdateIds}. ` : ''}${failedUpdateIds ? `Failed: ${failedUpdateIds}.` : ''}`,
          message: updateFailureCount === 0
            ? `✅ Successfully updated ${updateSuccessCount} record(s) in table "${args.table}".\n\n📋 Updated IDs: ${succeededUpdateIds}`
            : `⚠️ Updated ${updateSuccessCount} record(s), ${updateFailureCount} failed in table "${args.table}".\n\n${succeededUpdateIds ? `✅ Succeeded (${updateSuccessCount}): ${succeededUpdateIds}\n\n` : ''}${failedUpdateIds ? `❌ Failed (${updateFailureCount}): ${failedUpdateIds}\n\nCheck errors array for details.` : ''}`,
        };

      case 'batch_delete':
        if (!args.ids || !Array.isArray(args.ids)) {
          throw new Error('ids (array) is required for batch_delete operation');
        }
        
        for (let start = 0; start < args.ids.length; start += batchSize) {
          if (abortSignal?.aborted) break;
          const chunk = args.ids.slice(start, start + batchSize);
          await Promise.all(chunk.map(async (id, offset) => {
            const i = start + offset;
            if (abortSignal?.aborted) return;
          try {
            await repo.delete({ id });
            results.push({
              success: true,
              index: i,
              id: id,
            });
          } catch (error: any) {
            const errorMsg = error?.message || String(error);
            logger.error(`[batch_dynamic_repository] Error deleting record ${i + 1}/${args.ids.length} (ID: ${id}): ${errorMsg}`);
            errors.push({
              index: i,
              id: id,
              error: 'DELETE_FAILED',
              message: errorMsg,
            });
            results.push({
              success: false,
              index: i,
              id: id,
              error: 'DELETE_FAILED',
              message: errorMsg,
            });
          }
          }));
        }
        
        const deleteSuccessCount = results.filter(r => r.success).length;
        const deleteFailureCount = results.filter(r => !r.success).length;
        
        const succeededDeleteIds = results.filter(r => r.success).map(r => `ID ${r.id}`).join(', ');
        const failedDeleteIds = results.filter(r => !r.success).map(r => `ID ${r.id}`).join(', ');
        
        return {
          success: deleteFailureCount === 0,
          total: args.ids.length,
          succeeded: deleteSuccessCount,
          failed: deleteFailureCount,
          results,
          errors: errors.length > 0 ? errors : undefined,
          summary: deleteFailureCount === 0
            ? `Successfully deleted ${deleteSuccessCount} record(s) from table "${args.table}". Deleted IDs: ${succeededDeleteIds}.`
            : `Deleted ${deleteSuccessCount} record(s), ${deleteFailureCount} failed from table "${args.table}". ${succeededDeleteIds ? `Succeeded: ${succeededDeleteIds}. ` : ''}${failedDeleteIds ? `Failed: ${failedDeleteIds}.` : ''}`,
          message: deleteFailureCount === 0
            ? `✅ Successfully deleted ${deleteSuccessCount} record(s) from table "${args.table}".\n\n📋 Deleted IDs: ${succeededDeleteIds}`
            : `⚠️ Deleted ${deleteSuccessCount} record(s), ${deleteFailureCount} failed from table "${args.table}".\n\n${succeededDeleteIds ? `✅ Succeeded (${deleteSuccessCount}): ${succeededDeleteIds}\n\n` : ''}${failedDeleteIds ? `❌ Failed (${deleteFailureCount}): ${failedDeleteIds}\n\nCheck errors array for details.` : ''}`,
        };

      default:
        throw new Error(`Unknown batch operation: ${args.operation}`);
    }
  } catch (error: any) {
    const errorMessage = error?.message || error?.response?.message || String(error);
    logger.error(`[batch_dynamic_repository] Error in ${args.operation} on ${args.table}: ${errorMessage}`, error?.stack);
    const recovery = getRecoveryStrategy(error);
    const details = error?.details || error?.response?.details || {};

    const isPermissionError =
      errorMessage.includes('permission denied') ||
      errorMessage.includes('unauthorized') ||
      errorMessage.includes('forbidden') ||
      errorMessage.includes('access denied') ||
      error?.status === 401 ||
      error?.status === 403 ||
      error?.statusCode === 401 ||
      error?.statusCode === 403 ||
      error?.code === 'PERMISSION_DENIED';

    if (isPermissionError) {
      return {
        error: true,
        errorCode: 'PERMISSION_DENIED',
        message: errorMessage,
        userMessage: `❌ **Permission Denied**: You do not have permission to perform ${args.operation} operation on table "${args.table}".\n\n💡 **Note**: This operation cannot proceed. Please check your access rights or contact an administrator.`,
        suggestion: `You MUST inform the user: "You do not have permission to ${args.operation} on table ${args.table}. Please check your access rights or contact an administrator." Then STOP - do NOT retry this operation or call any other tools.`,
      };
    }

    const isConstraintError =
      (args.operation === 'batch_create' || args.operation === 'batch_update') &&
      (errorMessage.includes('null value in column') ||
        errorMessage.includes('violates not-null constraint') ||
        errorMessage.includes('violates check constraint') ||
        errorMessage.includes('column') && errorMessage.includes('is required'));

    if (isConstraintError) {
      const columnMatch = errorMessage.match(/column "([^"]+)" of relation "([^"]+)"/);
      const columnName = columnMatch ? columnMatch[1] : 'unknown';
      const tableName = columnMatch ? columnMatch[2] : args.table;

      const providedFields = args.dataArray && args.dataArray.length > 0 ? Object.keys(args.dataArray[0]) : (args.updates && args.updates.length > 0 ? Object.keys(args.updates[0].data || {}) : []);
      
      const snakeToCamel = (str: string) => str.replace(/_([a-z])/g, (_, letter) => letter.toUpperCase());
      const camelToSnake = (str: string) => str.replace(/([a-z])([A-Z])/g, '$1_$2').toLowerCase();
      
      const columnCamelCase = snakeToCamel(columnName);
      const columnSnakeCase = camelToSnake(columnName);
      
      const hasColumn = providedFields.includes(columnName) || providedFields.includes(columnCamelCase) || providedFields.includes(columnSnakeCase);
      
      if (!hasColumn) {
        return {
          error: true,
          errorCode: 'MISSING_REQUIRED_FIELD',
          message: errorMessage,
          userMessage: `❌ **Schema Error**: Missing required field "${columnName}" in ${args.operation} operation on table "${tableName}".\n\n📋 **Action Required**: Call get_table_details FIRST to check the table schema, then include ALL required fields (not-null, non-generated) in your data.\n\n💡 **Hint**: Column names might be in snake_case (e.g., "${columnSnakeCase}") or camelCase (e.g., "${columnCamelCase}"). Check the schema to get the exact column name.`,
          suggestion: `Call get_table_details with tableName="${tableName}" to check the schema, then update your data to include the required field "${columnName}".`,
          columnName,
          tableName,
        };
      }
    }

    const escalation = shouldEscalateToHuman({
      operation: args.operation,
      table: args.table,
      error,
    });

    if (escalation.shouldEscalate) {
      const escalationMessage = formatEscalationMessage(escalation);
      const userFacing = escalationMessage || formatErrorForUser(error);
      return {
        error: true,
        errorType: recovery.errorType,
        message: errorMessage,
        userMessage: userFacing,
        suggestion: recovery.suggestion || 'Please review the error and try again.',
        escalation: true,
      };
    }

    return {
      error: true,
      errorType: recovery.errorType,
      message: errorMessage,
      userMessage: formatErrorForUser(error),
      suggestion: recovery.suggestion || 'Please review the error and try again.',
      details,
    };
  }
}

