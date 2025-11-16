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
import { executeCheckPermission, CheckPermissionExecutorDependencies } from './check-permission.executor';

const logger = new Logger('BatchDynamicRepositoryExecutor');

export interface BatchDynamicRepositoryExecutorDependencies extends CheckPermissionExecutorDependencies {
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

export async function executeBatchDynamicRepository(
  args: {
    table: string;
    operation: 'batch_create' | 'batch_update' | 'batch_delete';
    fields?: string;
    dataArray?: any[];
    updates?: Array<{ id: string | number; data: any }>;
    ids?: Array<string | number>;
    skipPermissionCheck?: boolean;
  },
  context: TDynamicContext,
  abortSignal: AbortSignal | undefined,
  deps: BatchDynamicRepositoryExecutorDependencies,
): Promise<any> {
  logger.debug(`[batch_dynamic_repository] Called with operation=${args.operation}, table=${args.table}`, {
    operation: args.operation,
    table: args.table,
    hasDataArray: !!args.dataArray,
    hasUpdates: !!args.updates,
    hasIds: !!args.ids,
    fields: args.fields,
  });

  if (abortSignal?.aborted) {
    logger.debug(`[batch_dynamic_repository] Request aborted`);
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
    logger.debug(`[batch_dynamic_repository] Checking permission for ${args.operation} on ${args.table}`);
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
        logger.debug(`[batch_dynamic_repository] Permission check result: allowed=${permissionResult?.allowed}, reason=${permissionResult?.reason || 'N/A'}`);
        if (!permissionResult?.allowed) {
          logger.debug(`[batch_dynamic_repository] Permission denied for ${operation} on ${args.table}`);
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
      logger.debug(`[batch_dynamic_repository] Using cached permission: allowed=${permissionResult?.allowed}`);
      if (!permissionResult?.allowed) {
        logger.debug(`[batch_dynamic_repository] Cached permission denied for ${operation} on ${args.table}`);
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

    let result: any;
    switch (args.operation) {
      case 'batch_create':
        if (!args.dataArray || !Array.isArray(args.dataArray)) {
          throw new Error('dataArray (array) is required for batch_create operation');
        }
        const itemsWithId = args.dataArray.filter((item: any) => item.id !== undefined);
        if (itemsWithId.length > 0) {
          throw new Error(`CRITICAL: Do NOT include "id" field in batch_create operations. The database will automatically generate the id. Found "id" field in ${itemsWithId.length} item(s). Remove "id" from all data objects and try again.`);
        }
        logger.debug(`[batch_dynamic_repository] Executing batch_create on ${args.table}`, { count: args.dataArray.length, fields: safeFields });
        result = await Promise.all(
          args.dataArray.map(data => repo.create({ data, fields: safeFields }))
        );
        logger.debug(`[batch_dynamic_repository] Batch_create result: ${result.length} records created`);
        return result;
      case 'batch_update':
        if (!args.updates || !Array.isArray(args.updates)) {
          throw new Error('updates (array of {id, data}) is required for batch_update operation');
        }
        logger.debug(`[batch_dynamic_repository] Executing batch_update on ${args.table}`, { count: args.updates.length, fields: safeFields });
        result = await Promise.all(
          args.updates.map(update => repo.update({ id: update.id, data: update.data, fields: safeFields }))
        );
        logger.debug(`[batch_dynamic_repository] Batch_update result: ${result.length} records updated`);
        return result;
      case 'batch_delete':
        if (!args.ids || !Array.isArray(args.ids)) {
          throw new Error('ids (array) is required for batch_delete operation');
        }
        logger.debug(`[batch_dynamic_repository] Executing batch_delete on ${args.table}`, { count: args.ids.length });
        result = await Promise.all(
          args.ids.map(id => repo.delete({ id }))
        );
        logger.debug(`[batch_dynamic_repository] Batch_delete result: ${result.length} records deleted`);
        return result;
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

