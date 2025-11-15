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

export interface DynamicRepositoryExecutorDependencies extends CheckPermissionExecutorDependencies {
  metadataCacheService: MetadataCacheService;
}

export async function executeDynamicRepository(
  args: {
    table: string;
    operation: 'find' | 'findOne' | 'create' | 'update' | 'delete' | 'batch_create' | 'batch_update' | 'batch_delete';
    where?: any;
    fields?: string;
    limit?: number;
    sort?: string;
    meta?: string;
    data?: any;
    id?: string | number;
    dataArray?: any[];
    updates?: Array<{ id: string | number; data: any }>;
    ids?: Array<string | number>;
    skipPermissionCheck?: boolean;
  },
  context: TDynamicContext,
  abortSignal: AbortSignal | undefined,
  deps: DynamicRepositoryExecutorDependencies,
): Promise<any> {
  if (abortSignal?.aborted) {
    return {
      error: true,
      errorCode: 'REQUEST_ABORTED',
      message: 'Request aborted by client',
    };
  }

  if (args.table === 'table_definition' && (args.operation === 'create' || args.operation === 'update' || args.operation === 'batch_create' || args.operation === 'batch_update')) {
    return {
      error: true,
      errorCode: 'INVALID_OPERATION',
      message: `Cannot use dynamic_repository to ${args.operation} table_definition. Use create_table or update_table tool instead.`,
      userMessage: `❌ **Invalid Operation**: You cannot use dynamic_repository to ${args.operation} table_definition.\n\n📋 **Action Required**: Use the correct tool:\n- To create a new table: Use \`create_table\` tool\n- To update an existing table: Use \`update_table\` tool\n\n💡 **Note**: Table schema operations (create/update) must use the dedicated table management tools, not dynamic_repository.`,
      suggestion: `Use ${args.operation === 'create' || args.operation === 'batch_create' ? 'create_table' : 'update_table'} tool instead of dynamic_repository for table_definition operations.`,
    };
  }

  if (args.operation === 'findOne') {
    args.operation = 'find' as any;
    if (!args.limit || args.limit > 1) {
      args.limit = 1;
    }
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
    bootstrapScriptService: undefined,
    redisPubSubService: undefined,
    swaggerService,
    graphqlService,
  });

  await repo.init();

  const isMetadataTable = args.table.endsWith('_definition');
  const needsPermissionCheck =
    !args.skipPermissionCheck &&
    !isMetadataTable &&
    ['find', 'create', 'update', 'delete', 'batch_create', 'batch_update', 'batch_delete'].includes(args.operation);

  if (needsPermissionCheck) {
    const permissionCache: Map<string, any> =
      ((context as any).__permissionCache as Map<string, any>) ||
      (((context as any).__permissionCache = new Map<string, any>()) as Map<string, any>);

    const userId = context.$user?.id;
    const operation = args.operation === 'find' || args.operation === 'findOne' ? 'read' : args.operation === 'batch_create' ? 'create' : args.operation === 'batch_update' ? 'update' : args.operation === 'batch_delete' ? 'delete' : args.operation;
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

  const preview: Record<string, any> = {
    operation: args.operation,
    table: args.table,
    id: args.id,
    meta: args.meta,
  };
  if (args.where) {
    preview.where = args.where;
  }
  if (args.data) {
    preview.dataKeys = Object.keys(args.data);
  }
  if (args.dataArray) {
    preview.dataArrayLength = Array.isArray(args.dataArray) ? args.dataArray.length : 0;
  }
  if (args.updates) {
    preview.updatesLength = Array.isArray(args.updates) ? args.updates.length : 0;
  }
  if (args.ids) {
    preview.idsLength = Array.isArray(args.ids) ? args.ids.length : 0;
  }

  try {
    if (args.operation === 'delete' && !args.id) {
      if (!args.where) {
        throw new Error('id or where is required for delete operation');
      }

      const lookup = await repo.find({
        where: args.where,
        fields: 'id',
        limit: 0,
      });

      const records = (lookup.data || []).map((item: any) => item.id || item._id).filter(Boolean);

      if (records.length === 0) {
        throw new Error('No records found for delete operation');
      }

      if (records.length === 1) {
        args.id = records[0];
      } else {
        const deleteResults = [];
        for (const recordId of records) {
          deleteResults.push(await repo.delete({ id: recordId }));
        }
        return {
          deleted: records.length,
          ids: records,
          results: deleteResults,
        };
      }
    }

    switch (args.operation) {
      case 'find':
        return await repo.find({
          where: args.where,
          fields: args.fields,
          limit: args.limit,
          sort: args.sort,
          meta: args.meta,
        });
      case 'create':
        if (!args.data) {
          throw new Error('data is required for create operation');
        }
        return await repo.create({ data: args.data, fields: args.fields });
      case 'update':
        if (!args.id) {
          throw new Error('id is required for update operation');
        }
        if (!args.data) {
          throw new Error('data is required for update operation');
        }
        return await repo.update({ id: args.id, data: args.data, fields: args.fields });
      case 'delete':
        if (!args.id) {
          throw new Error('id is required for delete operation');
        }
        return await repo.delete({ id: args.id });
      case 'batch_create':
        if (!args.dataArray || !Array.isArray(args.dataArray)) {
          throw new Error('dataArray (array) is required for batch_create operation');
        }
        return Promise.all(
          args.dataArray.map(data => repo.create({ data, fields: args.fields }))
        );
      case 'batch_update':
        if (!args.updates || !Array.isArray(args.updates)) {
          throw new Error('updates (array of {id, data}) is required for batch_update operation');
        }
        return Promise.all(
          args.updates.map(update => repo.update({ id: update.id, data: update.data, fields: args.fields }))
        );
      case 'batch_delete':
        if (!args.ids || !Array.isArray(args.ids)) {
          throw new Error('ids (array) is required for batch_delete operation');
        }
        return Promise.all(
          args.ids.map(id => repo.delete({ id }))
        );
      default:
        throw new Error(`Unknown operation: ${args.operation}`);
    }
  } catch (error: any) {
    const errorMessage = error?.message || error?.response?.message || String(error);
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
      const isMetadataTable = args.table?.endsWith('_definition');
      return {
        error: true,
        errorCode: 'PERMISSION_DENIED',
        message: errorMessage,
        userMessage: `❌ **Permission Denied**: Operation ${args.operation} on table "${args.table}" was denied.\n\n📋 **Error**: ${errorMessage}\n\n💡 **Note**: You do not have permission to perform this operation. Please check your access rights or contact an administrator.`,
        suggestion: `You MUST inform the user: "You do not have permission to ${args.operation} on table ${args.table}. Please check your access rights or contact an administrator." Then STOP - do NOT retry this operation or call any other tools.`,
        details: {
          ...details,
          table: args.table,
          operation: args.operation,
        },
      };
    }

    const isForeignKeyError =
      errorMessage.includes('violates foreign key constraint') ||
      errorMessage.includes('foreign key constraint') ||
      errorMessage.includes('Key (') && errorMessage.includes(') is not present in table');

    if (isForeignKeyError) {
      const fkMatch = errorMessage.match(/Key \(([^)]+)\)/);
      const fkColumn = fkMatch ? fkMatch[1] : 'unknown';
      const refTableMatch = errorMessage.match(/table "([^"]+)"/);
      const refTable = refTableMatch ? refTableMatch[1] : 'unknown';

      return {
        error: true,
        errorType: 'INVALID_INPUT',
        errorCode: 'FOREIGN_KEY_VIOLATION',
        message: errorMessage,
        userMessage: `❌ **Foreign Key Constraint Error**: The value for "${fkColumn}" in table "${args.table}" references a record that doesn't exist in table "${refTable}".\n\n📋 **Action Required**:\n1. Call get_table_details with tableName="${args.table}" to see the relation structure\n2. Call dynamic_repository with find operation to check if the referenced record exists\n   - Example: {"table":"${refTable}","operation":"find","where":{"id":{"_eq":<your_id>}},"fields":"id"}\n3. If the record doesn't exist, create it first or use an existing ID\n4. NEVER use hardcoded IDs (like ${fkColumn}: 1) without verifying they exist\n\n💡 **Note**: Always verify foreign key references exist BEFORE creating records with foreign keys.`,
        suggestion: `Call dynamic_repository with find operation to verify the referenced record exists in table "${refTable}" before creating the record.`,
        details: {
          ...details,
          foreignKeyColumn: fkColumn,
          referencedTable: refTable,
          table: args.table,
        },
      };
    }

    const isConstraintError =
      (args.operation === 'create' || args.operation === 'batch_create' || args.operation === 'update' || args.operation === 'batch_update') &&
      (errorMessage.includes('null value in column') ||
        errorMessage.includes('violates not-null constraint') ||
        errorMessage.includes('violates check constraint') ||
        errorMessage.includes('column') && errorMessage.includes('is required'));

    if (isConstraintError) {
      const columnMatch = errorMessage.match(/column "([^"]+)" of relation "([^"]+)"/);
      const columnName = columnMatch ? columnMatch[1] : 'unknown';
      const tableName = columnMatch ? columnMatch[2] : args.table;

      const providedFields = args.data ? Object.keys(args.data) : (args.dataArray && args.dataArray.length > 0 ? Object.keys(args.dataArray[0]) : []);
      
      const snakeToCamel = (str: string) => str.replace(/_([a-z])/g, (_, letter) => letter.toUpperCase());
      const camelToSnake = (str: string) => str.replace(/([a-z])([A-Z])/g, '$1_$2').toLowerCase();
      
      const columnCamelCase = snakeToCamel(columnName);
      const possibleMismatch = providedFields.find((field: string) => {
        const fieldSnakeCase = camelToSnake(field);
        return field === columnCamelCase || 
               fieldSnakeCase === columnName ||
               field.toLowerCase() === columnName.toLowerCase() ||
               field.replace(/_/g, '').toLowerCase() === columnName.replace(/_/g, '').toLowerCase();
      });

      const isFkColumn = columnName.includes('_id') || columnName.toLowerCase().endsWith('id');
      const possibleRelationField = providedFields.find((field: string) => {
        const fieldSnakeCase = camelToSnake(field);
        const fieldWithoutId = field.replace(/[Ii]d$/, '').replace(/_id$/, '');
        const columnWithoutId = columnName.replace(/_id$/, '').replace(/[Ii]d$/, '');
        return fieldWithoutId === columnWithoutId || fieldSnakeCase.replace(/_id$/, '') === columnName.replace(/_id$/, '');
      });

      let mismatchHint = '';
      if (possibleMismatch && possibleMismatch !== columnName) {
        mismatchHint = `\n\n⚠️ **Column Name Mismatch Detected**: You used "${possibleMismatch}" but the database column is "${columnName}". Column names in the database are in snake_case (e.g., order_number, unit_price), not camelCase (e.g., orderNumber, unitPrice). Always use the exact column names from get_table_details.`;
      }
      
      if (isFkColumn && possibleRelationField && possibleRelationField !== columnName) {
        mismatchHint += `\n\n⚠️ **Relation Format Error**: You used FK column "${possibleRelationField}" (or similar), but relations should use propertyName from get_table_details result.relations[]. Use propertyName format: {"${possibleRelationField.replace(/[Ii]d$/, '').replace(/_id$/, '')}": {"id": <value>}} OR {"${possibleRelationField.replace(/[Ii]d$/, '').replace(/_id$/, '')}": <value>}. NEVER use FK column names like "${columnName}" - system auto-generates them from propertyName.`;
      }

      return {
        error: true,
        errorType: 'INVALID_INPUT',
        errorCode: 'MISSING_REQUIRED_FIELD',
        message: errorMessage,
        userMessage: `❌ **Schema Constraint Error**: Missing required field "${columnName}" in table "${tableName}".${mismatchHint}\n\n📋 **Action Required**:\n1. Call get_table_details with tableName="${tableName}" to get the full schema with EXACT column names\n2. Check which columns are required (isNullable=false) and have default values\n3. Use EXACT column names from schema (snake_case format, e.g., order_number, unit_price, not camelCase)\n4. Update your data object to include ALL required fields with correct column names\n5. Retry the operation with complete data\n\n💡 **Note**: Always check schema BEFORE creating/updating records. Use the exact column names from get_table_details (they are in snake_case format).`,
        suggestion: `Call get_table_details with tableName="${tableName}" to see all required fields with exact column names (snake_case), then update your data object accordingly.`,
        details: {
          ...details,
          missingColumn: columnName,
          table: tableName,
          providedFields,
          possibleMismatch,
        },
      };
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
        errorCode: error?.errorCode || error?.response?.errorCode || recovery.errorType,
        message: recovery.message,
        userMessage: userFacing,
        details,
        requiresHumanConfirmation: true,
        escalationReason: escalation.reason,
        escalationMessage,
      };
    }

    const businessLogicError =
      error?.errorCode === 'BUSINESS_LOGIC_ERROR' ||
      error?.response?.errorCode === 'BUSINESS_LOGIC_ERROR';

    if (businessLogicError) {
      return {
        error: true,
        errorType: recovery.errorType,
        errorCode: error?.errorCode || error?.response?.errorCode || recovery.errorType,
        message: errorMessage,
        userMessage: '🛑 CRITICAL: STOP ALL OPERATIONS NOW! Inform the user about the business logic error and ask how to proceed. Do not call additional tools.',
        details,
      };
    }

    return {
      error: true,
      errorType: recovery.errorType,
      errorCode: error?.errorCode || error?.response?.errorCode || recovery.errorType,
      message: errorMessage,
      userMessage: formatErrorForUser(error),
      details,
    };
  }
}

