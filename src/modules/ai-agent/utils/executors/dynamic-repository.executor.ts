import { Logger } from '@nestjs/common';
import { EventEmitter2 } from '@nestjs/event-emitter';
import { DynamicRepository } from '../../../dynamic-api/repositories/dynamic.repository';
import { TDynamicContext } from '../../../../shared/types';
import {
  formatErrorForUser,
  shouldEscalateToHuman,
  formatEscalationMessage,
  getRecoveryStrategy,
} from '../error-recovery.helper';
import { executeCheckPermission } from './check-permission.executor';
import { DynamicRepositoryExecutorDependencies } from '../../types';
const logger = new Logger('DynamicRepositoryExecutor');
export async function executeDynamicRepository(
  args: {
    table: string;
    operation: 'find' | 'findOne' | 'create' | 'update' | 'delete';
    where?: any;
    fields?: string;
    limit?: number;
    sort?: string;
    meta?: string;
    data?: any;
    id?: string | number;
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
  if (typeof args.table === 'string') {
    args.table = args.table.trim();
  }
  if (!args.table) {
    logger.debug(JSON.stringify({
      layer: 'dynamic_repository',
      stage: 'validation',
      error: 'MISSING_TABLE',
      operation: args.operation,
      argsPreview: (() => {
        const clone = { ...args };
        if (clone.data) {
          clone.data = '[omitted]';
        }
        return clone;
      })(),
    }));
    return {
      error: true,
      errorCode: 'MISSING_TABLE',
      message: 'Table parameter is required for this operation',
      userMessage: `❌ **Missing Table Information**: You must specify the target table before calling this tool.\n\n📋 **Next Steps**:\n1. Identify the table name using get_table_details or find_records on table_definition\n2. Retry this tool with a valid table parameter (example: {"table":"table_definition","filter":{...}})\n\n💡 **Tip**: Always fetch table metadata first when the user has not provided the exact table name.`,
      suggestion: 'Find the table name or ID first (get_table_details or find_records on table_definition), then call this tool with the table parameter.',
    };
  }
  if (args.table === 'table_definition' && (args.operation === 'create' || args.operation === 'update')) {
    return {
      error: true,
      errorCode: 'INVALID_OPERATION',
      message: `Cannot use ${args.operation === 'create' ? 'create_record' : 'update_record'} to ${args.operation} table_definition. Use create_tables or update_tables tool instead.`,
      userMessage: `❌ **Invalid Operation**: You cannot use ${args.operation === 'create' ? 'create_record' : 'update_record'} to ${args.operation} table_definition.\n\n📋 **Action Required**: Use the correct tool:\n- To create a new table: Use \`create_tables\` tool\n- To update an existing table: Use \`update_tables\` tool\n\n💡 **Note**: Table schema operations (create/update) must use the dedicated table management tools.`,
      suggestion: `Use ${args.operation === 'create' ? 'create_tables' : 'update_tables'} tool instead for table_definition operations.`,
    };
  }
  if (args.operation === 'findOne') {
    args.operation = 'find' as any;
    if (!args.limit || args.limit > 1) {
      args.limit = 1;
    }
  }
  let filterValue = (args as any).filter ?? args.where;
  if (filterValue != null && typeof filterValue === 'string') {
    try {
      const trimmed = filterValue.trim();
      if (trimmed && (trimmed.startsWith('{') || trimmed.startsWith('['))) {
        filterValue = JSON.parse(trimmed);
      }
    } catch (e) {
      logger.warn(`[dynamic_repository] Failed to parse filter string for ${args.table}: ${(e as Error)?.message}`);
    }
  }
  const {
    queryBuilder,
    tableHandlerService,
    queryEngine,
    metadataCacheService,
    systemProtectionService,
    tableValidationService,
    eventEmitter,
  } = deps;
  const repo = new DynamicRepository({
    context,
    tableName: args.table,
    queryBuilder,
    tableHandlerService,
    queryEngine,
    metadataCacheService,
    systemProtectionService,
    tableValidationService,
    eventEmitter,
  });
  await repo.init();
  const isMetadataTable = args.table.endsWith('_definition');
  const needsPermissionCheck =
    !isMetadataTable &&
    ['find', 'create', 'update', 'delete'].includes(args.operation);
  if (needsPermissionCheck) {
    const permissionCache: Map<string, any> =
      ((context as any).__permissionCache as Map<string, any>) ||
      (((context as any).__permissionCache = new Map<string, any>()) as Map<string, any>);
    const userId = context.$user?.id;
    const operation = args.operation === 'find' || args.operation === 'findOne' ? 'read' : args.operation;
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
  const preview: Record<string, any> = {
    operation: args.operation,
    table: args.table,
    id: args.id,
    meta: args.meta,
  };
  const filterVal = (args as any).filter ?? args.where;
  if (filterVal) {
    preview.filter = filterVal;
  }
  if (args.data) {
    preview.dataKeys = Object.keys(args.data);
  }
  try {
    if (args.operation === 'delete' && !args.id) {
      if (!filterValue) {
        throw new Error('id or filter is required for delete operation');
      }
      const lookup = await repo.find({
        where: filterValue,
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
    const safeFields = args.fields && args.fields.trim() ? args.fields : 'id';
    let result: any;
    switch (args.operation) {
      case 'find':
        result = await repo.find({
          where: filterValue,
          fields: args.fields,
          limit: args.limit,
          sort: args.sort,
          meta: args.meta,
        });
        return result;
      case 'create':
        if (!args.data) {
          throw new Error('data is required for create operation');
        }
        if (args.data.id !== undefined) {
          throw new Error('CRITICAL: Do NOT include "id" field in create operations. The database will automatically generate the id. Remove "id" from your data object and try again.');
        }
        result = await repo.create({ data: args.data, fields: safeFields });
        return result;
      case 'update':
        if (!args.id) {
          throw new Error('id is required for update operation');
        }
        if (!args.data) {
          throw new Error('data is required for update operation');
        }
        result = await repo.update({ id: args.id, data: args.data, fields: safeFields });
        return result;
      case 'delete':
        if (!args.id) {
          throw new Error('id is required for delete operation');
        }
        result = await repo.delete({ id: args.id });
        return result;
      default:
        throw new Error(`Unknown operation: ${args.operation}`);
    }
  } catch (error: any) {
    const errorMessage = error?.message || error?.response?.message || String(error);
    logger.error(`[dynamic_repository] Error in ${args.operation} on ${args.table}: ${errorMessage}`, error?.stack);
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
        userMessage: `❌ **Foreign Key Constraint Error**: The value for "${fkColumn}" in table "${args.table}" references a record that doesn't exist in table "${refTable}".\n\n📋 **Action Required**:\n1. Call get_table_details with tableName="${args.table}" to see the relation structure\n2. Call find_records to check if the referenced record exists\n   - Example: {"table":"${refTable}","filter":{"id":{"_eq":<your_id>}},"fields":"id","limit":1}\n3. If the record doesn't exist, create it first or use an existing ID\n4. NEVER use hardcoded IDs (like ${fkColumn}: 1) without verifying they exist\n\n💡 **Note**: Always verify foreign key references exist BEFORE creating records with foreign keys.`,
        suggestion: `Call find_records to verify the referenced record exists in table "${refTable}" before creating the record.`,
        details: {
          ...details,
          foreignKeyColumn: fkColumn,
          referencedTable: refTable,
          table: args.table,
        },
      };
    }
    const isTableNameAsIdError =
      (args.operation === 'delete' || args.operation === 'update') &&
      (errorMessage.includes('operator does not exist: character varying = uuid') ||
        errorMessage.includes('operator does not exist') && errorMessage.includes('character varying')) &&
      ((filterValue as any)?.id?._eq === args.table || args.id === args.table);
    if (isTableNameAsIdError) {
      return {
        error: true,
        errorType: 'INVALID_INPUT',
        errorCode: 'TABLE_NAME_AS_ID',
        message: `Cannot use table name "${args.table}" as id value. To delete a TABLE (not data), you must delete the table_definition record.`,
        userMessage: `❌ **Error**: You cannot use table name "${args.table}" as an id value.\n\n📋 **To DELETE/DROP a TABLE** (not data records), you MUST:\n1. Find the table_definition record: find_records({"table":"table_definition","filter":{"name":{"_eq":"${args.table}"}},"fields":"id,name","limit":1})\n2. Get the id (number) from the result\n3. Delete the table using delete_tables tool: delete_tables({"ids":[<id_from_step_1>]})\n\n💡 **Note**: Using delete_record on data tables (${args.table}) only deletes data records, NOT the table itself. To delete the table structure, you must use delete_tables tool.`,
        suggestion: `To delete table "${args.table}", first find it in table_definition: find_records({"table":"table_definition","filter":{"name":{"_eq":"${args.table}"}},"fields":"id,name","limit":1}). Then use the id (number) from the result to delete: delete_tables({"ids":[<id>]}).`,
        details: {
          ...details,
          table: args.table,
          operation: args.operation,
          incorrectIdValue: (filterValue as any)?.id?._eq || args.id,
        },
      };
    }
    const isColumnNotExistError =
      errorMessage.includes('column') &&
      (errorMessage.includes('does not exist') ||
        errorMessage.includes('Invalid column in query'));
    if (isColumnNotExistError) {
      const columnMatch = errorMessage.match(/column "([^"]+)"|column ([^\s]+) does not exist/i);
      const columnName = columnMatch ? (columnMatch[1] || columnMatch[2]) : 'unknown';
      const tableMatch = errorMessage.match(/from "([^"]+)"|table "([^"]+)"/i);
      const tableName = tableMatch ? (tableMatch[1] || tableMatch[2]) : args.table;
      const usedInFilter = filterValue ? JSON.stringify(filterValue) : '';
      const usedInFields = args.fields || '';
      return {
        error: true,
        errorType: 'INVALID_INPUT',
        errorCode: 'COLUMN_DOES_NOT_EXIST',
        message: errorMessage,
        userMessage: `❌ **Column Does Not Exist**: Column "${columnName}" does not exist in table "${tableName}".\n\n📋 **CRITICAL ERROR - You skipped schema validation!**\n\n🔧 **MANDATORY FIX - Follow these steps:**\n1. **STOP** - Do NOT retry the same query\n2. Call get_table_details FIRST: get_table_details({"tableName": ["${tableName}"]})\n3. **WAIT** for the result - DO NOT proceed until you have the schema\n4. Check result.columns[].name to see ALL available column names\n5. Check result.relations[].propertyName to see available relation properties\n6. Use ONLY field names that exist in result.columns[].name\n7. If you need "${columnName}", check if it exists as:\n   - A column in result.columns[].name\n   - A relation property in result.relations[].propertyName\n   - A field in a related table (check relations)\n8. Retry query with verified fields ONLY\n\n⚠️ **What you did wrong:**\n- You used field "${columnName}" without checking if it exists in the schema\n- You skipped the get_table_details step\n- You guessed the field name instead of verifying it\n\n💡 **Remember:** ALWAYS call get_table_details BEFORE any query operation. NEVER guess field names.`,
        suggestion: `Call get_table_details({"tableName": ["${tableName}"]}) FIRST, wait for result, then use ONLY fields from result.columns[].name. If "${columnName}" doesn't exist, check relations or related tables.`,
        details: {
          ...details,
          invalidColumn: columnName,
          table: tableName,
          usedInFilter,
          usedInFields,
        },
      };
    }
    const isConstraintError =
      (args.operation === 'create' || args.operation === 'update') &&
      (errorMessage.includes('null value in column') ||
        errorMessage.includes('violates not-null constraint') ||
        errorMessage.includes('violates check constraint') ||
        errorMessage.includes('column') && errorMessage.includes('is required'));
    if (isConstraintError) {
      const columnMatch = errorMessage.match(/column "([^"]+)" of relation "([^"]+)"/);
      const columnName = columnMatch ? columnMatch[1] : 'unknown';
      const tableName = columnMatch ? columnMatch[2] : args.table;
      const providedFields = args.data ? Object.keys(args.data) : [];
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