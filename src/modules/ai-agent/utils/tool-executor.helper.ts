import { Logger } from '@nestjs/common';
import { MetadataCacheService } from '../../../infrastructure/cache/services/metadata-cache.service';
import { DynamicRepository } from '../../dynamic-api/repositories/dynamic.repository';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { TableHandlerService } from '../../table-management/services/table-handler.service';
import { QueryEngine } from '../../../infrastructure/query-engine/services/query-engine.service';
import { RouteCacheService } from '../../../infrastructure/cache/services/route-cache.service';
import { StorageConfigCacheService } from '../../../infrastructure/cache/services/storage-config-cache.service';
import { AiConfigCacheService } from '../../../infrastructure/cache/services/ai-config-cache.service';
import { SystemProtectionService } from '../../dynamic-api/services/system-protection.service';
import { TableValidationService } from '../../dynamic-api/services/table-validation.service';
import { SwaggerService } from '../../../infrastructure/swagger/services/swagger.service';
import { GraphqlService } from '../../graphql/services/graphql.service';
import { TDynamicContext } from '../../../shared/interfaces/dynamic-context.interface';
import { optimizeMetadataForLLM } from './metadata-optimizer.helper';
import {
  formatErrorForUser,
  shouldEscalateToHuman,
  formatEscalationMessage,
  getRecoveryStrategy,
} from './error-recovery.helper';
import { TableCreationWorkflow } from './table-creation-workflow';
import { TableUpdateWorkflow } from './table-update-workflow';
import { ConversationService } from '../services/conversation.service';

export class ToolExecutor {
  private readonly logger = new Logger(ToolExecutor.name);

  constructor(
    private readonly metadataCacheService: MetadataCacheService,
    private readonly queryBuilder: QueryBuilderService,
    private readonly tableHandlerService: TableHandlerService,
    private readonly queryEngine: QueryEngine,
    private readonly routeCacheService: RouteCacheService,
    private readonly storageConfigCacheService: StorageConfigCacheService,
    private readonly aiConfigCacheService: AiConfigCacheService,
    private readonly systemProtectionService: SystemProtectionService,
    private readonly tableValidationService: TableValidationService,
    private readonly swaggerService: SwaggerService,
    private readonly graphqlService: GraphqlService,
    private readonly conversationService: ConversationService,
  ) {}

  async executeTool(
    toolCall: {
      id: string;
      function: {
        name: string;
        arguments: string;
      };
    },
    context: TDynamicContext,
    abortSignal?: AbortSignal,
  ): Promise<any> {
    if (abortSignal?.aborted) {
      return {
        error: true,
        errorCode: 'REQUEST_ABORTED',
        message: 'Request aborted by client',
      };
    }
    const { name, arguments: argsStr } = toolCall.function;
    let args: any;

    try {
      args = JSON.parse(argsStr);
    } catch (e) {
      throw new Error(`Invalid tool arguments: ${argsStr}`);
    }

    switch (name) {
      case 'check_permission':
        return await this.executeCheckPermission(args, context);
      case 'list_tables':
        return await this.executeListTables();
      case 'get_table_details':
        return await this.executeGetTableDetails(args, context);
      case 'get_fields':
        return await this.executeGetFields(args);
      case 'get_hint':
        return await this.executeGetHint(args, context);
      case 'create_table':
        return await this.executeCreateTable(args, context, abortSignal);
      case 'update_table':
        return await this.executeUpdateTable(args, context, abortSignal);
      case 'update_task':
        return await this.executeUpdateTask(args, context);
      case 'dynamic_repository':
        return await this.executeDynamicRepository(args, context, abortSignal);
      default:
        throw new Error(`Unknown tool: ${name}`);
    }
  }

  private async executeCheckPermission(
    args: { routePath?: string; table?: string; operation: 'read' | 'create' | 'update' | 'delete' },
    context: TDynamicContext,
  ): Promise<any> {
    const { routePath, table, operation } = args;
    const userId = context.$user?.id;

    const permissionCache: Map<string, any> =
      ((context as any).__permissionCache as Map<string, any>) ||
      (((context as any).__permissionCache = new Map<string, any>()) as Map<string, any>);
    const cacheKey = table
      ? `${userId || 'anon'}|${operation}|${table}|`
      : `${userId || 'anon'}|${operation}||${routePath || ''}`;

    if (permissionCache.has(cacheKey)) {
      return permissionCache.get(cacheKey);
    }

    const setCache = (result: any) => {
      const finalResult = { ...result, cacheKey };
      permissionCache.set(cacheKey, finalResult);
      return finalResult;
    };

    if (!userId) {
      return setCache({
        allowed: false,
        reason: 'not_authenticated',
        message: 'User is not authenticated. Please login first.',
      });
    }

    const operationToMethod: Record<string, string> = {
      read: 'GET',
      create: 'POST',
      update: 'PATCH',
      delete: 'DELETE',
    };
    const requiredMethod = operationToMethod[operation];

    const userRepo = new DynamicRepository({
      context,
      tableName: 'user_definition',
      queryBuilder: this.queryBuilder,
      tableHandlerService: this.tableHandlerService,
      queryEngine: this.queryEngine,
      routeCacheService: this.routeCacheService,
      storageConfigCacheService: this.storageConfigCacheService,
      aiConfigCacheService: this.aiConfigCacheService,
      metadataCacheService: this.metadataCacheService,
      systemProtectionService: this.systemProtectionService,
      tableValidationService: this.tableValidationService,
      bootstrapScriptService: undefined,
      redisPubSubService: undefined,
      swaggerService: this.swaggerService,
      graphqlService: this.graphqlService,
    });

    await userRepo.init();

    const userResult = await userRepo.find({
      where: { id: { _eq: userId } },
      fields: 'id,email,isRootAdmin,role.id,role.name',
      limit: 1,
    });

    if (!userResult || !userResult.data || userResult.data.length === 0) {
      return setCache({
        allowed: false,
        reason: 'user_not_found',
        message: 'User not found in the system.',
      });
    }

    const user = userResult.data[0];

    if (user.isRootAdmin === true) {
      return setCache({
        allowed: true,
        reason: 'root_admin',
        message: 'User is root admin with full access.',
        userInfo: {
          id: user.id,
          email: user.email,
          isRootAdmin: true,
          role: user.role || null,
        },
      });
    }

    let finalRoutePath = routePath;
    if (!finalRoutePath && table) {
      const tableName = table.replace(/_definition$/, '');
      finalRoutePath = `/${tableName}`;
    }

    if (!finalRoutePath) {
      return setCache({
        allowed: false,
        reason: 'no_route_specified',
        message: 'Cannot determine route path. Please provide routePath or table parameter.',
      });
    }

    const routeRepo = new DynamicRepository({
      context,
      tableName: 'route_definition',
      queryBuilder: this.queryBuilder,
      tableHandlerService: this.tableHandlerService,
      queryEngine: this.queryEngine,
      routeCacheService: this.routeCacheService,
      storageConfigCacheService: this.storageConfigCacheService,
      aiConfigCacheService: this.aiConfigCacheService,
      metadataCacheService: this.metadataCacheService,
      systemProtectionService: this.systemProtectionService,
      tableValidationService: this.tableValidationService,
      bootstrapScriptService: undefined,
      redisPubSubService: undefined,
      swaggerService: this.swaggerService,
      graphqlService: this.graphqlService,
    });

    await routeRepo.init();

    const routeResult = await routeRepo.find({
      where: { path: { _eq: finalRoutePath } },
      fields: 'id,path,routePermissions.methods.method,routePermissions.allowedUsers.id,routePermissions.role.id,routePermissions.role.name',
      limit: 1,
    });

    if (!routeResult || !routeResult.data || routeResult.data.length === 0) {
      if (operation === 'read') {
        return setCache({
          allowed: true,
          reason: 'route_not_found_public_read',
          message: `Route ${finalRoutePath} not found. Assuming public read access.`,
          userInfo: {
            id: user.id,
            email: user.email,
            isRootAdmin: false,
            role: user.role || null,
          },
        });
      } else {
        return setCache({
          allowed: false,
          reason: 'route_not_found_write_denied',
          message: `Route ${finalRoutePath} not found. Write operations require explicit permissions.`,
          userInfo: {
            id: user.id,
            email: user.email,
            isRootAdmin: false,
            role: user.role || null,
          },
        });
      }
    }

    const route = routeResult.data[0];
    const routePermissions = route.routePermissions || [];

    if (routePermissions.length === 0) {
      return setCache({
        allowed: false,
        reason: 'no_permissions_configured',
        message: `No permissions configured for ${finalRoutePath}.`,
        userInfo: {
          id: user.id,
          email: user.email,
          isRootAdmin: false,
          role: user.role || null,
        },
      });
    }

    for (const permission of routePermissions) {
      const allowedMethods = permission.methods || [];
      const hasMethodAccess = allowedMethods.some((m: any) => m.method === requiredMethod);

      if (!hasMethodAccess) {
        continue;
      }

      const allowedUsers = permission.allowedUsers || [];
      if (allowedUsers.some((u: any) => u?.id === userId)) {
        return setCache({
          allowed: true,
          reason: 'user_specific_access',
          message: `User has direct access to ${operation} on ${finalRoutePath}.`,
          userInfo: {
            id: user.id,
            email: user.email,
            isRootAdmin: false,
            role: user.role || null,
          },
        });
      }

      const allowedRole = permission.role || null;
      if (allowedRole && user.role && allowedRole.id === user.role.id) {
        return setCache({
          allowed: true,
          reason: 'role_based_access',
          message: `User has role-based access to ${operation} on ${finalRoutePath} via role: ${user.role.name || user.role.id}.`,
          userInfo: {
            id: user.id,
            email: user.email,
            isRootAdmin: false,
            role: user.role,
          },
        });
      }
    }

    return setCache({
      allowed: false,
      reason: 'permission_denied',
      message: `User does not have permission to ${operation} on ${finalRoutePath}.`,
      userInfo: {
        id: user.id,
        email: user.email,
        isRootAdmin: false,
        role: user.role || null,
      },
    });
  }

  private async executeListTables(): Promise<any> {
    const metadata = await this.metadataCacheService.getMetadata();
    const tablesList = Array.from(metadata.tables.entries()).map(([name, table]) => ({
      name,
      description: table.description || '',
    }));

    return {
      totalCount: tablesList.length,
      tables: tablesList,
    };
  }

  private async executeGetTableDetails(
    args: {
      tableName: string[];
      forceRefresh?: boolean;
      id?: string | number;
      name?: string;
      getData?: boolean;
    },
    context?: TDynamicContext,
  ): Promise<any> {
    if (args.forceRefresh) {
      await this.metadataCacheService.reload();
    }

    if (!Array.isArray(args.tableName)) {
      throw new Error('tableName must be an array. For single table, use array with 1 element: ["table_name"]');
    }

    const tableNames = args.tableName;

    if (tableNames.length === 0) {
      throw new Error('At least one table name is required');
    }

    const shouldGetData = args.getData === true && (args.id !== undefined || args.name !== undefined);

    if (tableNames.length === 1) {
      const tableName = tableNames[0];
      const metadata = await this.metadataCacheService.getTableMetadata(tableName);
      if (!metadata) {
        throw new Error(`Table ${tableName} not found`);
      }

      const result: any = optimizeMetadataForLLM(metadata);

      if (shouldGetData && context) {
        try {
          const repo = new DynamicRepository({
            context,
            tableName,
            queryBuilder: this.queryBuilder,
            tableHandlerService: this.tableHandlerService,
            queryEngine: this.queryEngine,
            routeCacheService: this.routeCacheService,
            storageConfigCacheService: this.storageConfigCacheService,
            aiConfigCacheService: this.aiConfigCacheService,
            metadataCacheService: this.metadataCacheService,
            systemProtectionService: this.systemProtectionService,
            tableValidationService: this.tableValidationService,
            bootstrapScriptService: undefined,
            redisPubSubService: undefined,
            swaggerService: this.swaggerService,
            graphqlService: this.graphqlService,
          });

          await repo.init();

          let where: any = {};
          if (args.id !== undefined) {
            where.id = { _eq: args.id };
          } else if (args.name !== undefined) {
            where.name = { _eq: args.name };
          }

          const dataResult = await repo.find({
            where,
            fields: '*',
            limit: 1,
          });

          if (dataResult?.data && dataResult.data.length > 0) {
            result.data = dataResult.data[0];
          } else {
            result.data = null;
          }
        } catch (error: any) {
          result.dataError = error.message;
        }
      }

      return result;
    }

    const result: Record<string, any> = {};
    const errors: string[] = [];

    for (const tableName of tableNames) {
      try {
        const metadata = await this.metadataCacheService.getTableMetadata(tableName);
        if (!metadata) {
          errors.push(`Table ${tableName} not found`);
          continue;
        }
        result[tableName] = optimizeMetadataForLLM(metadata);

        if (shouldGetData && context) {
          try {
            const repo = new DynamicRepository({
              context,
              tableName,
              queryBuilder: this.queryBuilder,
              tableHandlerService: this.tableHandlerService,
              queryEngine: this.queryEngine,
              routeCacheService: this.routeCacheService,
              storageConfigCacheService: this.storageConfigCacheService,
              aiConfigCacheService: this.aiConfigCacheService,
              metadataCacheService: this.metadataCacheService,
              systemProtectionService: this.systemProtectionService,
              tableValidationService: this.tableValidationService,
              bootstrapScriptService: undefined,
              redisPubSubService: undefined,
              swaggerService: this.swaggerService,
              graphqlService: this.graphqlService,
            });

            await repo.init();

            let where: any = {};
            if (args.id !== undefined) {
              where.id = { _eq: args.id };
            } else if (args.name !== undefined) {
              where.name = { _eq: args.name };
            }

            const dataResult = await repo.find({
              where,
              fields: '*',
              limit: 1,
            });

            if (dataResult?.data && dataResult.data.length > 0) {
              result[tableName].data = dataResult.data[0];
            } else {
              result[tableName].data = null;
            }
          } catch (error: any) {
            result[tableName].dataError = error.message;
          }
        }
      } catch (error: any) {
        errors.push(`Error loading ${tableName}: ${error.message}`);
      }
    }

    if (errors.length > 0) {
      result._errors = errors;
    }

    if (Object.keys(result).length === 0 && errors.length > 0) {
      result._allFailed = true;
    }

    return result;
  }

  private async executeGetFields(args: { tableName: string }): Promise<any> {
    const metadata = await this.metadataCacheService.getTableMetadata(args.tableName);
    if (!metadata) {
      throw new Error(`Table ${args.tableName} not found`);
    }

    const fieldNames = metadata.columns.map((col: any) => col.name);

    return {
      table: args.tableName,
      fields: fieldNames,
    };
  }

  private async executeCreateTable(
    args: {
      name: string;
      description?: string;
      columns: any[];
      relations?: any[];
      uniques?: any[][];
      indexes?: any[];
    },
    context: TDynamicContext,
    abortSignal?: AbortSignal,
  ): Promise<any> {
    if (abortSignal?.aborted) {
      return {
        error: true,
        errorCode: 'REQUEST_ABORTED',
        message: 'Request aborted by client',
      };
    }
    const workflow = new TableCreationWorkflow(
      this.metadataCacheService,
      this.queryBuilder,
      this.tableHandlerService,
      this.queryEngine,
      this.routeCacheService,
      this.storageConfigCacheService,
      this.aiConfigCacheService,
      this.systemProtectionService,
      this.tableValidationService,
      this.swaggerService,
      this.graphqlService,
    );

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
      return {
        error: true,
        errorCode: 'WORKFLOW_ERROR',
        message: errorDetails,
        errors: workflowResult.errors,
        stopReason: workflowResult.stopReason,
      };
    }

    return workflowResult.result;
  }

  private async executeUpdateTable(
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
    abortSignal?: AbortSignal,
  ): Promise<any> {
    if (abortSignal?.aborted) {
      return {
        error: true,
        errorCode: 'REQUEST_ABORTED',
        message: 'Request aborted by client',
      };
    }
    try {

      const workflow = new TableUpdateWorkflow(
        this.metadataCacheService,
        this.queryBuilder,
        this.tableHandlerService,
        this.queryEngine,
        this.routeCacheService,
        this.storageConfigCacheService,
        this.aiConfigCacheService,
        this.systemProtectionService,
        this.tableValidationService,
        this.swaggerService,
        this.graphqlService,
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
        
        return {
          success: true,
          tableName: args.tableName,
          tableId: result?.id || args.tableId,
          updated: updatedFields.length > 0 ? updatedFields.join(', ') : 'table metadata',
          result,
        };
      }

      const errorMessage = workflowResult.stopReason || 'Table update workflow failed';
      this.logger.error(`[ToolExecutor] update_table → FAILED: ${errorMessage}`);

      return {
        error: true,
        errorCode: 'TABLE_UPDATE_FAILED',
        message: errorMessage,
        errors: workflowResult.errors,
        suggestion: 'Check the errors array for details. Ensure table exists and update data is valid.',
      };
    } catch (error: any) {
      this.logger.error(`[ToolExecutor] update_table → EXCEPTION: ${error.message}`);
      return {
        error: true,
        errorCode: 'TABLE_UPDATE_EXCEPTION',
        message: error.message,
        suggestion: 'An unexpected error occurred. Please try again or check table name and update data.',
      };
    }
  }

  private async executeUpdateTask(
    args: {
      conversationId: string | number;
      type: 'create_table' | 'update_table' | 'delete_table' | 'custom';
      status: 'pending' | 'in_progress' | 'completed' | 'cancelled' | 'failed';
      data?: any;
      result?: any;
      error?: string;
      priority?: number;
    },
    context: TDynamicContext,
  ): Promise<any> {
    try {
      const { conversationId, type, status, data, result, error, priority } = args;

      const conversation = await this.conversationService.getConversation({ id: conversationId });
      if (!conversation) {
        return {
          error: true,
          errorCode: 'CONVERSATION_NOT_FOUND',
          message: `Conversation with ID ${conversationId} not found`,
        };
      }

      const now = new Date();
      const existingTask = conversation.task;

      let task: any;
      if (existingTask && existingTask.status !== 'completed' && existingTask.status !== 'failed' && existingTask.status !== 'cancelled') {
        task = {
          ...existingTask,
          type,
          status,
          priority: priority !== undefined ? priority : existingTask.priority || 0,
          updatedAt: now,
        };
        if (data !== undefined) task.data = data;
        if (result !== undefined) task.result = result;
        if (error !== undefined) task.error = error;
      } else {
        task = {
          type,
          status,
          priority: priority || 0,
          createdAt: now,
          updatedAt: now,
        };
        if (data !== undefined) task.data = data;
        if (result !== undefined) task.result = result;
        if (error !== undefined) task.error = error;
      }

      await this.conversationService.updateConversation({
        id: conversationId,
        data: { task },
      });

      return {
        success: true,
        task,
      };
    } catch (error: any) {
      this.logger.error(`[ToolExecutor] update_task → EXCEPTION: ${error.message}`);
      return {
        error: true,
        errorCode: 'TASK_UPDATE_EXCEPTION',
        message: error.message,
        suggestion: 'An unexpected error occurred while updating task.',
      };
    }
  }

  private async executeGetHint(args: { category?: string | string[] }, context: TDynamicContext): Promise<any> {
    const dbType = this.queryBuilder.getDbType();
    const isMongoDB = dbType === 'mongodb';
    const idFieldName = isMongoDB ? '_id' : 'id';

    const allHints = [];

    const dbTypeContent = `Database context:
- Engine: ${dbType}
- ID field: ${isMongoDB ? '"_id"' : '"id"'}
- New table ID type → ${isMongoDB ? '"uuid"' : '"int" (auto increment) hoặc "uuid"'}
- Relation payload → {${isMongoDB ? '"_id"' : '"id"'}: value}`;

    const dbTypeHint = {
      category: 'database_type',
      title: 'Database Type Information',
      content: dbTypeContent,
    };

    const fieldOptContent = `Field & limit checklist:
- Call get_fields or get_table_details before querying
- get_table_details supports single table (string) or multiple tables (array): {"tableName": "post"} or {"tableName": ["post", "category", "user_definition"]}
- When comparing multiple tables or need schemas for multiple tables, use array format to get all in one call: {"tableName": ["table1", "table2", "table3"]}
- Count queries: fields="${idFieldName}", limit=1, meta="totalCount"
- Name lists: fields="${idFieldName},name", pick limit as needed
- Use limit=0 only when you truly need every row (default limit is 10)
- CRITICAL: For create/update operations, ALWAYS specify minimal fields parameter (e.g., "fields": "${idFieldName}" or "fields": "${idFieldName},name") to save tokens. This is MANDATORY - do NOT omit fields parameter in create/update calls.
- Read operations: Specify only needed fields (e.g., "id,name" for lists, "id" for counts). Supports wildcards like "columns.*", "relations.*".
- Write operations: Always specify minimal fields (e.g., "id" or "id,name") to save tokens. Do NOT use "*" or omit fields parameter.

CRITICAL - Schema Check Before Create/Update:
- BEFORE creating or updating records, you MUST call get_table_details to get the full schema
- Check which columns are required (isNullable=false) and have default values
- Ensure your data object includes ALL required fields (not-null constraints)
- Common required fields: id (auto-generated), createdAt/updatedAt (auto-generated), but ALWAYS check for others like slug, stock, order_number, unit_price, etc.
- If you get constraint errors, you MUST call get_table_details to see all required fields and fix your data

Workflow for create/update:
1. 🚨 MANDATORY FIRST STEP: Call check_permission(table="X", operation="create/update") for business tables - DO NOT SKIP THIS
2. Call get_table_details with tableName to get schema (required fields, types, defaults, relations)
3. For relations: Use propertyName from result.relations[] (e.g., "category", "customer"), NOT FK columns (e.g., "category_id", "customerId")
   - Format: {"category": {"id": 19}} OR {"category": 19}
   - NEVER use FK column names - system auto-generates them from propertyName
4. Prepare data object with ALL required fields
5. Call dynamic_repository with create/update operation

Nested relations & query optimization:
- fields → use "relation.field" or "relation.*" (multi-level like "routePermissions.role.name")
- where → nest objects {"roles":{"name":{"_eq":"Admin"}}}
- Prefer one nested query instead of multiple separate calls
- Select only the fields you need (avoid broad "*")

Sample nested query:
{"table":"route_definition","operation":"find","fields":"id,path,roles.name","where":{"roles":{"name":{"_eq":"Admin"}}}}`;

    const fieldOptHint = {
      category: 'field_optimization',
      title: 'Field & Query Optimization',
      content: fieldOptContent,
    };

    const tableOpsContent = `Table operations - use tools for automatic validation & error handling:

Creating tables:
- Use create_table tool (automatically checks existence, validates, handles errors)
- Check if table exists first: {"table":"table_definition","operation":"find","where":{"name":{"_eq":"table_name"}},"fields":"${idFieldName},name","limit":1}
- CRITICAL: Every table MUST have "${idFieldName}" column with isPrimary=true, type="int" (SQL) or "uuid" (MongoDB)
- CRITICAL: NEVER include createdAt/updatedAt in columns - system auto-generates them
- Include ALL columns in one create call (excluding createdAt/updatedAt)

Updating tables:
- Use update_table tool (automatically loads current data, validates, merges, checks FK conflicts)
- Columns merged by name, relations merged by propertyName
- System columns (id, createdAt, updatedAt) automatically preserved

Relations:
- Use update_table tool to add relations (recommended - handles everything automatically)
- Find target table ID first, then: {"tableName": "post", "relations": [{"propertyName": "categories", "type": "many-to-many", "targetTable": {"id": <REAL_ID>}, "inversePropertyName": "posts"}]}
- Create on ONE side only - system handles inverse automatically
- O2M and M2M MUST include inversePropertyName
- targetTable.id MUST be REAL ID from find result (never use IDs from history)

Batch operations:
- Metadata tables (table_definition): Process sequentially, NO batch operations. CRITICAL: When deleting tables, delete ONE BY ONE sequentially (not batch_delete) to avoid deadlocks
- Data tables: Use batch_delete for 2+ deletes, batch_create/batch_update for 5+ creates/updates
- When find returns multiple records, collect ALL IDs and use batch operations (except table deletion - must be sequential)

Best practices:
- Use get_metadata to discover table names
- Schema changes target *_definition tables only
- Use _in filter to find multiple tables in one call
- Always specify minimal fields parameter to save tokens`;

    const tableOpsHint = {
      category: 'table_operations',
      title: 'Table Creation & Management',
      content: tableOpsContent,
    };

    const complexWorkflowsContent = `Complex workflows - use tools for automatic handling:

Recreate tables with relations:
1. Find existing tables: {"table":"table_definition","operation":"find","where":{"name":{"_in":["post","category"]}},"fields":"${idFieldName},name","limit":0}
2. Check permissions, then delete ONE BY ONE sequentially (not batch_delete) to avoid deadlocks: {"table":"table_definition","operation":"delete","id":<id1>}, then {"table":"table_definition","operation":"delete","id":<id2>}, etc.
3. Use create_table tool to create new tables (validates automatically)
4. Find new table IDs, then use update_table tool to add relations (merges automatically)

Common mistakes:
❌ Creating tables without id column
❌ Including createdAt/updatedAt in columns
❌ Updating both sides of relation
❌ Multiple find calls instead of _in filter
❌ Not using batch operations for multiple deletes

Efficiency:
✅ Use _in filter for multiple tables
✅ Use create_table/update_table tools (automatic validation)
✅ Use batch operations for data tables (not metadata tables)`;

    const complexWorkflowsHint = {
      category: 'complex_workflows',
      title: 'Complex Task Workflows',
      content: complexWorkflowsContent,
    };

    const errorContent = `CRITICAL - Sequential Execution (PREVENTS ERRORS):
- ALWAYS execute tools ONE AT A TIME, step by step
- Do NOT call multiple tools simultaneously in a single response
- Execute first tool → wait for result → analyze → proceed to next
- If you call multiple tools at once and one fails, you'll have to retry all, causing duplicates and wasted tokens
- Example workflow: check_permission → wait → dynamic_repository find → wait → dynamic_repository delete → wait → continue
- This prevents errors, duplicate operations, and ensures proper error handling

Error handling:
- If tool returns error=true → stop workflow and report error to user
- Tools have automatic retry logic - let them handle retries
- Report exact error message from tool result to user
- If you encounter errors after calling multiple tools at once, execute them sequentially instead`;

    const errorHint = {
      category: 'error_handling',
      title: 'Error Handling Protocol',
      content: errorContent,
    };

    const discoveryContent = `Table discovery:
- Never guess table names from user phrasing
- Use get_metadata to list tables and pick the closest match
- Need structure? call get_table_details
- Need multiple table structures? Use get_table_details with array: {"tableName": ["table1", "table2"]}

Examples:
- "route" → get_metadata → choose "route_definition"
- "users" → get_metadata → choose "user_definition"
- Need schemas for post, category, and user → get_table_details with {"tableName": ["post", "category", "user_definition"]}`;

    const discoveryHint = {
      category: 'table_discovery',
      title: 'Table Discovery Rules',
      content: discoveryContent,
    };

    const permissionContent = `Permission checks for business tables:

Required workflow:
1. Call check_permission FIRST: {"table":"product","operation":"create"}
2. Wait for result: {"allowed":true,"reason":"..."}
3. If allowed=true → proceed with dynamic_repository
4. If allowed=false → STOP, inform user

Example - Creating a product:
Step 1: check_permission({"table":"product","operation":"create"})
Step 2: Wait for result → {"allowed":true}
Step 3: dynamic_repository({"table":"product","operation":"create","data":{"name":"Product 1","price":100}})

Example - Reading orders:
Step 1: check_permission({"table":"order","operation":"read"})
Step 2: Wait for result → {"allowed":true}
Step 3: dynamic_repository({"table":"order","operation":"find","fields":"id,total","limit":10})

Example - Updating customer:
Step 1: check_permission({"table":"customer","operation":"update"})
Step 2: Wait for result → {"allowed":true}
Step 3: dynamic_repository({"table":"customer","operation":"update","where":{"id":{"_eq":1}},"data":{"name":"New Name"}})

Business tables = any table that is NOT a *_definition table (e.g., "post", "user", "order", "product", "customer", "category")

Metadata tables exception:
- Metadata tables (*_definition) can skip: dynamic_repository({"table":"table_definition","operation":"find","skipPermissionCheck":true})

Reuse results:
- Call check_permission ONLY ONCE per table+operation combination
- After calling, REUSE the result for all subsequent operations on the same table+operation

check_permission automatically handles:
- User lookup (from context)
- Route lookup (if routePath provided)
- Role matching (if roles configured)
- Returns: {allowed: boolean, reason: string, userInfo: object, routeInfo: object}

Tool executor validation:
- Tool executor automatically validates permission for business tables
- If permission check not found → warning logged (but operation may proceed)
- If permission denied → operation fails immediately with error message
- Metadata tables with skipPermissionCheck=true bypass validation`;

    const permissionHint = {
      category: 'permission_check',
      title: 'Permission & Route Access Control',
      content: permissionContent,
    };

    allHints.push(dbTypeHint, fieldOptHint, tableOpsHint, errorHint, discoveryHint, permissionHint, complexWorkflowsHint);

    let filteredHints = allHints;
    if (args.category) {
      const categories = Array.isArray(args.category) ? args.category : [args.category];
      filteredHints = allHints.filter(h => categories.includes(h.category));
    }

    return {
      dbType,
      isMongoDB,
      idField: idFieldName,
      hints: filteredHints,
      count: filteredHints.length,
      availableCategories: ['database_type', 'field_optimization', 'table_operations', 'error_handling', 'table_discovery', 'permission_check', 'complex_workflows'],
    };
  }

  private async executeDynamicRepository(
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
    abortSignal?: AbortSignal,
  ): Promise<any> {
    if (abortSignal?.aborted) {
      return {
        error: true,
        errorCode: 'REQUEST_ABORTED',
        message: 'Request aborted by client',
      };
    }
    if (args.operation === 'findOne') {
      args.operation = 'find' as any;
      if (!args.limit || args.limit > 1) {
        args.limit = 1;
      }
    }

    const metaRaw = args.meta;
    const humanConfirmed = typeof metaRaw === 'string' && metaRaw.toLowerCase().includes('confirm');

    if (typeof args.meta === 'string') {
      try {
        const parsed = JSON.parse(args.meta);
        if (parsed && typeof parsed === 'object') {
          if (parsed.columns && !Array.isArray(parsed.columns)) {
            parsed.columns = [parsed.columns];
          }
          if (parsed.relations && !Array.isArray(parsed.relations)) {
            parsed.relations = [parsed.relations];
          }
          args.meta = JSON.stringify(parsed);
        }
      } catch (_) {}
    }

    if (args.table === 'table_definition' && args.data) {
      if (args.data.columns && !Array.isArray(args.data.columns)) {
        args.data.columns = [args.data.columns];
      }
      if (args.data.relations && !Array.isArray(args.data.relations)) {
        args.data.relations = [args.data.relations];
      }
    }

    const repo = new DynamicRepository({
      context,
      tableName: args.table,
      queryBuilder: this.queryBuilder,
      tableHandlerService: this.tableHandlerService,
      queryEngine: this.queryEngine,
      routeCacheService: this.routeCacheService,
      storageConfigCacheService: this.storageConfigCacheService,
      aiConfigCacheService: this.aiConfigCacheService,
      metadataCacheService: this.metadataCacheService,
      systemProtectionService: this.systemProtectionService,
      tableValidationService: this.tableValidationService,
      bootstrapScriptService: undefined,
      redisPubSubService: undefined,
      swaggerService: this.swaggerService,
      graphqlService: this.graphqlService,
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
      const operation = args.operation === 'find' ? 'read' : args.operation === 'batch_create' ? 'create' : args.operation === 'batch_update' ? 'update' : args.operation === 'batch_delete' ? 'delete' : args.operation;
      const cacheKey = `${userId || 'anon'}|${operation}|${args.table || ''}|`;

      if (!permissionCache.has(cacheKey)) {
        const isMetadataTable = args.table?.endsWith('_definition');
        if (!isMetadataTable) {
          return {
            error: true,
            errorCode: 'MISSING_PERMISSION_CHECK',
            message: `Permission check required but not found for ${operation} operation on ${args.table}. You MUST call check_permission first before performing any operation on business tables.`,
            userMessage: `❌ **Permission Check Required**: You must call check_permission before performing ${operation} operation on table "${args.table}".\n\n📋 **Action Required**:\n1. Call check_permission with table="${args.table}" and operation="${operation}" first\n2. Verify that allowed=true in the result\n3. Only then proceed with the ${operation} operation\n\n💡 **Note**: Permission check is MANDATORY for all business tables (non-metadata tables). Metadata tables (*_definition) may skip with skipPermissionCheck=true.`,
            suggestion: `Call check_permission with table="${args.table}" and operation="${operation}" first to verify access before performing this operation.`,
          };
        } else {
          this.logger.warn(
            `[ToolExecutor] Permission check not found for ${operation} on ${args.table}. AI should have called check_permission first. Proceeding with operation, but this may fail if permission is denied.`,
          );
        }
      } else {
        const permissionResult = permissionCache.get(cacheKey);
        if (!permissionResult?.allowed) {
          return {
            error: true,
            errorCode: 'PERMISSION_DENIED',
            message: `Permission denied for ${operation} operation on ${args.table}. Reason: ${permissionResult?.reason || 'unknown'}. You must call check_permission first and ensure allowed=true before performing this operation.`,
            userMessage: `❌ **Permission Denied**: You do not have permission to perform ${operation} operation on table "${args.table}".\n\n📋 **Reason**: ${permissionResult?.reason || 'unknown'}\n\n💡 **Note**: This operation cannot proceed. Please check your access rights or contact an administrator.`,
            suggestion: `Call check_permission with table="${args.table}" and operation="${operation}" first to verify access.`,
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
          userMessage: `❌ **Permission Denied**: Operation ${args.operation} on table "${args.table}" was denied.\n\n📋 **Error**: ${errorMessage}\n\n💡 **Action Required**:\n1. Call check_permission with table="${args.table}" and operation="${args.operation}" first\n2. Verify that allowed=true in the result\n3. If permission is denied, you cannot proceed with this operation${isMetadataTable ? '\n4. For metadata tables, you may use skipPermissionCheck=true if you have system access' : ''}\n\n⚠️ **Note**: Permission check is MANDATORY for business tables. Always call check_permission before performing operations.`,
          suggestion: `Call check_permission with table="${args.table}" and operation="${args.operation}" first to verify access. If permission is denied, you cannot proceed with this operation.`,
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
        humanConfirmed,
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
}

