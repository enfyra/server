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

export class ToolExecutor {
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
  ): Promise<any> {
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
        return await this.executeGetTableDetails(args);
      case 'get_fields':
        return await this.executeGetFields(args);
      case 'get_hint':
        return await this.executeGetHint(args, context);
      case 'dynamic_repository':
        return await this.executeDynamicRepository(args, context);
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

    console.log(`[check_permission] Called with table=${table}, operation=${operation}, userId=${userId}`);

    if (!userId) {
      console.warn(`[check_permission] ❌ No userId in context`);
      return {
        allowed: false,
        reason: 'not_authenticated',
        message: 'User is not authenticated. Please login first.',
      };
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
      return {
        allowed: false,
        reason: 'user_not_found',
        message: 'User not found in the system.',
      };
    }

    const user = userResult.data[0];

    console.log(`[check_permission] User query result:`, JSON.stringify(user, null, 2));
    console.log(`[check_permission] isRootAdmin value: ${user.isRootAdmin} (type: ${typeof user.isRootAdmin})`);

    if (user.isRootAdmin === true) {
      console.log(`[check_permission] ✅ ALLOWED: User is root admin`);
      return {
        allowed: true,
        reason: 'root_admin',
        message: 'User is root admin with full access.',
        userInfo: {
          id: user.id,
          email: user.email,
          isRootAdmin: true,
          role: user.role || null,
        },
      };
    }

    let finalRoutePath = routePath;
    if (!finalRoutePath && table) {
      const tableName = table.replace(/_definition$/, '');
      finalRoutePath = `/${tableName}`;
    }

    if (!finalRoutePath) {
      return {
        allowed: false,
        reason: 'no_route_specified',
        message: 'Cannot determine route path. Please provide routePath or table parameter.',
      };
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
        return {
          allowed: true,
          reason: 'route_not_found_public_read',
          message: `Route ${finalRoutePath} not found. Assuming public read access.`,
          userInfo: {
            id: user.id,
            email: user.email,
            isRootAdmin: false,
            role: user.role || null,
          },
        };
      } else {
        return {
          allowed: false,
          reason: 'route_not_found_write_denied',
          message: `Route ${finalRoutePath} not found. Write operations require explicit permissions.`,
          userInfo: {
            id: user.id,
            email: user.email,
            isRootAdmin: false,
            role: user.role || null,
          },
        };
      }
    }

    const route = routeResult.data[0];
    const routePermissions = route.routePermissions || [];

    if (routePermissions.length === 0) {
      return {
        allowed: false,
        reason: 'no_permissions_configured',
        message: `No permissions configured for ${finalRoutePath}.`,
        userInfo: {
          id: user.id,
          email: user.email,
          isRootAdmin: false,
          role: user.role || null,
        },
      };
    }

    for (const permission of routePermissions) {
      const allowedMethods = permission.methods || [];
      const hasMethodAccess = allowedMethods.some((m: any) => m.method === requiredMethod);

      if (!hasMethodAccess) {
        continue;
      }

      const allowedUsers = permission.allowedUsers || [];
      if (allowedUsers.some((u: any) => u?.id === userId)) {
        return {
          allowed: true,
          reason: 'user_specific_access',
          message: `User has direct access to ${operation} on ${finalRoutePath}.`,
          userInfo: {
            id: user.id,
            email: user.email,
            isRootAdmin: false,
            role: user.role || null,
          },
        };
      }

      const allowedRole = permission.role || null;
      if (allowedRole && user.role && allowedRole.id === user.role.id) {
        return {
          allowed: true,
          reason: 'role_based_access',
          message: `User has role-based access to ${operation} on ${finalRoutePath} via role: ${user.role.name || user.role.id}.`,
          userInfo: {
            id: user.id,
            email: user.email,
            isRootAdmin: false,
            role: user.role,
          },
        };
      }
    }

    console.log(`[check_permission] ❌ DENIED: No matching permissions found`);
    return {
      allowed: false,
      reason: 'permission_denied',
      message: `User does not have permission to ${operation} on ${finalRoutePath}.`,
      userInfo: {
        id: user.id,
        email: user.email,
        isRootAdmin: false,
        role: user.role || null,
      },
    };
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

  private async executeGetTableDetails(args: { tableName: string; forceRefresh?: boolean }): Promise<any> {
    if (args.forceRefresh) {
      await this.metadataCacheService.reload();
    }

    const metadata = await this.metadataCacheService.getTableMetadata(args.tableName);
    if (!metadata) {
      throw new Error(`Table ${args.tableName} not found`);
    }

    return optimizeMetadataForLLM(metadata);
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

  private async executeGetHint(args: { category?: string }, context: TDynamicContext): Promise<any> {
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

    const relationContent = `Relation checklist:
- Always use propertyName (never raw FK columns like "userId")
- targetTable must be {"${idFieldName}": value}
- M2O / O2O: {"category":{"${idFieldName}":1}}, {"profile":{...}}
- M2M: {"tags":[{"${idFieldName}":1},{"${idFieldName}":2}]}
- O2M: {"items":[{"${idFieldName}":10,"qty":5}, {...newItem}]}`;

    const relationHint = {
      category: 'relations',
      title: 'Relation Behavior',
      content: relationContent,
    };

    const metadataContent = `Metadata rules:
- createdAt/updatedAt are auto-generated → do not declare them when creating tables
- Foreign keys are indexed automatically
- Table names usually end with "_definition"
- Discover actual names via get_metadata, then choose the closest match`;

    const metadataHint = {
      category: 'metadata',
      title: 'Table Metadata & Auto-fields',
      content: metadataContent,
    };

    const fieldOptContent = `Field & limit checklist:
- Call get_fields or get_table_details before querying
- Count queries: fields="${idFieldName}", limit=1, meta="totalCount"
- Name lists: fields="${idFieldName},name", pick limit as needed
- Use limit=0 only when you truly need every row (default limit is 10)`;

    const fieldOptHint = {
      category: 'field_optimization',
      title: 'Field & Query Optimization',
      content: fieldOptContent,
    };

    const tableOpsContent = `Table operations checklist:
- Every new table must include "${idFieldName}" with isPrimary=true
- SQL IDs: int (auto increment) or uuid; MongoDB IDs: uuid only
- Relations always use targetTable {"${idFieldName}": targetId}
- createdAt/updatedAt are auto-generated → omit them
- Confirm structural changes with the user before executing

M2M flow (post ↔ category):
1. Create the base tables without relations
2. Fetch their ${idFieldName} with dynamic_repository.find
3. Update exactly one table:
{"propertyName":"categories","type":"many-to-many","targetTable":{"${idFieldName}":11},"inversePropertyName":"posts"}

Removing M2M:
- If one table is deleted, update the remaining table and set relations=[] (or include only the relations to keep)
- The system handles inverse + junction cleanup automatically

Batch operations (>4 records):
- batch_create {"table":"product","operation":"batch_create","dataArray":[{...}]}
- batch_update {"table":"product","operation":"batch_update","updates":[{"id":1,"data":{...}}]}
- batch_delete {"table":"product","operation":"batch_delete","ids":[1,2,3]}`

    const tableOpsHint = {
      category: 'table_operations',
      title: 'Table Creation & Management',
      content: tableOpsContent,
    };

    const errorContent = `Error protocol:
- If a tool returns error=true → stop the entire workflow
- Do not call additional tools after the failure
- Report the error message/details back to the user
- Delete operations require ${idFieldName}; fetch the record first if needed`;

    const errorHint = {
      category: 'error_handling',
      title: 'Error Handling Protocol',
      content: errorContent,
    };

    const discoveryContent = `Table discovery:
- Never guess table names from user phrasing
- Use get_metadata to list tables and pick the closest match
- Need structure? call get_table_details

Examples:
- "route" → get_metadata → choose "route_definition"
- "users" → get_metadata → choose "user_definition"`;

    const discoveryHint = {
      category: 'table_discovery',
      title: 'Table Discovery Rules',
      content: discoveryContent,
    };

    const nestedContent = `Nested relations:
- fields → use "relation.field" or "relation.*" (multi-level like "routePermissions.role.name")
- where → nest objects {"roles":{"name":{"_eq":"Admin"}}}
- Prefer one nested query instead of multiple separate calls
- Select only the fields you need (avoid broad "*")

Sample request:
{"table":"route_definition","operation":"find","fields":"id,path,roles.name","where":{"roles":{"name":{"_eq":"Admin"}}}}`;

    const nestedHint = {
      category: 'nested_relations',
      title: 'Nested Relations & Query Optimization',
      content: nestedContent,
    };

    const routeAccessContent = `Route access flow:
1. @Public()/publishedMethods → allow
2. Missing JWT → 401
3. user.isRootAdmin → allow (skip remaining checks)
4. No matching routePermissions → 403
5. For each permission:
   - Request method matches
   - allowedUsers contains the user OR allowedRoles matches a user role

Notes:
- RoleGuard is disabled → authorization happens via routePermissions only
- Use nested fields like "routePermissions.role.name" and "routePermissions.methods.method"
- After changes, POST /admin/reload/routes to refresh the cache`;

    const routeAccessHint = {
      category: 'route_access',
      title: 'Route Access Control Flow',
      content: routeAccessContent,
    };

    const permissionContent = `Permission flow:
1. Fetch current user:
   {"table":"user_definition","operation":"find","where":{"${idFieldName}":{"_eq":"$user.${idFieldName}"}},"fields":"${idFieldName},email,isRootAdmin,roles.${idFieldName}"}
2. If isRootAdmin=true → allow immediately
3. Otherwise fetch route_definition:
   {"table":"route_definition","operation":"find","where":{"path":{"_eq":"/resource-path"}},"fields":"allowedUsers.${idFieldName},allowedRoles.${idFieldName}"}
4. Allow when any condition passes:
   - allowedUsers includes the user
   - allowedRoles overlaps a user role
   - User owns the resource (createdBy.${idFieldName} === user.${idFieldName})
5. If none match → deny and state the reason

Reminders:
- Check permissions for read/create/update/delete (metadata tools are exempt)
- Always return explicit denial messages
- When uncertain, deny (fail-safe)`;

    const permissionHint = {
      category: 'permission_check',
      title: 'Permission & Authorization Checking',
      content: permissionContent,
    };

    allHints.push(dbTypeHint, relationHint, metadataHint, fieldOptHint, tableOpsHint, errorHint, discoveryHint, nestedHint, routeAccessHint, permissionHint);

    const filteredHints = args.category
      ? allHints.filter(h => h.category === args.category)
      : allHints;

    return {
      dbType,
      isMongoDB,
      idField: idFieldName,
      hints: filteredHints,
      count: filteredHints.length,
      availableCategories: ['database_type', 'relations', 'metadata', 'field_optimization', 'table_operations', 'error_handling', 'table_discovery', 'nested_relations', 'route_access', 'permission_check'],
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
    },
    context: TDynamicContext,
  ): Promise<any> {
    if (args.operation === 'findOne') {
      args.operation = 'find' as any;
      if (!args.limit || args.limit > 1) {
        args.limit = 1;
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

    try {
      switch (args.operation) {
        case 'find':
          console.log('[Tool Executor - dynamic_repository] Find operation:', {
            table: args.table,
            where: args.where,
            fields: args.fields,
            limit: args.limit,
            sort: args.sort,
            meta: args.meta,
          });

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
          return await repo.create(args.data);
        case 'update':
          if (!args.id) {
            throw new Error('id is required for update operation');
          }
          if (!args.data) {
            throw new Error('data is required for update operation');
          }
          return await repo.update(args.id, args.data);
        case 'delete':
          if (!args.id) {
            throw new Error('id is required for delete operation');
          }
          return await repo.delete(args.id);
        case 'batch_create':
          if (!args.dataArray || !Array.isArray(args.dataArray)) {
            throw new Error('dataArray (array) is required for batch_create operation');
          }
          return Promise.all(
            args.dataArray.map(data => repo.create(data))
          );
        case 'batch_update':
          if (!args.updates || !Array.isArray(args.updates)) {
            throw new Error('updates (array of {id, data}) is required for batch_update operation');
          }
          return Promise.all(
            args.updates.map(update => repo.update(update.id, update.data))
          );
        case 'batch_delete':
          if (!args.ids || !Array.isArray(args.ids)) {
            throw new Error('ids (array) is required for batch_delete operation');
          }
          return Promise.all(
            args.ids.map(id => repo.delete(id))
          );
        default:
          throw new Error(`Unknown operation: ${args.operation}`);
      }
    } catch (error: any) {
      const errorMessage = error?.message || error?.response?.message || String(error);
      const recovery = getRecoveryStrategy(error);
      const details = error?.details || error?.response?.details || {};

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
}

