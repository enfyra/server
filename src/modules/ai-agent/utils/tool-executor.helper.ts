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

    const permissionCache: Map<string, any> =
      ((context as any).__permissionCache as Map<string, any>) ||
      (((context as any).__permissionCache = new Map<string, any>()) as Map<string, any>);
    const cacheKey = `${userId || 'anon'}|${operation}|${table || ''}|${routePath || ''}`;

    if (permissionCache.has(cacheKey)) {
      return permissionCache.get(cacheKey);
    }

    const setCache = (result: any) => {
      const finalResult = { ...result, cacheKey };
      permissionCache.set(cacheKey, finalResult);
      return finalResult;
    };

    console.log(`[check_permission] Called with table=${table}, operation=${operation}, userId=${userId}`);

    if (!userId) {
      console.warn(`[check_permission] ❌ No userId in context`);
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

    console.log(`[check_permission] User query result:`, JSON.stringify(user, null, 2));
    console.log(`[check_permission] isRootAdmin value: ${user.isRootAdmin} (type: ${typeof user.isRootAdmin})`);

    if (user.isRootAdmin === true) {
      console.log(`[check_permission] ✅ ALLOWED: User is root admin`);
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

    console.log(`[check_permission] ❌ DENIED: No matching permissions found`);
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
- CRITICAL: targetTable must use REAL ID from database, NEVER use IDs from history or previous operations
- Before creating relation, ALWAYS find table_definition by name to get the current ID
- targetTable format: {"${idFieldName}": <REAL_ID_FROM_FIND>}
- Workflow: 1) Find source table ID by name, 2) Find target table ID by name, 3) Verify both exist, 4) Create relation with REAL IDs
- M2O / O2O: {"category":{"${idFieldName}":<REAL_ID>}}, {"profile":{"${idFieldName}":<REAL_ID>}}
- M2M: {"tags":[{"${idFieldName}":<REAL_ID_1>},{"${idFieldName}":<REAL_ID_2>}]}
- O2M: {"items":[{"${idFieldName}":10,"qty":5}, {...newItem}]}
- Cascade: Use cascade option when you want automatic deletion/update of related records (e.g., {"propertyName":"orderItems","type":"one-to-many","targetTable":{"${idFieldName}":X},"inversePropertyName":"order","cascade":true})`;

    const relationHint = {
      category: 'relations',
      title: 'Relation Behavior',
      content: relationContent,
    };

    const metadataContent = `Metadata rules:
- createdAt/updatedAt are auto-generated → do not declare them when creating tables
- Foreign keys are indexed automatically
- Table names usually end with "_definition"
- Schema changes (columns, relations, indexes) belong to table_definition / column_definition / relation_definition only
- Update business data rows via the actual tables (post, order, etc.); do NOT touch them when editing structure
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
- Use limit=0 only when you truly need every row (default limit is 10)
- CRITICAL: For create/update operations, ALWAYS specify minimal fields parameter (e.g., "fields": "${idFieldName}" or "fields": "${idFieldName},name") to save tokens. This is MANDATORY - do NOT omit fields parameter in create/update calls.`;

    const fieldOptHint = {
      category: 'field_optimization',
      title: 'Field & Query Optimization',
      content: fieldOptContent,
    };

    const tableOpsContent = `Table creation rules:
- CRITICAL: Before creating a table, ALWAYS check if it already exists by finding table_definition by name first. If table exists, skip creation or inform user.
- Check existence: {"table":"table_definition","operation":"find","where":{"name":{"_eq":"table_name"}},"fields":"${idFieldName},name","limit":1}
- If find returns data → table exists, do NOT create again. If find returns empty → table does not exist, proceed with creation.
- Every table MUST have "${idFieldName}" column with isPrimary=true, isGenerated=true
- SQL: use type="int" for auto-increment ID, or "uuid" for UUID
- MongoDB: ONLY use type="uuid" for ID
- CRITICAL: createdAt/updatedAt are AUTO-GENERATED by system → NEVER include them in data.columns array. System automatically adds these columns to every table. If you include them, you will get "column specified more than once" error.
- Include ALL columns in ONE create call (including id column, but EXCLUDING createdAt/updatedAt)

Example - Creating a table (with existence check):
Step 1: Check if table exists:
{"table":"table_definition","operation":"find","where":{"name":{"_eq":"product"}},"fields":"${idFieldName},name","limit":1}

Step 2: If find returns empty (table does not exist), create table:
{
  "table": "table_definition",
  "operation": "create",
  "data": {
    "name": "product",
    "description": "Products",
    "columns": [
      {"name": "${idFieldName}", "type": "int", "isPrimary": true, "isGenerated": true},
      {"name": "name", "type": "varchar", "isNullable": false},
      {"name": "price", "type": "decimal", "isNullable": true}
    ]
  },
  "fields": "${idFieldName},name"
}
CRITICAL: Do NOT include createdAt or updatedAt in columns array - system automatically adds them to every table. If you include them, you will get "column specified more than once" error.
If find returns data → table already exists, skip creation and inform user.

CRITICAL: Always include "fields" parameter in create/update operations (e.g., "fields": "${idFieldName}" or "fields": "${idFieldName},name") to save tokens. Do NOT omit this parameter.

Relation rules:
- CRITICAL: Create relation on ONLY ONE SIDE (source table). NEVER create relation on both sides - this causes duplicate FK column errors.
- Workflow: 1) Create tables WITHOUT relations first, 2) Find source table ID by name, 3) Find target table ID by name, 4) Verify both IDs exist, 5) Update EXACTLY ONE table_definition (source table) with relations array
- CRITICAL: targetTable.id MUST be REAL ID from database. ALWAYS find table_definition by name first to get current ID. NEVER use IDs from history or previous operations.
- One-to-many (O2M): MUST include inversePropertyName. Create relation on source table (e.g., customer has orders → update customer table with {"propertyName":"orders","type":"one-to-many","targetTable":{"${idFieldName}":<order_table_id>},"inversePropertyName":"customer"}). System automatically creates FK column "customerId" in order table. DO NOT update order table separately.
- Many-to-one (M2O): Create relation on source table (e.g., product has category → update product table with {"propertyName":"category","type":"many-to-one","targetTable":{"${idFieldName}":<category_table_id>}}). System automatically creates FK column "categoryId" in product table. DO NOT update category table separately.
- One-to-one (O2O): inversePropertyName is optional. Create relation on ONE side only (e.g., user has profile → update user table with {"propertyName":"profile","type":"one-to-one","targetTable":{"${idFieldName}":<profile_table_id>}}). System automatically creates FK column. DO NOT update profile table separately.
- Many-to-many (M2M): MUST include inversePropertyName. Create relation on ONE side only (e.g., post has categories → update post table with {"propertyName":"categories","type":"many-to-many","targetTable":{"${idFieldName}":<category_table_id>},"inversePropertyName":"posts"}). System automatically creates junction table and inverse relation. DO NOT update category table separately.
- Cascade: Use cascade option to automatically delete/update related records when parent is deleted/updated (recommended for O2M and O2O)
- Update EXACTLY ONE table_definition (source table) with data.relations array (merge existing relations, add new one)
- System automatically handles: inverse relation, FK column creation, junction table (M2M). You only need to update ONE table.

Table operations (table_definition):
- CRITICAL: When creating/updating tables, do NOT use batch operations. Process each table sequentially (one create/update at a time).
- Batch operations are ONLY for data tables (post, category, etc.), NOT for table_definition.
- Example: Create 3 tables → 3 separate create calls, NOT batch_create

Example - Adding relation (workflow for ANY relation type):
Step 1: Find SOURCE table ID by name (ALWAYS do this first):
{"table":"table_definition","operation":"find","where":{"name":{"_eq":"post"}},"fields":"${idFieldName},name","limit":1}

Step 2: Find TARGET table ID by name (ALWAYS do this second):
{"table":"table_definition","operation":"find","where":{"name":{"_eq":"category"}},"fields":"${idFieldName},name","limit":1}

Step 3: Verify both IDs exist from Step 1 and Step 2 results. If either is missing, STOP and report error.

Step 4: Fetch current columns and relations from source table (CRITICAL - to check for conflicts and merge correctly):
{"table":"table_definition","operation":"find","where":{"name":{"_eq":"post"}},"fields":"${idFieldName},columns.*,relations.*","limit":1}

Step 5: Check for FK column conflicts (CRITICAL):
- System generates FK column name from propertyName using camelCase: propertyName="order" → FK column="orderId", propertyName="user" → FK column="userId", propertyName="customer" → FK column="customerId"
- CRITICAL: Check existing columns in Step 4 result. If table already has column "user_id", "order_id", "customer_id", "product_id" (snake_case) OR "userId", "orderId", "customerId", "productId" (camelCase), you MUST use a different propertyName to avoid conflict
- Example: If table has "customer_id" or "customerId" column, use propertyName="buyer" instead of "customer", or use propertyName="owner" instead of "user"
- If conflict exists, STOP and report error to user - do NOT proceed with relation creation

Step 6: Merge new relation with existing relations (preserve ALL existing relations, especially system relations):
- Get existing relations from Step 4 result
- Add new relation to the array
- NEVER replace the entire relations array - always merge
- For system tables (user_definition, etc.), preserve ALL existing system relations

Step 7: Update source table with merged relations:
{"table":"table_definition","operation":"update","id":"<REAL_POST_ID_FROM_STEP1>","data":{"relations":[{...ALL existing relations from Step 4...},{"propertyName":"categories","type":"many-to-many","targetTable":{"${idFieldName}":"<REAL_CATEGORY_ID_FROM_STEP2>"},"inversePropertyName":"posts"}]},"fields":"${idFieldName}"}

CRITICAL WORKFLOW:
1. ALWAYS find source table ID by name FIRST
2. ALWAYS find target table ID by name SECOND
3. ALWAYS verify both IDs exist before creating relation
4. ALWAYS fetch current columns and relations from source table to check for conflicts
5. CRITICAL: Check FK column conflict - system generates FK column from propertyName (e.g., propertyName="user" → FK column="userId"). If table already has "user_id" or similar column, use different propertyName or check if existing column can be used.
6. NEVER use IDs from conversation history or previous operations
7. NEVER use placeholder IDs like "X" or "new_table_id" - must use REAL IDs from find results
8. ALWAYS merge relations - preserve ALL existing relations (especially system relations), never replace entire array
9. For system tables (user_definition, etc.), preserve ALL existing system relations - only add new ones
10. For O2M: include inversePropertyName and optionally cascade. Create relation on source table ONLY (e.g., customer → orders, update customer table).
11. For M2M: include inversePropertyName, system handles inverse automatically. Create relation on ONE side ONLY (e.g., post → categories, update post table only).
12. CRITICAL: Update ONLY the source table. NEVER update both source and target tables - this causes duplicate FK column errors. System automatically handles inverse relation, FK column creation, and junction table.
13. If you need bidirectional relation (e.g., customer ↔ orders), create relation on ONE side only with inversePropertyName. System handles the inverse automatically.

Batch operations (≥5 records):
- batch_create with dataArray: [...]
- batch_update with updates: [{id, data}, ...]
- batch_delete with ids: [...]`;

    const tableOpsHint = {
      category: 'table_operations',
      title: 'Table Creation & Management',
      content: tableOpsContent,
    };

    const complexWorkflowsContent = `Workflow: Recreate tables with M2M relation

Goal: Delete existing post & category tables, recreate with M2M relation

Step-by-step (8-10 tool calls total):

1. Ask user confirmation & outline plan FIRST

2. Find existing table metadata (ONE query):
   {"table":"table_definition","operation":"find","where":{"name":{"_in":["post","category"]}},"fields":"${idFieldName},name","limit":0}

3. Permission checks (check once per unique table):
   {"table":"post","operation":"delete"} → check_permission
   {"table":"category","operation":"delete"} → check_permission

4. Delete table metadata (ONE batch_delete call):
   {"table":"table_definition","operation":"batch_delete","ids":["<post_table_${idFieldName}>","<category_table_${idFieldName}>"]}

5. Create new tables (2 calls, include ALL columns + id, ALWAYS include fields parameter):
   Post table:
   {"table":"table_definition","operation":"create","data":{"name":"post","description":"Blog posts","columns":[{"name":"${idFieldName}","type":"int","isPrimary":true,"isGenerated":true},{"name":"title","type":"varchar","isNullable":false},{"name":"content","type":"text","isNullable":true}]},"fields":"${idFieldName},name"}

   Category table:
   {"table":"table_definition","operation":"create","data":{"name":"category","description":"Categories","columns":[{"name":"${idFieldName}","type":"int","isPrimary":true,"isGenerated":true},{"name":"name","type":"varchar","isNullable":false},{"name":"description","type":"text","isNullable":true}]},"fields":"${idFieldName},name"}

6. Fetch newly created table IDs (ONE query):
   {"table":"table_definition","operation":"find","where":{"name":{"_in":["post","category"]}},"fields":"${idFieldName},name","limit":0}

7. Create M2M relation (ONE call, update post side only, ALWAYS include fields parameter):
   {"table":"table_definition","operation":"update","id":"<new_post_${idFieldName}>","data":{"relations":[{"propertyName":"categories","type":"many-to-many","targetTable":{"${idFieldName}":"<new_category_${idFieldName}>"},"inversePropertyName":"posts"}]},"fields":"${idFieldName}"}

8. Remind user to reload Admin UI

Common mistakes to AVOID:
❌ Calling get_table_details on data tables (post, category) for metadata work
❌ Creating tables without id column
❌ Defining createdAt/updatedAt manually
❌ Updating both sides of M2M relation
❌ Using findOne operation (use find with limit=1)
❌ Multiple find calls instead of using _in filter
❌ Not including all columns in table create
❌ Deleting multiple tables one by one instead of using batch_delete with ids array
❌ Not collecting all IDs before batch_delete (must find first, then batch_delete)

Efficiency rules:
✅ Use _in filter to find multiple tables in ONE call
✅ Include complete data in create/update (avoid multiple calls)
✅ Only operate on *_definition tables for metadata work
✅ Never scan data tables when working with metadata`;

    const complexWorkflowsHint = {
      category: 'complex_workflows',
      title: 'Complex Task Workflows',
      content: complexWorkflowsContent,
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

    allHints.push(dbTypeHint, relationHint, metadataHint, fieldOptHint, tableOpsHint, errorHint, discoveryHint, nestedHint, routeAccessHint, permissionHint, complexWorkflowsHint);

    const filteredHints = args.category
      ? allHints.filter(h => h.category === args.category)
      : allHints;

    return {
      dbType,
      isMongoDB,
      idField: idFieldName,
      hints: filteredHints,
      count: filteredHints.length,
      availableCategories: ['database_type', 'relations', 'metadata', 'field_optimization', 'table_operations', 'error_handling', 'table_discovery', 'nested_relations', 'route_access', 'permission_check', 'complex_workflows'],
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
    this.logger.log(`[ToolExecutor] dynamic_repository call → ${JSON.stringify(preview)}`);

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
          this.logger.log(`[ToolExecutor] dynamic_repository CREATE → table=${args.table}, fields=${args.fields || 'NOT SPECIFIED'}, dataKeys=${Object.keys(args.data || {}).join(',')}`);
          console.log('[Tool Executor - dynamic_repository] Create operation:', {
            table: args.table,
            fields: args.fields || 'NOT SPECIFIED ⚠️',
            data: args.data,
          });
          return await repo.create({ data: args.data, fields: args.fields });
        case 'update':
          if (!args.id) {
            throw new Error('id is required for update operation');
          }
          if (!args.data) {
            throw new Error('data is required for update operation');
          }
          this.logger.log(`[ToolExecutor] dynamic_repository UPDATE → table=${args.table}, id=${args.id}, fields=${args.fields || 'NOT SPECIFIED'}, dataKeys=${Object.keys(args.data || {}).join(',')}`);
          console.log('[Tool Executor - dynamic_repository] Update operation:', {
            table: args.table,
            id: args.id,
            fields: args.fields || 'NOT SPECIFIED ⚠️',
            data: args.data,
          });
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
          this.logger.log(`[ToolExecutor] dynamic_repository BATCH_CREATE → table=${args.table}, count=${args.dataArray.length}, fields=${args.fields || 'NOT SPECIFIED'}`);
          console.log('[Tool Executor - dynamic_repository] Batch create operation:', {
            table: args.table,
            count: args.dataArray.length,
            fields: args.fields || 'NOT SPECIFIED ⚠️',
            sampleData: args.dataArray.slice(0, 2),
          });
          return Promise.all(
            args.dataArray.map(data => repo.create({ data, fields: args.fields }))
          );
        case 'batch_update':
          if (!args.updates || !Array.isArray(args.updates)) {
            throw new Error('updates (array of {id, data}) is required for batch_update operation');
          }
          this.logger.log(`[ToolExecutor] dynamic_repository BATCH_UPDATE → table=${args.table}, count=${args.updates.length}, fields=${args.fields || 'NOT SPECIFIED'}`);
          console.log('[Tool Executor - dynamic_repository] Batch update operation:', {
            table: args.table,
            count: args.updates.length,
            fields: args.fields || 'NOT SPECIFIED ⚠️',
            sampleUpdates: args.updates.slice(0, 2),
          });
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

