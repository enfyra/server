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
  classifyError,
  formatErrorForUser,
  shouldEscalateToHuman,
  formatEscalationMessage,
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
      case 'get_metadata':
        return await this.executeGetMetadata(args);
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

    // Map CRUD operations to HTTP methods
    const operationToMethod: Record<string, string> = {
      read: 'GET',
      create: 'POST',
      update: 'PATCH',
      delete: 'DELETE',
    };
    const requiredMethod = operationToMethod[operation];

    // 1. Get user info with role (singular, matching RoleGuard)
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

    // 2. Check if root admin - full access (matching RoleGuard line 30)
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

    // 3. Determine route path
    let finalRoutePath = routePath;
    if (!finalRoutePath && table) {
      // Infer route path from table name
      // Remove _definition suffix if exists
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

    // 4. Get route with permissions (matching RoleGuard structure)
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
      // Route not found - assume public access for read, deny for write
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

    // No permissions configured - deny access
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

    // 5. Check permissions (matching RoleGuard logic lines 34-48)
    for (const permission of routePermissions) {
      // First check if this permission covers the required HTTP method
      const methods = permission.methods || [];
      const hasMethodAccess = methods.some((m: any) => m.method === requiredMethod);

      if (!hasMethodAccess) {
        continue; // Skip this permission if method not allowed
      }

      // Check user-specific access first (matching RoleGuard line 41)
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

      // Then check role-based access (matching RoleGuard line 46)
      // Note: user has single 'role', permission has single 'role'
      const permissionRole = permission.role;
      if (permissionRole && user.role && permissionRole.id === user.role.id) {
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

    // 6. No permission found - deny
    console.warn(`[check_permission] ❌ DENIED: No permission found for ${operation} on ${finalRoutePath}`);
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

  private async executeGetMetadata(args: { forceRefresh?: boolean }): Promise<any> {
    if (args.forceRefresh) {
      await this.metadataCacheService.reload();
    }

    const metadata = await this.metadataCacheService.getMetadata();
    
    const tablesSummary = Array.from(metadata.tables.entries()).map(([name, table]) => ({
      name,
      description: table.description || '',
      isSingleRecord: table.isSingleRecord || false,
    }));
    
    return {
      tables: tablesSummary,
      tablesList: metadata.tablesList,
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

    // Extract only field names from metadata
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

    // 1. Database Type Hint
    let dbTypeContent = `Current database type: ${dbType}\n\n`;
    if (isMongoDB) {
      dbTypeContent += `**MongoDB:**
- Primary key: "_id" (not "id")
- ID type when creating tables: MUST use "uuid" (NOT int)
- Relations: use "{_id: value}"`;
    } else {
      dbTypeContent += `**SQL (${dbType}):**
- Primary key: "id"
- ID type when creating tables: use "int" with auto-increment OR "uuid"
- Relations: use "{id: value}"`;
    }

    const dbTypeHint = {
      category: 'database_type',
      title: 'Database Type Information',
      content: dbTypeContent,
    };

    // 2. Relations Hint
    const relationContent = `**Relations:**
- Use propertyName (NOT FK column names like mainTableId, categoryId, userId)
- targetTable must be object: {"${idFieldName}": value}, NOT string
- M2O: {"category": {"${idFieldName}": 1}} or {"category": 1}
- O2O: {"profile": {"${idFieldName}": 5}} or {"profile": {new_data}}
- M2M: {"tags": [{"${idFieldName}": 1}, {"${idFieldName}": 2}, 3]}
- O2M: {"items": [{"${idFieldName}": 10, qty: 5}, {new_item}]}`;

    const relationHint = {
      category: 'relations',
      title: 'Relation Behavior',
      content: relationContent,
    };

    // 3. Metadata Hint
    const metadataContent = `**Auto-generated Fields:**
- createdAt/updatedAt are automatically added to all tables (DO NOT include in columns when creating tables)
- Foreign key columns are automatically indexed

**Table Naming:**
- Most tables follow "_definition" suffix pattern (e.g., route_definition, user_definition)
- Always use get_metadata to discover actual table names - NEVER assume from user phrasing
- Infer closest match from returned list (e.g., "route" → "route_definition")`;

    const metadataHint = {
      category: 'metadata',
      title: 'Table Metadata & Auto-fields',
      content: metadataContent,
    };

    // 4. Field Optimization Hint
    const fieldOptContent = `**Field Selection (CRITICAL for token saving):**
BEFORE fetching data:
1. Call get_table_details first to see available fields
2. Fetch ONLY needed fields:
   - Count query: fetch only "${idFieldName}" field
   - List names: fetch only "${idFieldName},name"
   - Specific fields: only those mentioned by user
3. Use limit = 0 for "all" or "how many" queries
4. Example: "How many routes?" → get_table_details("route_definition") → dynamic_repository(table="route_definition", operation="find", fields="${idFieldName}", limit=0)

**Limit Usage:**
- limit = 0: fetch ALL records (no limit)
- limit > 0: fetch specified number
- Default: 10 if not specified`;

    const fieldOptHint = {
      category: 'field_optimization',
      title: 'Field & Query Optimization',
      content: fieldOptContent,
    };

    // 5. Table Operations Hint
    const tableOpsContent = `**Creating Tables:**
1. Check existence: find table_definition where name = table_name
2. Use get_table_details on similar table for reference
3. **CRITICAL:** Every table MUST include a column named "${idFieldName}" with isPrimary = true
   - SQL databases: use "int" (auto-increment by default) OR "uuid"
   - MongoDB: MUST use "uuid" (NOT int)
4. targetTable in relations MUST be object: {"${idFieldName}": table_id}
5. DO NOT include createdAt/updatedAt in columns (auto-added)
6. ALWAYS ask user confirmation before creating tables

**Example CREATE TABLE (SQL - int):**
{
  "table": "table_definition",
  "operation": "create",
  "data": {
    "name": "products",
    "columns": [
      {"name": "${idFieldName}", "type": "int", "isPrimary": true},
      {"name": "name", "type": "varchar", "isNullable": false},
      {"name": "price", "type": "decimal", "isNullable": true}
    ]
  }
}

**Example CREATE TABLE (MongoDB - uuid):**
{
  "table": "table_definition",
  "operation": "create",
  "data": {
    "name": "products",
    "columns": [
      {"name": "${idFieldName}", "type": "uuid", "isPrimary": true},
      {"name": "name", "type": "varchar", "isNullable": false},
      {"name": "price", "type": "decimal", "isNullable": true}
    ]
  }
}

**Updating/Deleting Tables:**
1. Find table_definition by name to get its ${idFieldName}
2. Use that ${idFieldName} for update/delete operation

**CRITICAL - Respect User's Exact Request:**
- Use EXACTLY the names/values user provides
- Do NOT add suffixes like "_definition" or modify values unless user explicitly requests it
- Example: User asks for name "post" → use "post", NOT "post_definition"

**BATCH OPERATIONS (CRITICAL for multiple records):**
When user requests creating/updating/deleting MULTIPLE records (5+), ALWAYS use batch operations:

**Batch Create (create multiple records at once):**
{
  "table": "product",
  "operation": "batch_create",
  "dataArray": [
    {"name": "Product 1", "price": 10},
    {"name": "Product 2", "price": 20},
    {"name": "Product 3", "price": 30}
  ]
}

**Batch Update (update multiple records):**
{
  "table": "product",
  "operation": "batch_update",
  "updates": [
    {"${idFieldName}": 1, "data": {"price": 15}},
    {"${idFieldName}": 2, "data": {"price": 25}}
  ]
}

**Batch Delete (delete multiple records):**
{
  "table": "product",
  "operation": "batch_delete",
  "ids": [1, 2, 3, 4, 5]
}

**When to use batch:**
- User asks for "100 sample records" → use batch_create
- User asks to "update all prices" → find records, then batch_update
- User asks to "delete these 10 items" → use batch_delete
- NEVER loop with single create/update/delete when batch is available`;

    const tableOpsHint = {
      category: 'table_operations',
      title: 'Table Creation & Management',
      content: tableOpsContent,
    };

    // 6. Error Handling Hint
    const errorContent = `**CRITICAL Error Rules:**
- If ANY tool returns error: true, STOP ALL OPERATIONS IMMEDIATELY
- DO NOT call additional tools after error
- DO NOT attempt auto-recovery
- IMMEDIATELY report to user: "Error: [message]. [details]. What would you like to do?"
- Delete operations require ${idFieldName} (not where) - find record first if needed

**Violating these rules is STRICTLY FORBIDDEN**`;

    const errorHint = {
      category: 'error_handling',
      title: 'Error Handling Protocol',
      content: errorContent,
    };

    // 7. Table Discovery Hint
    const discoveryContent = `**Table Discovery Policy:**
- NEVER assume table names from user phrasing
- If unsure, CALL get_metadata to fetch list of tables
- Infer closest table name from returned list
- For detailed structure, CALL get_table_details with chosen table name

**Examples:**
- User says "route" → call get_metadata → find "route_definition"
- User says "users" → call get_metadata → find "user_definition" or "user"`;

    const discoveryHint = {
      category: 'table_discovery',
      title: 'Table Discovery Rules',
      content: discoveryContent,
    };

    // 8. Nested Relations Hint
    const nestedContent = `**CRITICAL: Nested Relations (Avoid Multiple Queries)**

**Nested Field Selection (Dot Notation):**
- Get related data: relation.field or relation.*
- Example: "roles.name" gets name from roles relation
- Multiple levels: "routePermissions.role.name"
- Wildcard: "roles.*" gets all fields from relation

**Common Examples:**
1. Get route with roles:
   fields="id,path,roles.name,roles.${idFieldName}"

2. Get route with permissions and roles:
   fields="id,path,routePermissions.role.name"

**Nested Filtering (Object Notation):**
- Filter by relation: { relation: { field: { operator: value } } }
- Example: Find routes with Admin role:
   where={ "roles": { "name": { "_eq": "Admin" } } }

**When to Use:**
✅ ALWAYS use nested queries instead of multiple separate queries
✅ "route id 20 roles" → ONE query with fields="id,path,roles.*"
❌ DON'T query route → then query role_definition separately

**Available Operators:**
_eq, _neq, _gt, _gte, _lt, _lte, _in, _not_in, _contains,
_starts_with, _ends_with, _between, _is_null, _is_not_null,
_and, _or, _not

**Performance:**
- Always prefer ONE nested query over multiple queries
- Use specific fields: "roles.name" better than "roles.*" when you only need name
- For reference, call get_hint(category="nested_relations") for detailed examples`;

    const nestedHint = {
      category: 'nested_relations',
      title: 'Nested Relations & Query Optimization',
      content: nestedContent,
    };

    // 9. Route Access Control Hint
    const routeAccessContent = `**How Does a Request Get Through a Route?**

**Access Check Flow (Priority Order):**
1. @Public() or publishedMethods → ✅ ALLOW (no auth)
2. No JWT token → ❌ DENY (401)
3. user.isRootAdmin = true → ✅ ALLOW (bypasses all)
4. No routePermissions → ❌ DENY (403)
5. Check routePermissions:
   - Find where methods includes request method (GET/POST/etc.)
   - AND (user in allowedUsers OR user.role matches role)
   - Found? → ✅ ALLOW : ❌ DENY (403)

**Access Levels (Highest → Lowest Priority):**
1. Public: @Public() or publishedMethods (no auth needed)
2. Root admin: isRootAdmin = true (bypasses all checks)
3. User-specific: allowedUsers in route_permission (bypasses role)
4. Role-based: user.role matches route_permission.role

**Important:**
- RoleGuard DISABLED (app.module.ts:104) - only auth, NO authorization
- Method-level: GET/POST/PATCH/DELETE checked separately
- Query with nested: fields="routePermissions.role.name,routePermissions.methods.method"
- Cache reload: POST /admin/reload/routes after changes
- For detailed flow: call get_hint(category="route_access")`;

    const routeAccessHint = {
      category: 'route_access',
      title: 'Route Access Control Flow',
      content: routeAccessContent,
    };

    // 10. Permission Check Hint
    const permissionContent = `**CRITICAL: Permission & Authorization Checking**

**BEFORE any Create/Update/Delete operation, you MUST check permissions!**

**Permission Model:**

1. **Root Admin (isRootAdmin: true):**
   - Full access to everything
   - No further checks needed
   - Proceed with operation

2. **Regular Users:**
   Must check via route_definition permissions:
   - allowedUsers: direct user access
   - allowedRoles: role-based access
   - OR resource ownership (createdBy/userId)

**Permission Check Flow:**

\`\`\`
Step 1: Get current user info + roles
→ dynamic_repository({
    table: "user_definition",
    operation: "findOne",
    where: { ${idFieldName}: { _eq: $user.${idFieldName} } },
    fields: "${idFieldName},email,isRootAdmin,roles.*"
  })

Step 2: Check if root admin
→ IF user.isRootAdmin === true → ✅ FULL ACCESS, proceed

Step 3: Check route permissions
→ dynamic_repository({
    table: "route_definition",
    operation: "findOne",
    where: { path: { _eq: "/resource-path" } },
    fields: "allowedUsers.${idFieldName},allowedRoles.${idFieldName}"
  })

Step 4: Verify access
→ Check: user in allowedUsers?
→ Check: user.roles matches allowedRoles?
→ Check: user owns resource? (createdBy.${idFieldName} === user.${idFieldName})

Step 5: Decision
→ IF any of Step 4 is true → ✅ ALLOW
→ ELSE → ❌ DENY with clear message
\`\`\`

**When to Check:**
✅ ALWAYS check before: ALL operations (Read, Create, Update, Delete)
✅ Including Read/Find operations - user may not have permission to view certain tables/data
✅ This includes queries on user_definition, route_definition, any _definition tables, sensitive configs, etc.
⚠️ Exception: Metadata queries (get_metadata, get_table_details) don't need permission check

**Error Messages:**
- Clear: "Permission denied: You don't have access to manage routes"
- Don't expose: "Permission denied: Resource not found" (if user shouldn't know it exists)

**Examples:**

**Example 1: Delete Route**
\`\`\`
// 1. Get user
const user = await dynamic_repository({
  table: "user_definition",
  operation: "findOne",
  where: { ${idFieldName}: { _eq: $user.${idFieldName} } },
  fields: "${idFieldName},isRootAdmin,roles.${idFieldName}"
});

// 2. Check root admin
if (user.isRootAdmin) {
  await dynamic_repository({
    table: "route_definition",
    operation: "delete",
    id: routeId
  });
  return { success: true };
}

// 3. Check route access
const access = await dynamic_repository({
  table: "route_definition",
  operation: "findOne",
  where: { path: { _eq: "/admin/routes" } },
  fields: "allowedUsers.${idFieldName},allowedRoles.${idFieldName}"
});

const hasAccess = access.allowedUsers.some(u => u.${idFieldName} === user.${idFieldName}) ||
                  access.allowedRoles.some(r => user.roles.some(ur => ur.${idFieldName} === r.${idFieldName}));

if (!hasAccess) {
  return {
    error: true,
    message: "Permission denied: You don't have access to manage routes"
  };
}

// 4. Proceed
await dynamic_repository({
  table: "route_definition",
  operation: "delete",
  id: routeId
});
\`\`\`

**Example 2: Update User Profile**
\`\`\`
// 1. Check if updating own profile
if (targetUserId === $user.${idFieldName}) {
  // Users can update their own profile (limited fields)
  await dynamic_repository({
    table: "user_definition",
    operation: "update",
    id: $user.${idFieldName},
    data: { /* safe fields only */ }
  });
  return { success: true };
}

// 2. Check admin permission
const currentUser = await dynamic_repository({
  table: "user_definition",
  operation: "findOne",
  where: { ${idFieldName}: { _eq: $user.${idFieldName} } },
  fields: "isRootAdmin"
});

if (!currentUser.isRootAdmin) {
  return {
    error: true,
    message: "Permission denied: You can only update your own profile"
  };
}

// 3. Proceed
await dynamic_repository({
  table: "user_definition",
  operation: "update",
  id: targetUserId,
  data: { /* all fields */ }
});
\`\`\`

**CRITICAL RULES:**
- NEVER skip permission checks
- Fail securely by default (deny if unclear)
- Always check BEFORE operations, not after
- Root admin bypasses all checks
- Clear error messages to users`;

    const permissionHint = {
      category: 'permission_check',
      title: 'Permission & Authorization Checking',
      content: permissionContent,
    };

    allHints.push(dbTypeHint, relationHint, metadataHint, fieldOptHint, tableOpsHint, errorHint, discoveryHint, nestedHint, routeAccessHint, permissionHint);

    // Filter by category if specified
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
    // Defensive: Convert findOne to find with limit=1
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
          // Log the actual parameters for debugging
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
          // Execute all creates in parallel with Promise.all for performance
          const createResults = await Promise.all(
            args.dataArray.map(data => repo.create(data))
          );
          return {
            message: `Successfully created ${createResults.length} records`,
            count: createResults.length,
            data: createResults.map(r => r.data?.[0]).filter(Boolean),
            statusCode: 200,
          };
        case 'batch_update':
          if (!args.updates || !Array.isArray(args.updates)) {
            throw new Error('updates (array of {id, data}) is required for batch_update operation');
          }
          // Execute all updates in parallel with Promise.all for performance
          const updateResults = await Promise.all(
            args.updates.map(update => repo.update(update.id, update.data))
          );
          return {
            message: `Successfully updated ${updateResults.length} records`,
            count: updateResults.length,
            data: updateResults.map(r => r.data?.[0]).filter(Boolean),
            statusCode: 200,
          };
        case 'batch_delete':
          if (!args.ids || !Array.isArray(args.ids)) {
            throw new Error('ids (array) is required for batch_delete operation');
          }
          // Execute all deletes in parallel with Promise.all for performance
          const deleteResults = await Promise.all(
            args.ids.map(id => repo.delete(id))
          );
          return {
            message: `Successfully deleted ${deleteResults.length} records`,
            count: deleteResults.length,
            statusCode: 200,
          };
        default:
          throw new Error(`Unknown operation: ${args.operation}`);
      }
    } catch (error: any) {
      const errorMessage = error?.message || error?.response?.message || String(error);
      const errorType = classifyError(error);
      const details = error?.details || error?.response?.details || {};

      // Check if operation needs human escalation
      const escalation = shouldEscalateToHuman({
        operation: args.operation,
        table: args.table,
        error,
      });

      // Format user-friendly error message
      const userMessage = formatErrorForUser(error);

      // Build enhanced error response
      const errorResponse: any = {
        error: true,
        errorType,
        errorCode: error?.errorCode || error?.response?.errorCode || errorType,
        message: errorMessage,
        userMessage,
        details,
      };

      // Add escalation info if needed
      if (escalation.shouldEscalate) {
        errorResponse.requiresHumanConfirmation = true;
        errorResponse.escalationReason = escalation.reason;
        errorResponse.escalationMessage = formatEscalationMessage(escalation);
      }

      // Add critical stop instruction for business logic errors
      if (
        errorType === 'RESOURCE_EXISTS' ||
        errorType === 'RESOURCE_NOT_FOUND' ||
        errorType === 'PERMISSION_DENIED' ||
        errorType === 'INVALID_INPUT'
      ) {
        errorResponse.suggestion =
          '🛑 CRITICAL: STOP ALL OPERATIONS NOW! You MUST report this error to the user immediately and ask how to proceed. DO NOT call any more tools.';
      }

      return errorResponse;
    }
  }
}

