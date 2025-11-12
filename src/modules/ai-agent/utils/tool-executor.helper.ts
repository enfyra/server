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
      case 'get_metadata':
        return await this.executeGetMetadata(args);
      case 'get_table_details':
        return await this.executeGetTableDetails(args);
      case 'get_hint':
        return await this.executeGetHint(args, context);
      case 'dynamic_repository':
        return await this.executeDynamicRepository(args, context);
      default:
        throw new Error(`Unknown tool: ${name}`);
    }
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

    allHints.push(dbTypeHint, relationHint, metadataHint, fieldOptHint, tableOpsHint, errorHint, discoveryHint, nestedHint, routeAccessHint);

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
      availableCategories: ['database_type', 'relations', 'metadata', 'field_optimization', 'table_operations', 'error_handling', 'table_discovery'],
    };
  }

  private async executeDynamicRepository(
    args: {
      table: string;
      operation: 'find' | 'create' | 'update' | 'delete' | 'batch_create' | 'batch_update' | 'batch_delete';
      where?: any;
      fields?: string;
      limit?: number;
      sort?: string;
      data?: any;
      id?: string | number;
      dataArray?: any[];
      updates?: Array<{ id: string | number; data: any }>;
      ids?: Array<string | number>;
    },
    context: TDynamicContext,
  ): Promise<any> {
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
          });

          return await repo.find({
            where: args.where,
            fields: args.fields,
            limit: args.limit,
            sort: args.sort,
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
      const errorCode = error?.errorCode || error?.response?.errorCode || 'UNKNOWN_ERROR';
      const details = error?.details || error?.response?.details || {};
      
      if (errorMessage.includes('already exists') || errorMessage.includes('duplicate')) {
        return {
          error: true,
          errorCode: 'RESOURCE_EXISTS',
          message: errorMessage,
          suggestion: '🛑 CRITICAL: STOP ALL OPERATIONS NOW! The resource already exists. You MUST report this to the user immediately and ask how to proceed. DO NOT call any more tools.',
          details,
        };
      }

      if (errorMessage.includes('not found') || errorMessage.includes('does not exist')) {
        return {
          error: true,
          errorCode: 'RESOURCE_NOT_FOUND',
          message: errorMessage,
          suggestion: '🛑 CRITICAL: STOP ALL OPERATIONS NOW! The resource does not exist. You MUST report this to the user immediately and ask how to proceed. DO NOT call any more tools.',
          details,
        };
      }

      return {
        error: true,
        errorCode,
        message: errorMessage,
        suggestion: '🛑 CRITICAL: STOP ALL OPERATIONS NOW! An error occurred. You MUST report this to the user immediately and ask how to proceed. DO NOT call any more tools.',
        details,
      };
    }
  }
}

