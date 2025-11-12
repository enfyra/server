export function getTools(provider: string = 'OpenAI') {
  if (provider === 'Anthropic') {
    return [
      {
        name: 'get_metadata',
        description: 'Get a brief list of all available tables in the system. Returns only table names and descriptions. ONLY use this when the user explicitly asks about available tables or needs to discover what tables exist. DO NOT use for simple greetings or general conversations.',
        input_schema: {
          type: 'object',
          properties: {
            forceRefresh: {
              type: 'boolean',
              description: 'If true, reloads metadata from database before returning. Default: false.',
              default: false,
            },
          },
        },
      },
      {
        name: 'get_table_details',
        description: `Get detailed metadata of a specific table including columns, relations, constraints. Returns: table name, description, columns (name, type, isNullable, isPrimary, isGenerated, defaultValue, options), relations, uniques, indexes.

**CRITICAL - Avoid Redundant Calls:**
- If you ALREADY called get_table_details for a table in THIS conversation, DO NOT call it again
- REUSE the result from the previous call - table schema doesn't change during conversation
- ONLY call again if user explicitly modifies the table structure
- Example: Already got "post" schema? Don't call get_table_details("post") again - use the previous result

ONLY use this when the user explicitly asks for details about a specific table or needs information to create/modify tables.`,
        input_schema: {
          type: 'object',
          properties: {
            tableName: {
              type: 'string',
              description: 'Name of the table to get detailed metadata for.',
            },
            forceRefresh: {
              type: 'boolean',
              description: 'If true, reloads metadata from database before returning. Default: false.',
              default: false,
            },
          },
          required: ['tableName'],
        },
      },
      {
        name: 'get_hint',
        description: `Get system hints on-demand. Call ONLY when you need specific guidance - DO NOT call for greetings.

**Categories:** nested_relations, route_access, database_type, relations, metadata, field_optimization, table_operations, error_handling, table_discovery

**When to Call:**
- Nested queries/relations → "nested_relations"
- Route access control flow → "route_access"
- Before table operations → "table_operations"
- Before working with relations → "relations"
- Field name issues → "database_type"
- Query optimization → "field_optimization"
- Error handling → "error_handling"
- Table name discovery → "table_discovery"
- Auto-generated fields → "metadata"`,
        input_schema: {
          type: 'object',
          properties: {
            category: {
              type: 'string',
              description: 'Hint category: database_type, relations, metadata, field_optimization, table_operations, error_handling, table_discovery. Omit for all.',
            },
          },
        },
      },
      {
        name: 'dynamic_repository',
        description: `Perform CRUD operations on any table. ONLY use when user explicitly requests database operations.

**⚠️ BATCH OPERATIONS (5+ records):**
- Creating/updating/deleting 5+ records? Use batch_create/batch_update/batch_delete
- ❌ NEVER loop with single create/update/delete (will hit limits!)
- ✅ ONE batch call handles all records efficiently

**CRITICAL: Nested Relations (Query Optimization):**
- ALWAYS use nested fields for related data: "relation.field" or "relation.*"
- ALWAYS use nested filters: {"relation": {"field": {"_eq": value}}}
- Example: "route with roles" → fields="id,path,roles.name,roles.id"
- Example: "routes with Admin role" → where={"roles": {"name": {"_eq": "Admin"}}}
- DON'T make separate queries when you can use nested fields/filters
- Deep nesting: "routePermissions.role.name" (multiple levels)
- For complex cases: call get_hint(category="nested_relations")

**Query Operators:**
- _eq: equals, _neq: not equals, _gt: greater than, _gte: >=, _lt: less than, _lte: <=
- _in: in array, _nin: not in array, _contains: string contains, _is_null: is null
- _and: [conditions], _or: [conditions], _not: {condition}

**Field Selection & Data Structure (CRITICAL):**
ALWAYS call get_table_details FIRST to see table schema (columns, types, relations), then:
- For FIND: Fetch ONLY needed fields (e.g., count → "id"; list names → "id,name")
- For CREATE/UPDATE: Use EXACT column names from schema in your data objects
- Example: User asks "how many routes?" → {"table": "route_definition", "operation": "find", "fields": "id", "limit": 0}
- Example: User asks "create post with title" → get_table_details first, then use exact columns
- DON'T fetch all fields or guess column names

**Limit (IMPORTANT):**
- limit = 0: fetch ALL records without limit (use this for "all", "how many", or counting)
- limit > 0: fetch only specified number of records
- Default: 10 records if limit not specified
- ALWAYS set limit = 0 when user asks for total count or "all" records

**Sort (Field Ordering):**
- Format: "fieldName" (ascending) or "-fieldName" (descending)
- Multi-field: Use comma-separated fields, e.g., "name,-createdAt" (sort by name ASC, then createdAt DESC)
- Default: "id" if not specified
- Examples: "createdAt", "-createdAt", "name,-price", "-updatedAt,name"

**⚠️ CRITICAL - BATCH vs SINGLE Operations:**
**WHEN TO USE BATCH (for 5+ records):**
- User asks to create/update/delete MULTIPLE records (e.g., "create 10 products", "delete these 5 users")
- Use batch_create, batch_update, batch_delete - ONE call handles ALL records
- ✅ CORRECT: {"operation": "batch_create", "dataArray": [{...}, {...}, {...}]}
- ❌ WRONG: Loop calling {"operation": "create"} multiple times (will hit limits!)

**WHEN TO USE SINGLE (for 1-4 records):**
- Creating/updating/deleting individual records
- Use create, update, delete operations

**🔴 CRITICAL - Before batch_create/create/update:**
1. ALWAYS call get_table_details FIRST to see exact column names
2. USE those EXACT column names in your data/dataArray objects - DO NOT guess or infer!
3. Example: User says "create posts with title and content"
   → Step 1: get_table_details(tableName="post") → columns: ["id", "title", "content", "authorId"]
   → Step 2: batch_create with dataArray=[{"title": "...", "content": "...", "authorId": 1}, ...]
   → ❌ WRONG: Using "name" or "body" instead of actual column names from schema

**🔴 CRITICAL - After create/update operations:**
- The tool returns created/updated records directly - DO NOT query again
- When presenting results to user, FILTER and show only relevant fields (don't dump all data)
- Example: After creating posts, show summary like "Created 5 posts: Post 1, Post 2, Post 3, Post 4, Post 5"
- Example: After update, show "Updated product #123: price changed to $15"

**Examples:**

BATCH OPERATIONS (for 5+ records - ALWAYS use these instead of looping):
{"table": "product", "operation": "batch_create", "dataArray": [{"name": "Product 1", "price": 10}, {"name": "Product 2", "price": 20}, {"name": "Product 3", "price": 30}]}
{"table": "product", "operation": "batch_update", "updates": [{"id": 1, "data": {"price": 15}}, {"id": 2, "data": {"price": 25}}]}
{"table": "product", "operation": "batch_delete", "ids": [1, 2, 3, 4, 5]}

FIND records:
{"table": "user", "operation": "find", "where": {"name": {"_eq": "John"}}, "fields": "id,name,email"}
{"table": "product", "operation": "find", "where": {"_and": [{"price": {"_gte": 100}}, {"stock": {"_gt": 0}}]}, "limit": 0}
{"table": "route_definition", "operation": "find", "fields": "id,path", "limit": 0}
{"table": "user", "operation": "find", "fields": "id,name,createdAt", "sort": "-createdAt", "limit": 5}
{"table": "product", "operation": "find", "fields": "id,name,price", "sort": "price", "limit": 0}

NESTED FIELDS (get related data in ONE query):
{"table": "route_definition", "operation": "find", "where": {"id": {"_eq": 20}}, "fields": "id,path,roles.id,roles.name", "limit": 1}
{"table": "user", "operation": "find", "fields": "id,name,posts.title,posts.createdAt", "limit": 5}
{"table": "route_definition", "operation": "find", "fields": "id,path,handlers.*,routePermissions.role.name", "limit": 0}

NESTED FILTERS (filter by related data):
{"table": "route_definition", "operation": "find", "where": {"roles": {"name": {"_eq": "Admin"}}}, "fields": "id,path,roles.name"}
{"table": "route_definition", "operation": "find", "where": {"roles": {"id": {"_eq": 5}}}, "fields": "id,path", "limit": 0}
{"table": "user", "operation": "find", "where": {"_or": [{"roles": {"name": {"_eq": "Admin"}}}, {"roles": {"name": {"_eq": "Moderator"}}}]}, "fields": "id,name,roles.name"}

CREATE record (MUST call get_table_details first to see exact columns):
{"table": "order", "operation": "create", "data": {"userId": 5, "total": 100}}
{"table": "post", "operation": "create", "data": {"title": "Hello", "author": {"id": 3}}}

UPDATE record (MUST use exact column names from schema):
{"table": "user", "operation": "update", "id": 5, "data": {"name": "Jane"}}

DELETE record:
{"table": "user", "operation": "delete", "id": 5}

**Relations:**
- Use propertyName (NOT FK column): {"author": {"id": 3}} not {"authorId": 3}
- M2O: {"category": {"id": 1}} or {"category": 1}
- M2M: {"tags": [{"id": 1}, {"id": 2}, 3]}
- O2M: {"items": [{"id": 10, "qty": 5}, {"productId": 1, "qty": 2}]}

**Route Access Control:**
Request access flow: @Public() > isRootAdmin > allowedUsers > role match
RoleGuard DISABLED - only auth runs, no authorization
For access flow details: call get_hint(category="route_access")

**CREATE TABLE:**
1. Check exists first: find table_definition where name = table_name
2. Use get_table_details on similar table for reference
3. **CRITICAL:** MUST include "id" column with isPrimary=true
   - SQL (MySQL/PostgreSQL/SQLite): use int (auto-increment by default) OR uuid
     {"name": "id", "type": "int", "isPrimary": true}
   - MongoDB: MUST use uuid (NOT int)
     {"name": "id", "type": "uuid", "isPrimary": true}
4. targetTable in relations MUST be object: {"id": table_id}
5. createdAt/updatedAt auto-added, DO NOT include in columns
6. **RESPECT USER'S EXACT REQUEST:** Use EXACTLY the names/values user provides. Do NOT add suffixes like "_definition" or modify values unless user explicitly requests it.
Example SQL: {"table": "table_definition", "operation": "create", "data": {"name": "products", "columns": [{"name": "id", "type": "int", "isPrimary": true}, {"name": "name", "type": "varchar"}]}}
Example MongoDB: {"table": "table_definition", "operation": "create", "data": {"name": "products", "columns": [{"name": "id", "type": "uuid", "isPrimary": true}, {"name": "name", "type": "varchar"}]}}

**DELETE/UPDATE TABLE:**
1. Find table_definition by name to get its id
2. Use that id for delete/update operation

**BATCH OPERATIONS (CRITICAL - use for creating/updating/deleting MANY records):**
BATCH CREATE (create multiple records at once):
{"table": "product", "operation": "batch_create", "dataArray": [{"name": "Product 1", "price": 10}, {"name": "Product 2", "price": 20}]}

BATCH UPDATE (update multiple records):
{"table": "product", "operation": "batch_update", "updates": [{"id": 1, "data": {"price": 15}}, {"id": 2, "data": {"price": 25}}]}

BATCH DELETE (delete multiple records):
{"table": "product", "operation": "batch_delete", "ids": [1, 2, 3, 4, 5]}

**IMPORTANT:** When user asks to create multiple records (5+), ALWAYS use batch_create instead of creating one by one.`,
        input_schema: {
          type: 'object',
          properties: {
            table: {
              type: 'string',
              description: 'Name of the table to operate on. Use "table_definition" to create new tables.',
            },
            operation: {
              type: 'string',
              enum: ['find', 'create', 'update', 'delete', 'batch_create', 'batch_update', 'batch_delete'],
              description: 'Operation to perform. Use "create" with table="table_definition" to create new tables. Use batch_* operations for creating/updating/deleting multiple records (5+) at once.',
            },
            where: {
              type: 'object',
              description: 'Filter conditions for find/update/delete operations. Supports _and, _or, _not operators.',
            },
            fields: {
              type: 'string',
              description: 'Fields to return. CRITICAL: Call get_table_details first, then specify ONLY needed fields (e.g., "id" for count, "id,name" for list). Supports wildcards like "columns.*", "relations.*"',
            },
            limit: {
              type: 'number',
              description: 'Max records to return. 0 = no limit (fetch all), > 0 = specified number. Default: 10. Use 0 when user asks for "all" or "how many".',
            },
            sort: {
              type: 'string',
              description: 'Sort field(s). Format: "fieldName" (ascending) or "-fieldName" (descending). Multi-field: comma-separated, e.g., "name,-createdAt". Examples: "createdAt", "-createdAt", "name,-price". Default: "id".',
            },
            data: {
              type: 'object',
              description: 'Data for create/update operations. For creating tables, include: name, description, columns, relations, uniques, indexes.',
            },
            id: {
              oneOf: [{ type: 'string' }, { type: 'number' }],
              description: 'ID for update/delete operations',
            },
            dataArray: {
              type: 'array',
              items: {
                type: 'object',
              },
              description: 'Array of data objects for batch_create operation. Each object represents one record to create.',
            },
            updates: {
              type: 'array',
              items: {
                type: 'object',
                properties: {
                  id: {
                    type: ['string', 'number'],
                  },
                  data: {
                    type: 'object',
                  },
                },
                required: ['id', 'data'],
              },
              description: 'Array of update objects for batch_update operation. Each object must have {id: string|number, data: object}.',
            },
            ids: {
              type: 'array',
              items: {
                type: ['string', 'number'],
              },
              description: 'Array of IDs for batch_delete operation.',
            },
          },
          required: ['table', 'operation'],
        },
      },
    ];
  }

  return [
    {
      type: 'function' as const,
      function: {
        name: 'get_metadata',
        description: 'Get a brief list of all available tables in the system. Returns only table names and descriptions. ONLY use this when the user explicitly asks about available tables or needs to discover what tables exist. DO NOT use for simple greetings or general conversations.',
        parameters: {
          type: 'object',
          properties: {
            forceRefresh: {
              type: 'boolean',
              description: 'If true, reloads metadata from database before returning. Default: false.',
              default: false,
            },
          },
        },
      },
    },
      {
        type: 'function' as const,
        function: {
          name: 'get_table_details',
          description: 'Get detailed metadata of a specific table including columns, relations, constraints. Returns: table name, description, columns (name, type, isNullable, isPrimary, isGenerated, defaultValue, options), relations, uniques, indexes. CRITICAL: If you ALREADY called get_table_details for a table in THIS conversation, DO NOT call it again - REUSE the result from the previous call (table schema doesn\'t change during conversation). ONLY call again if user explicitly modifies the table structure. ONLY use this when the user explicitly asks for details about a specific table or needs information to create/modify tables.',
          parameters: {
            type: 'object',
            properties: {
              tableName: {
                type: 'string',
                description: 'Name of the table to get detailed metadata for.',
              },
              forceRefresh: {
                type: 'boolean',
                description: 'If true, reloads metadata from database before returning. Default: false.',
                default: false,
              },
            },
            required: ['tableName'],
          },
        },
      },
      {
        type: 'function' as const,
        function: {
          name: 'get_hint',
          description: `Get system hints on-demand. Call ONLY when you need specific guidance - DO NOT call for greetings.

**Categories:** nested_relations, route_access, database_type, relations, metadata, field_optimization, table_operations, error_handling, table_discovery

**When to Call:**
- Nested queries/relations → "nested_relations"
- Route access control flow → "route_access"
- Before table operations → "table_operations"
- Before working with relations → "relations"
- Field name issues → "database_type"
- Query optimization → "field_optimization"
- Error handling → "error_handling"
- Table name discovery → "table_discovery"
- Auto-generated fields → "metadata"`,
          parameters: {
            type: 'object',
            properties: {
              category: {
                type: 'string',
                description: 'Hint category: nested_relations, route_access, database_type, relations, metadata, field_optimization, table_operations, error_handling, table_discovery. Omit for all.',
              },
            },
          },
        },
      },
      {
        type: 'function' as const,
        function: {
          name: 'dynamic_repository',
          description: `Perform CRUD operations on any table. ONLY use when user explicitly requests database operations.

**CRITICAL: Nested Relations (Query Optimization):**
- ALWAYS use nested fields for related data: "relation.field" or "relation.*"
- ALWAYS use nested filters: {"relation": {"field": {"_eq": value}}}
- Example: "route with roles" → fields="id,path,roles.name,roles.id"
- Example: "routes with Admin role" → where={"roles": {"name": {"_eq": "Admin"}}}
- DON'T make separate queries when you can use nested fields/filters
- Deep nesting: "routePermissions.role.name" (multiple levels)
- For complex cases: call get_hint(category="nested_relations")

**Query Operators:**
- _eq: equals, _neq: not equals, _gt: greater than, _gte: >=, _lt: less than, _lte: <=
- _in: in array, _nin: not in array, _contains: string contains, _is_null: is null
- _and: [conditions], _or: [conditions], _not: {condition}

**Field Selection & Data Structure (CRITICAL):**
ALWAYS call get_table_details FIRST to see table schema (columns, types, relations), then:
- For FIND: Fetch ONLY needed fields (e.g., count → "id"; list names → "id,name")
- For CREATE/UPDATE: Use EXACT column names from schema in your data objects
- Example: User asks "how many routes?" → {"table": "route_definition", "operation": "find", "fields": "id", "limit": 0}
- Example: User asks "create post with title" → get_table_details first, then use exact columns
- DON'T fetch all fields or guess column names

**Limit (IMPORTANT):**
- limit = 0: fetch ALL records without limit (use this for "all", "how many", or counting)
- limit > 0: fetch only specified number of records
- Default: 10 records if limit not specified
- ALWAYS set limit = 0 when user asks for total count or "all" records

**Sort (Field Ordering):**
- Format: "fieldName" (ascending) or "-fieldName" (descending)
- Multi-field: Use comma-separated fields, e.g., "name,-createdAt" (sort by name ASC, then createdAt DESC)
- Default: "id" if not specified
- Examples: "createdAt", "-createdAt", "name,-price", "-updatedAt,name"

**⚠️ CRITICAL - BATCH vs SINGLE Operations:**
**WHEN TO USE BATCH (for 5+ records):**
- User asks to create/update/delete MULTIPLE records (e.g., "create 10 products", "delete these 5 users")
- Use batch_create, batch_update, batch_delete - ONE call handles ALL records
- ✅ CORRECT: {"operation": "batch_create", "dataArray": [{...}, {...}, {...}]}
- ❌ WRONG: Loop calling {"operation": "create"} multiple times (will hit limits!)

**WHEN TO USE SINGLE (for 1-4 records):**
- Creating/updating/deleting individual records
- Use create, update, delete operations

**🔴 CRITICAL - Before batch_create/create/update:**
1. ALWAYS call get_table_details FIRST to see exact column names
2. USE those EXACT column names in your data/dataArray objects - DO NOT guess or infer!
3. Example: User says "create posts with title and content"
   → Step 1: get_table_details(tableName="post") → columns: ["id", "title", "content", "authorId"]
   → Step 2: batch_create with dataArray=[{"title": "...", "content": "...", "authorId": 1}, ...]
   → ❌ WRONG: Using "name" or "body" instead of actual column names from schema

**🔴 CRITICAL - After create/update operations:**
- The tool returns created/updated records directly - DO NOT query again
- When presenting results to user, FILTER and show only relevant fields (don't dump all data)
- Example: After creating posts, show summary like "Created 5 posts: Post 1, Post 2, Post 3, Post 4, Post 5"
- Example: After update, show "Updated product #123: price changed to $15"

**Examples:**

BATCH OPERATIONS (for 5+ records - ALWAYS use these instead of looping):
{"table": "product", "operation": "batch_create", "dataArray": [{"name": "Product 1", "price": 10}, {"name": "Product 2", "price": 20}, {"name": "Product 3", "price": 30}]}
{"table": "product", "operation": "batch_update", "updates": [{"id": 1, "data": {"price": 15}}, {"id": 2, "data": {"price": 25}}]}
{"table": "product", "operation": "batch_delete", "ids": [1, 2, 3, 4, 5]}

FIND records:
{"table": "user", "operation": "find", "where": {"name": {"_eq": "John"}}, "fields": "id,name,email"}
{"table": "product", "operation": "find", "where": {"_and": [{"price": {"_gte": 100}}, {"stock": {"_gt": 0}}]}, "limit": 0}
{"table": "route_definition", "operation": "find", "fields": "id,path", "limit": 0}
{"table": "user", "operation": "find", "fields": "id,name,createdAt", "sort": "-createdAt", "limit": 5}
{"table": "product", "operation": "find", "fields": "id,name,price", "sort": "price", "limit": 0}

NESTED FIELDS (get related data in ONE query):
{"table": "route_definition", "operation": "find", "where": {"id": {"_eq": 20}}, "fields": "id,path,roles.id,roles.name", "limit": 1}
{"table": "user", "operation": "find", "fields": "id,name,posts.title,posts.createdAt", "limit": 5}
{"table": "route_definition", "operation": "find", "fields": "id,path,handlers.*,routePermissions.role.name", "limit": 0}

NESTED FILTERS (filter by related data):
{"table": "route_definition", "operation": "find", "where": {"roles": {"name": {"_eq": "Admin"}}}, "fields": "id,path,roles.name"}
{"table": "route_definition", "operation": "find", "where": {"roles": {"id": {"_eq": 5}}}, "fields": "id,path", "limit": 0}
{"table": "user", "operation": "find", "where": {"_or": [{"roles": {"name": {"_eq": "Admin"}}}, {"roles": {"name": {"_eq": "Moderator"}}}]}, "fields": "id,name,roles.name"}

CREATE record (MUST call get_table_details first to see exact columns):
{"table": "order", "operation": "create", "data": {"userId": 5, "total": 100}}
{"table": "post", "operation": "create", "data": {"title": "Hello", "author": {"id": 3}}}

UPDATE record (MUST use exact column names from schema):
{"table": "user", "operation": "update", "id": 5, "data": {"name": "Jane"}}

DELETE record:
{"table": "user", "operation": "delete", "id": 5}

**Relations:**
- Use propertyName (NOT FK column): {"author": {"id": 3}} not {"authorId": 3}
- M2O: {"category": {"id": 1}} or {"category": 1}
- M2M: {"tags": [{"id": 1}, {"id": 2}, 3]}
- O2M: {"items": [{"id": 10, "qty": 5}, {"productId": 1, "qty": 2}]}

**Route Access Control:**
Request access flow: @Public() > isRootAdmin > allowedUsers > role match
RoleGuard DISABLED - only auth runs, no authorization
For access flow details: call get_hint(category="route_access")

**CREATE TABLE:**
1. Check exists first: find table_definition where name = table_name
2. Use get_table_details on similar table for reference
3. **CRITICAL:** MUST include "id" column with isPrimary=true
   - SQL (MySQL/PostgreSQL/SQLite): use int (auto-increment by default) OR uuid
     {"name": "id", "type": "int", "isPrimary": true}
   - MongoDB: MUST use uuid (NOT int)
     {"name": "id", "type": "uuid", "isPrimary": true}
4. targetTable in relations MUST be object: {"id": table_id}
5. createdAt/updatedAt auto-added, DO NOT include in columns
6. **RESPECT USER'S EXACT REQUEST:** Use EXACTLY the names/values user provides. Do NOT add suffixes like "_definition" or modify values unless user explicitly requests it.
Example SQL: {"table": "table_definition", "operation": "create", "data": {"name": "products", "columns": [{"name": "id", "type": "int", "isPrimary": true}, {"name": "name", "type": "varchar"}]}}
Example MongoDB: {"table": "table_definition", "operation": "create", "data": {"name": "products", "columns": [{"name": "id", "type": "uuid", "isPrimary": true}, {"name": "name", "type": "varchar"}]}}

**DELETE/UPDATE TABLE:**
1. Find table_definition by name to get its id
2. Use that id for delete/update operation`,
        parameters: {
          type: 'object',
          properties: {
            table: {
              type: 'string',
              description: 'Name of the table to operate on. Use "table_definition" to create new tables.',
            },
            operation: {
              type: 'string',
              enum: ['find', 'create', 'update', 'delete', 'batch_create', 'batch_update', 'batch_delete'],
              description: 'Operation to perform. Use "create" with table="table_definition" to create new tables. Use batch_* operations for creating/updating/deleting multiple records (5+) at once.',
            },
            where: {
              type: 'object',
              description: 'Filter conditions for find/update/delete operations. Supports _and, _or, _not operators.',
            },
            fields: {
              type: 'string',
              description: 'Fields to return. CRITICAL: Call get_table_details first, then specify ONLY needed fields (e.g., "id" for count, "id,name" for list). Supports wildcards like "columns.*", "relations.*"',
            },
            limit: {
              type: 'number',
              description: 'Max records to return. 0 = no limit (fetch all), > 0 = specified number. Default: 10. Use 0 when user asks for "all" or "how many".',
            },
            sort: {
              type: 'string',
              description: 'Sort field(s). Format: "fieldName" (ascending) or "-fieldName" (descending). Multi-field: comma-separated, e.g., "name,-createdAt". Examples: "createdAt", "-createdAt", "name,-price". Default: "id".',
            },
            data: {
              type: 'object',
              description: 'Data for create/update operations. For creating tables, include: name, description, columns, relations, uniques, indexes.',
            },
            id: {
              type: ['string', 'number'],
              description: 'ID for update/delete operations',
            },
            dataArray: {
              type: 'array',
              items: {
                type: 'object',
              },
              description: 'Array of data objects for batch_create operation. Each object represents one record to create.',
            },
            updates: {
              type: 'array',
              items: {
                type: 'object',
                properties: {
                  id: {
                    type: ['string', 'number'],
                  },
                  data: {
                    type: 'object',
                  },
                },
                required: ['id', 'data'],
              },
              description: 'Array of update objects for batch_update operation. Each object must have {id: string|number, data: object}.',
            },
            ids: {
              type: 'array',
              items: {
                type: ['string', 'number'],
              },
              description: 'Array of IDs for batch_delete operation.',
            },
          },
          required: ['table', 'operation'],
        },
      },
    },
  ];
}

