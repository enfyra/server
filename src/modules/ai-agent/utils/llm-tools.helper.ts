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
        description: 'Get detailed metadata of a specific table including columns, relations, constraints, and all properties needed for creating or modifying the table. Use this to understand table structure before creating similar tables or modifying existing ones. Returns: table name, description, columns (name, type, isNullable, isPrimary, isGenerated, defaultValue, options), relations (type, propertyName, targetTableName, description), uniques, indexes. ONLY use this when the user explicitly asks for details about a specific table or needs information to create/modify tables.',
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

**Categories:** nested_relations, database_type, relations, metadata, field_optimization, table_operations, error_handling, table_discovery

**When to Call:**
- Nested queries/relations → "nested_relations"
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

**Field Selection (CRITICAL for token optimization):**
ALWAYS call get_table_details FIRST to see available fields, then:
- Fetch ONLY needed fields (e.g., count → "id"; list names → "id,name")
- Example: User asks "how many routes?" → {"table": "route_definition", "operation": "find", "fields": "id", "limit": 0}
- DON'T fetch all fields unless explicitly needed

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

**Examples:**

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

CREATE record (with relation):
{"table": "order", "operation": "create", "data": {"userId": 5, "total": 100}}
{"table": "post", "operation": "create", "data": {"title": "Hello", "author": {"id": 3}}}

UPDATE record:
{"table": "user", "operation": "update", "id": 5, "data": {"name": "Jane"}}

DELETE record:
{"table": "user", "operation": "delete", "id": 5}

**Relations:**
- Use propertyName (NOT FK column): {"author": {"id": 3}} not {"authorId": 3}
- M2O: {"category": {"id": 1}} or {"category": 1}
- M2M: {"tags": [{"id": 1}, {"id": 2}, 3]}
- O2M: {"items": [{"id": 10, "qty": 5}, {"productId": 1, "qty": 2}]}

**CREATE TABLE:**
1. Check exists first: find table_definition where name = table_name
2. Use get_table_details on similar table for reference
3. targetTable in relations MUST be object: {"id": table_id}
4. createdAt/updatedAt auto-added, DO NOT include in columns

**DELETE/UPDATE TABLE:**
1. Find table_definition by name to get its id
2. Use that id for delete/update operation`,
        input_schema: {
          type: 'object',
          properties: {
            table: {
              type: 'string',
              description: 'Name of the table to operate on. Use "table_definition" to create new tables.',
            },
            operation: {
              type: 'string',
              enum: ['find', 'create', 'update', 'delete'],
              description: 'Operation to perform. Use "create" with table="table_definition" to create new tables.',
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
          description: 'Get detailed metadata of a specific table including columns, relations, constraints, and all properties needed for creating or modifying the table. Use this to understand table structure before creating similar tables or modifying existing ones. Returns: table name, description, columns (name, type, isNullable, isPrimary, isGenerated, defaultValue, options), relations (type, propertyName, targetTableName, description), uniques, indexes. ONLY use this when the user explicitly asks for details about a specific table or needs information to create/modify tables.',
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

**Categories:** nested_relations, database_type, relations, metadata, field_optimization, table_operations, error_handling, table_discovery

**When to Call:**
- Nested queries/relations → "nested_relations"
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
                description: 'Hint category: nested_relations, database_type, relations, metadata, field_optimization, table_operations, error_handling, table_discovery. Omit for all.',
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

**Field Selection (CRITICAL for token optimization):**
ALWAYS call get_table_details FIRST to see available fields, then:
- Fetch ONLY needed fields (e.g., count → "id"; list names → "id,name")
- Example: User asks "how many routes?" → {"table": "route_definition", "operation": "find", "fields": "id", "limit": 0}
- DON'T fetch all fields unless explicitly needed

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

**Examples:**

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

CREATE record (with relation):
{"table": "order", "operation": "create", "data": {"userId": 5, "total": 100}}
{"table": "post", "operation": "create", "data": {"title": "Hello", "author": {"id": 3}}}

UPDATE record:
{"table": "user", "operation": "update", "id": 5, "data": {"name": "Jane"}}

DELETE record:
{"table": "user", "operation": "delete", "id": 5}

**Relations:**
- Use propertyName (NOT FK column): {"author": {"id": 3}} not {"authorId": 3}
- M2O: {"category": {"id": 1}} or {"category": 1}
- M2M: {"tags": [{"id": 1}, {"id": 2}, 3]}
- O2M: {"items": [{"id": 10, "qty": 5}, {"productId": 1, "qty": 2}]}

**CREATE TABLE:**
1. Check exists first: find table_definition where name = table_name
2. Use get_table_details on similar table for reference
3. targetTable in relations MUST be object: {"id": table_id}
4. createdAt/updatedAt auto-added, DO NOT include in columns

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
              enum: ['find', 'create', 'update', 'delete'],
              description: 'Operation to perform. Use "create" with table="table_definition" to create new tables.',
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
          },
          required: ['table', 'operation'],
        },
      },
    },
  ];
}

