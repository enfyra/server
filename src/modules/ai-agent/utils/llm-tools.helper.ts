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
        description: 'Get system hints and best practices for working with the Enfyra system. CRITICAL: You MUST read hints before performing any database operations (create, update, delete) to understand the correct behavior, especially for relations and database type. The response includes dbType and isMongoDB fields - if isMongoDB is true, you MUST use "_id" instead of "id" for primary key operations. Hints contain important information about how the system handles relations, cascades, and data operations. Always read hints first to understand the database type and correct field names.',
        input_schema: {
          type: 'object',
          properties: {
            category: {
              type: 'string',
              description: 'Category of hint to retrieve (e.g., "relations", "cascade", "data_operations"). If not provided, returns all hints.',
            },
          },
        },
      },
      {
        name: 'dynamic_repository',
        description: `Perform CRUD operations on any table. ONLY use when user explicitly requests database operations.

**Query Operators:**
- _eq: equals, _neq: not equals, _gt: greater than, _gte: >=, _lt: less than, _lte: <=
- _in: in array, _nin: not in array, _contains: string contains, _is_null: is null
- _and: [conditions], _or: [conditions], _not: {condition}

**Examples:**

FIND records:
{"table": "user", "operation": "find", "where": {"name": {"_eq": "John"}}, "fields": "id,name,email"}
{"table": "product", "operation": "find", "where": {"_and": [{"price": {"_gte": 100}}, {"stock": {"_gt": 0}}]}}
{"table": "table_definition", "operation": "find", "where": {"name": {"_eq": "users"}}, "fields": "id,name,columns.*,relations.*"}

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
              description: 'Fields to return. Supports wildcards like "columns.*", "relations.*"',
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
          description: 'Get system hints and best practices for working with the Enfyra system. CRITICAL: You MUST read hints before performing any database operations (create, update, delete) to understand the correct behavior, especially for relations and database type. The response includes dbType and isMongoDB fields - if isMongoDB is true, you MUST use "_id" instead of "id" for primary key operations. Hints contain important information about how the system handles relations, cascades, and data operations. Always read hints first to understand the database type and correct field names.',
          parameters: {
            type: 'object',
            properties: {
              category: {
                type: 'string',
                description: 'Category of hint to retrieve (e.g., "relations", "cascade", "data_operations"). If not provided, returns all hints.',
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

**Query Operators:**
- _eq: equals, _neq: not equals, _gt: greater than, _gte: >=, _lt: less than, _lte: <=
- _in: in array, _nin: not in array, _contains: string contains, _is_null: is null
- _and: [conditions], _or: [conditions], _not: {condition}

**Examples:**

FIND records:
{"table": "user", "operation": "find", "where": {"name": {"_eq": "John"}}, "fields": "id,name,email"}
{"table": "product", "operation": "find", "where": {"_and": [{"price": {"_gte": 100}}, {"stock": {"_gt": 0}}]}}
{"table": "table_definition", "operation": "find", "where": {"name": {"_eq": "users"}}, "fields": "id,name,columns.*,relations.*"}

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
              description: 'Fields to return. Supports wildcards like "columns.*", "relations.*"',
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

