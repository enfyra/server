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
        description: `Perform CRUD operations on any table using DynamicRepository. ONLY use this when the user explicitly asks to create, read, update, or delete data from tables. Supports complex queries with _and/_or/_not operators and field expansion with wildcards. DO NOT use for simple greetings or general conversations.

**CRITICAL: ALWAYS call get_hint tool FIRST before any database operations!**
The get_hint response includes:
- dbType: Current database type (mysql, postgresql, mongodb, etc.)
- isMongoDB: Boolean indicating if MongoDB is used
- idField: The correct ID field name to use ("id" for SQL, "_id" for MongoDB)

**IMPORTANT: Before creating a table, always check if it already exists using operation='find' with table='table_definition' and where={"name": {"_eq": "table_name"}}. If the table exists, inform the user instead of trying to create it again.**

For CREATE TABLE: Use operation='create' with table='table_definition' and data containing:
{
  "name": "table_name",
  "description": "Table description",
  "columns": [
    { "name": "id", "type": "int", "isPrimary": true, "isGenerated": true, "isNullable": false },
    { "name": "columnName", "type": "varchar", "length": 255, "isNullable": false, "description": "..." },
    { "name": "foreignKeyId", "type": "int", "isNullable": true, "description": "FK to other table" }
  ],
  "relations": [
    { "type": "many-to-one", "propertyName": "relationName", "targetTable": { "id": 1 }, "description": "..." }
  ],
  "uniques": [["columnName"]],
  "indexes": [["columnName"]]
}
CRITICAL: For relations, targetTable MUST be an object with id field: { "id": table_id }
- CORRECT: "targetTable": { "id": 5 }
- WRONG: "targetTable": "user_definition"
- You MUST use get_metadata or dynamic_repository to find the target table's ID first
Note:
- createdAt and updatedAt are AUTOMATICALLY added to ALL tables - DO NOT include them in columns array
- DO NOT mention createdAt/updatedAt when describing table structure to users
- FK columns are auto-indexed

For DELETE TABLE:
1. Call get_hint to get the correct idField ("id" or "_id")
2. Find the table using operation='find' with table='table_definition' and where={"name": {"_eq": "table_name"}}
3. Extract the ID from the found record using the idField from step 1
4. Delete using operation='delete' with table='table_definition' and id=[table_id]

For UPDATE TABLE:
1. Call get_hint to get the correct idField ("id" or "_id")
2. Find the table to get its ID using the correct idField
3. Update using operation='update' with table='table_definition', id=[table_id], and data containing the changes.`,
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
          description: `Perform CRUD operations on any table using DynamicRepository. ONLY use this when the user explicitly asks to create, read, update, or delete data from tables. Supports complex queries with _and/_or/_not operators and field expansion with wildcards. DO NOT use for simple greetings or general conversations.

**CRITICAL: ALWAYS call get_hint tool FIRST before any database operations!**
The get_hint response includes:
- dbType: Current database type (mysql, postgresql, mongodb, etc.)
- isMongoDB: Boolean indicating if MongoDB is used
- idField: The correct ID field name to use ("id" for SQL, "_id" for MongoDB)

**IMPORTANT: Before creating a table, always check if it already exists using operation='find' with table='table_definition' and where={"name": {"_eq": "table_name"}}. If the table exists, inform the user instead of trying to create it again.**

For CREATE TABLE: Use operation='create' with table='table_definition' and data containing:
{
  "name": "table_name",
  "description": "Table description",
  "columns": [
    { "name": "id", "type": "int", "isPrimary": true, "isGenerated": true, "isNullable": false },
    { "name": "columnName", "type": "varchar", "length": 255, "isNullable": false, "description": "..." },
    { "name": "foreignKeyId", "type": "int", "isNullable": true, "description": "FK to other table" }
  ],
  "relations": [
    { "type": "many-to-one", "propertyName": "relationName", "targetTable": { "id": 1 }, "description": "..." }
  ],
  "uniques": [["columnName"]],
  "indexes": [["columnName"]]
}
CRITICAL: For relations, targetTable MUST be an object with id field: { "id": table_id }
- CORRECT: "targetTable": { "id": 5 }
- WRONG: "targetTable": "user_definition"
- You MUST use get_metadata or dynamic_repository to find the target table's ID first
Note:
- createdAt and updatedAt are AUTOMATICALLY added to ALL tables - DO NOT include them in columns array
- DO NOT mention createdAt/updatedAt when describing table structure to users
- FK columns are auto-indexed

For DELETE TABLE:
1. Call get_hint to get the correct idField ("id" or "_id")
2. Find the table using operation='find' with table='table_definition' and where={"name": {"_eq": "table_name"}}
3. Extract the ID from the found record using the idField from step 1
4. Delete using operation='delete' with table='table_definition' and id=[table_id]

For UPDATE TABLE:
1. Call get_hint to get the correct idField ("id" or "_id")
2. Find the table to get its ID using the correct idField
3. Update using operation='update' with table='table_definition', id=[table_id], and data containing the changes.`,
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

