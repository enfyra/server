interface ToolDefinition {
  name: string;
  description: string;
  parameters: {
    type: string;
    properties: Record<string, any>;
    required?: string[];
  };
}

export const COMMON_TOOLS: ToolDefinition[] = [
  {
    name: 'check_permission',
    description: `Purpose → verify access before any data operation.

CRITICAL - Call ONCE per table+operation:
- Call check_permission ONLY ONCE for each unique table+operation combination
- After calling check_permission, REUSE the result for all subsequent operations on the same table+operation
- Do NOT call check_permission multiple times for the same table+operation in the same response
- If you already called check_permission for "table=order_item, operation=delete", do NOT call it again - reuse the previous result

Use when:
- Handling read/create/update/delete on protected data
- User targets restricted tables or admin routes
- No check_permission result exists yet for the same table/route and operation in this response

Skip when:
- Only calling get_metadata, get_table_details, get_fields, get_hint
- Answering casual questions without touching data
- A matching check_permission result already exists in this response (reuse it instead of calling again)
- You already called check_permission for the same table+operation earlier in this response

Inputs:
- operation (required): read | create | update | delete
- table (preferred) → exact table name (e.g., "route_definition")
- routePath (fallback) → exact API route (e.g., "/admin/routes")
- Provide only one; table takes precedence if both sent

Output fields:
- allowed (boolean)
- reason (string: root_admin | user_match | role_match | denied | no_route)
- userInfo (object: id/email/isRootAdmin/roles[])
- routeInfo (object: matched route + permissions array when applicable)
- cacheKey (string) to help identify duplicate checks within the same turn

Example:
{"table":"route_definition","operation":"delete"}`,
    parameters: {
      type: 'object',
      properties: {
        routePath: {
          type: 'string',
          description: 'The route path to check permissions for (e.g., "/admin/routes", "/user"). Optional if table is provided.',
        },
        table: {
          type: 'string',
          description: 'The table name to check permissions for. System will infer the route path from table. Optional if routePath is provided.',
        },
        operation: {
          type: 'string',
          enum: ['read', 'create', 'update', 'delete'],
          description: 'The operation type to check permission for.',
        },
      },
      required: ['operation'],
    },
  },
  {
    name: 'list_tables',
    description: `Purpose → refresh the current list of tables with short descriptions.

Use when:
- Unsure about the exact table the user means
- The request is literally "which tables do we have?"

Skip when:
- The system prompt already gives the table you need

Inputs: {}

Returns:
- tables (array) -> [{name, description?, isSingleRecord?}]
- tablesList (array of names for quick lookup)`,
    parameters: {
      type: 'object',
      properties: {},
    },
  },
  {
    name: 'get_table_details',
    description: `Purpose → load the full schema (columns, relations, indexes, constraints), table ID, AND optionally table data for one or multiple tables.

CRITICAL - Call ONCE per table:
- Call get_table_details ONLY ONCE for each table you need schema for
- After calling, REUSE the schema information for all subsequent operations on that table
- Do NOT call get_table_details multiple times for the same table in the same response
- If you already called get_table_details for "product", do NOT call it again - reuse the previous result
- This tool returns LARGE amounts of data - calling it multiple times wastes tokens

Use when:
- Preparing create/update payloads that must match schema exactly (call ONCE, then reuse)
- Investigating relation structure or constraints (call ONCE, then reuse)
- Need to compare multiple tables' schemas (call once with array of all table names)
- Need to check if tables have relations (call ONCE, then reuse)
- Need to get table ID (e.g., for relations, updates) - the metadata includes the table ID field
- Need to get actual table data by ID or name - set getData=true with id or name parameter (array with 1 element only)
- CRITICAL: When you need table ID or table data, use this tool instead of querying table_definition with dynamic_repository

Skip when:
- You only need field names → prefer get_fields
- You already called get_table_details for this table earlier in this response - reuse the previous result

Inputs:
- tableName (required) → array of table names (case-sensitive). For single table, use array with 1 element: ["user_definition"]
- forceRefresh (optional) true to reload metadata
- getData (optional) true to fetch actual table data (requires id or name, array must have exactly 1 element)
- id (optional) table record ID to fetch (requires getData=true, array must have exactly 1 element)
- name (optional) table record name to fetch (requires getData=true, array must have exactly 1 element, searches by name column)

Response format:
- Returns object with table names as keys, each containing that table's metadata including id field (if available), and data field (if getData=true)
- Example: {"product": {...}, "category": {...}, "order": {...}, "_errors": [...] if any errors}
- Single table: {"user_definition": {...}}

Response highlights (per table):
- name, description, isSingleRecord, database type info, id (table metadata ID)
- columns[] → {name,type,isNullable,isPrimary,defaultValue,isUnique}
- relations[] → {propertyName,type,targetTable:{id:<REAL_ID_FROM_FIND>},inversePropertyName?,cascade?}
  - To check if a table has relations: if relations array is empty [] or missing → no relations; if has items → has relations
- data (if getData=true) → actual table record data matching id or name, or null if not found
- CRITICAL: targetTable.id MUST be REAL ID from database. ALWAYS use get_table_details to get current ID. NEVER use IDs from history.

Examples:
Single table: {"tableName":["user_definition"]}
Single table + data: {"tableName":["user_definition"],"getData":true,"id":123}
Multiple tables: {"tableName":["product","category","order","order_item","customer"]}
With force refresh: {"tableName":["product","category"],"forceRefresh":true}`,
    parameters: {
      type: 'object',
      properties: {
        tableName: {
          type: 'array',
          items: {
            type: 'string',
          },
          description: 'REQUIRED. Array of table names. For single table, use array with 1 element: ["user_definition"]. For multiple tables: ["product", "category", "order"].',
        },
        forceRefresh: {
          type: 'boolean',
          description: 'Optional. Set to true to reload metadata from database. Default: false.',
          default: false,
        },
        getData: {
          type: 'boolean',
          description: 'Optional. Set to true to fetch actual table data. Requires id or name parameter. Default: false.',
          default: false,
        },
        id: {
          oneOf: [{ type: 'string' }, { type: 'number' }],
          description: 'Optional. Table record ID to fetch. Requires getData=true. Use this to get a specific record by ID.',
        },
        name: {
          type: 'string',
          description: 'Optional. Table record name to fetch. Requires getData=true. Searches by name column (exact match).',
        },
      },
      required: ['tableName'],
    },
  },
  {
    name: 'get_fields',
    description: `Purpose → list valid field names for a table (lightweight).

Use when:
- Before dynamic_repository.find to avoid invalid field selections
- You know the table but forgot exact column names

Skip when:
- You need types, relations, or constraints → use get_table_details

Inputs:
- tableName (required)

Response:
- table (string echo)
- fields (string[]) sorted alphabetically

Example request:
{"tableName":"post"}`,
    parameters: {
      type: 'object',
      properties: {
        tableName: {
          type: 'string',
          description: 'Name of the table to get field names for. Examples: "user_definition", "post", "route_definition"',
        },
      },
      required: ['tableName'],
    },
  },
  {
    name: 'get_hint',
    description: `Purpose → Comprehensive guidance, examples, and checklists. Your SAFETY NET when uncertain.

Call IMMEDIATELY when:
- Confidence <100% on any operation
- Encountering errors you don't understand
- Unsure about workflow or sequence
- Need checklists before complex operations

Available categories:
- table_operations (MOST IMPORTANT) → Table creation, relations, batch operations
- permission_check → Permission flows
- field_optimization → Field selection, query optimization
- database_type → Database-specific context
- error_handling → Error protocols
- table_discovery → Finding tables
- complex_workflows → Step-by-step workflows

Input: category (string) or categories (array) or omit for all
Example: {"category":"table_operations"} or {"category":["table_operations","permission_check"]}

Returns: {dbType, idField, hints[], availableCategories[]}

STRATEGY: When in doubt, call get_hint FIRST. Better to spend one tool call for guidance than make mistakes.`,
    parameters: {
      type: 'object',
      properties: {
        category: {
          oneOf: [
            {
              type: 'string',
              description:
                'Single hint category: permission_check, database_type, field_optimization, table_operations, error_handling, table_discovery, complex_workflows.',
            },
            {
              type: 'array',
              items: {
                type: 'string',
                enum: ['permission_check', 'database_type', 'field_optimization', 'table_operations', 'error_handling', 'table_discovery', 'complex_workflows'],
              },
              description:
                'Multiple hint categories to retrieve at once. Useful when you need guidance on multiple topics (e.g., ["table_operations", "permission_check"]).',
            },
          ],
          description:
            'Hint category (string) or categories (array of strings). Available: permission_check, database_type, field_optimization, table_operations, error_handling, table_discovery, complex_workflows. Omit for all hints.',
        },
      },
    },
  },
  {
    name: 'create_table',
    description: `Purpose → Create a new table with automatic validation, FK conflict detection, and retry logic.

This tool automatically:
- Checks if table already exists (fails if exists - use update_table instead)
- Validates table name (snake_case, lowercase), columns, relations
- Validates target tables for relations
- Checks FK column conflicts (system auto-generates FK columns from relation propertyName)
- Retries on retryable errors

CRITICAL - FK Columns:
- System automatically generates FK columns from relation propertyName (e.g., "user" → "userId")
- Do NOT manually create FK columns in columns array - system handles this automatically
- If FK column name conflicts with existing column, use different propertyName

CRITICAL - Create with Relations:
- ALWAYS include relations in the initial create_table call. Do NOT create table first then update with relations
- Include all relations in data.relations array from the start - this saves tool calls and time
- For multiple related tables: Create tables with FK columns (M2O/O2O) BEFORE creating target tables they reference

Inputs:
- name (required): Table name (snake_case, lowercase)
- description (optional): Table description
- columns (required): Array of column definitions. MUST include id column with isPrimary=true, type="int" or "uuid". CRITICAL: Do NOT include createdAt/updatedAt - system auto-generates them. Do NOT include FK columns - system auto-generates them from relations
- relations (optional): Array of relation definitions. targetTable.id MUST be REAL ID from find result. one-to-many and many-to-many MUST include inversePropertyName. System automatically creates FK columns. CRITICAL: Include relations here, not in separate update_table call
- uniques (optional): Array of unique constraints (e.g., [["slug"], ["email", "username"]])
- indexes (optional): Array of index definitions

Output: {success: boolean, result?: object, errors?: array, stopReason?: string}

For detailed examples and validation rules, call get_hint(category="table_operations")`,
    parameters: {
      type: 'object',
      properties: {
        name: {
          type: 'string',
          description: 'Table name (must be snake_case, lowercase, a-z0-9_)',
        },
        description: {
          type: 'string',
          description: 'Optional table description',
        },
        columns: {
          type: 'array',
          items: {
            type: 'object',
          },
          description: 'Array of column definitions. MUST include id column with isPrimary=true.',
        },
        relations: {
          type: 'array',
          items: {
            type: 'object',
          },
          description: 'Optional array of relation definitions. targetTable.id must be REAL ID from find result.',
        },
        uniques: {
          type: 'array',
          items: {
            type: 'array',
            items: {
              type: 'string',
            },
          },
          description: 'Optional array of unique constraints. Format: [["slug"], ["email", "username"]]',
        },
        indexes: {
          type: 'array',
          items: {
            type: 'object',
          },
          description: 'Optional array of index definitions',
        },
      },
      required: ['name', 'columns'],
    },
  },
  {
    name: 'update_table',
    description: `Purpose → Update an existing table with automatic validation, FK conflict detection, merging, and retry logic.

This tool automatically:
- Loads current table data
- Validates update data (columns, relations)
- Validates target tables for new relations
- Checks FK column conflicts (system auto-generates FK columns from relation propertyName)
- Merges update data with existing (preserves system columns/relations)
- Retries on retryable errors

CRITICAL - FK Columns:
- System automatically generates FK columns from relation propertyName (e.g., "user" → "userId")
- Do NOT manually create FK columns in columns array - system handles this automatically
- If FK column name conflicts with existing column, use different propertyName

Inputs:
- tableName (required): Table name to update (must exist)
- tableId (optional): Table ID for faster lookup
- description (optional): New table description
- columns (optional): Array of column definitions to add/update. Merged by name. CRITICAL: Do NOT include createdAt/updatedAt - system auto-generates them. Do NOT include FK columns - system auto-generates them from relations. System columns (id, createdAt, updatedAt) preserved automatically
- relations (optional): Array of relation definitions to add/update. Merged by propertyName. targetTable.id MUST be REAL ID from find result. one-to-many and many-to-many MUST include inversePropertyName. System automatically creates FK columns
- uniques (optional): Array of unique constraints (replaces existing)
- indexes (optional): Array of index definitions (replaces existing)

Output: {success: boolean, result?: object, errors?: array, stopReason?: string}

For detailed examples and workflows, call get_hint(category="table_operations")`,
    parameters: {
      type: 'object',
      properties: {
        tableName: {
          type: 'string',
          description: 'Table name to update (must exist)',
        },
        tableId: {
          type: 'number',
          description: 'Optional table ID for faster lookup (if you already have it)',
        },
        description: {
          type: 'string',
          description: 'Optional new table description',
        },
        columns: {
          type: 'array',
          items: {
            type: 'object',
          },
          description: 'Optional array of column definitions to add/update. Columns are merged with existing columns by name.',
        },
        relations: {
          type: 'array',
          items: {
            type: 'object',
          },
          description: 'Optional array of relation definitions to add/update. Relations are merged with existing relations by propertyName.',
        },
        uniques: {
          type: 'array',
          items: {
            type: 'array',
            items: {
              type: 'string',
            },
          },
          description: 'Optional array of unique constraints. Replaces existing uniques.',
        },
        indexes: {
          type: 'array',
          items: {
            type: 'object',
          },
          description: 'Optional array of index definitions. Replaces existing indexes.',
        },
      },
      required: ['tableName'],
    },
  },
  {
    name: 'update_task',
    description: `Purpose → manage task state in conversation.

Use when:
- Starting a new task (create task with status='pending' or 'in_progress')
- Updating task progress (status='in_progress', add data/result)
- Completing task (status='completed' with result)
- Cancelling task (status='cancelled')
- Task failed (status='failed' with error)

CRITICAL - Task Conflict Detection:
- Before creating a new task, check if conversation already has a task with status='pending' or 'in_progress'
- If new task type conflicts with existing task (e.g., create vs delete), cancel existing task first (status='cancelled')
- If new task is continuation of existing task, update existing task instead of creating new one

Task types:
- create_table: Creating new tables
- update_table: Updating existing tables
- delete_table: Deleting tables
- custom: Other custom tasks

Task status flow:
- pending → in_progress → completed/failed/cancelled

Inputs:
- conversationId (required): Current conversation ID
- type (required): Task type (create_table|update_table|delete_table|custom)
- status (required): Task status (pending|in_progress|completed|cancelled|failed)
- data (optional): Task-specific data (e.g., table names, operations)
- result (optional): Task result when completed
- error (optional): Error message when failed
- priority (optional): Task priority (default: 0, higher = more priority)

Returns:
- success (boolean)
- task (object): Updated task object`,
    parameters: {
      type: 'object',
      properties: {
        conversationId: {
          oneOf: [{ type: 'string' }, { type: 'number' }],
          description: 'Current conversation ID. Required.',
        },
        type: {
          type: 'string',
          enum: ['create_table', 'update_table', 'delete_table', 'custom'],
          description: 'Task type.',
        },
        status: {
          type: 'string',
          enum: ['pending', 'in_progress', 'completed', 'cancelled', 'failed'],
          description: 'Task status.',
        },
        data: {
          type: 'object',
          description: 'Optional. Task-specific data (e.g., table names, operations).',
        },
        result: {
          type: 'object',
          description: 'Optional. Task result when status is completed.',
        },
        error: {
          type: 'string',
          description: 'Optional. Error message when status is failed.',
        },
        priority: {
          type: 'number',
          description: 'Optional. Task priority (default: 0, higher = more priority).',
          default: 0,
        },
      },
      required: ['conversationId', 'type', 'status'],
    },
  },
  {
    name: 'dynamic_repository',
    description: `Purpose → single gateway for CRUD and batch operations.

CRITICAL - Sequential Execution:
- ALWAYS execute operations ONE AT A TIME, step by step
- Do NOT call multiple dynamic_repository operations simultaneously
- Execute first operation → wait for result → analyze → proceed to next
- If you call multiple operations at once and one fails, you'll have to retry all, causing duplicates
- Example: If deleting from multiple tables, delete from table1 → wait for result → delete from table2 → wait for result → continue
- This prevents errors, duplicate operations, and ensures proper error handling

CRITICAL - Schema Check (MANDATORY for create/update):
- BEFORE creating or updating records, you MUST call get_table_details to get the full schema
- Check which columns are required (isNullable=false) and have default values
- Ensure your data object includes ALL required fields (not-null constraints)
- Common required fields: id (auto-generated), createdAt/updatedAt (auto-generated), but check for others like slug, stock, order_number, etc.
- If you skip schema check and get constraint errors, you MUST call get_table_details to fix the issue

CRITICAL - Permission Check:
- MUST call check_permission BEFORE any read/create/update/delete on business tables (non-metadata)
- Metadata tables (*_definition) may skip with skipPermissionCheck=true
- Permission check is MANDATORY for business tables

Operations:
- find: Read records with filters, fields, limit, sort, meta
- create/update/delete: Single record operations
- batch_create/batch_update/batch_delete: Multiple records (use for 2+ deletes, 5+ creates/updates)
- CRITICAL: Always specify minimal fields parameter for create/update (e.g., "id" or "id,name") to save tokens

Workflow for create/update:
1. Call get_table_details with tableName to get schema (required fields, types, defaults, relations)
2. Check permission with check_permission if needed (MANDATORY for business tables)
3. For relations (foreign keys): You MUST verify the referenced record exists first
   - Example: If creating product with category relation, first call dynamic_repository with find to check if category exists
   - NEVER use hardcoded IDs without verifying they exist
   - If the referenced record doesn't exist, create it first or use an existing ID
4. Prepare data object with ALL required fields (check schema for isNullable=false fields)
   - CRITICAL: Use EXACT column names from get_table_details (they are in snake_case format, e.g., order_number, unit_price, not camelCase like orderNumber, unitPrice)
   - Column names in database are ALWAYS snake_case, never camelCase
   - If you see an error about missing column, check if you used camelCase instead of snake_case
5. CRITICAL - Relations Format (TypeORM style):
   - Use propertyName from relations array (e.g., "category", "customer", "order"), NOT FK column names (e.g., "category_id", "customerId")
   - Format: {"category": {"id": 19}} OR {"category": 19} (pass object with id, or just id directly)
   - NEVER use FK column names like "category_id", "customerId", "orderId" - these are auto-generated by system
   - Example: If product has relation "category" → use {"category": {"id": 19}}, NOT {"category_id": 19}
   - Check get_table_details result.relations[] to see available propertyNames
6. Call dynamic_repository with create/update operation

Best practices:
- Use get_table_details/get_fields before writing to understand schema - THIS IS MANDATORY
- Use nested filters/fields (e.g., "roles.name") instead of multiple queries
- For counts: limit=1 + meta="totalCount" (faster than limit=0)
- Metadata operations target *_definition tables only
- Table_definition operations: process sequentially, NO batch operations
- When find returns multiple records, collect ALL IDs and use batch operations

For detailed workflows and examples, call get_hint(category="table_operations") or get_hint(category="complex_workflows")`,
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
          description:
            'Operation to perform: "find" (read records), "create" (insert one), "update" (modify one by id), "delete" (remove one by id), "batch_create" (insert many), "batch_update" (modify many), "batch_delete" (remove many). CRITICAL: Use batch_delete for 2+ deletes, batch_create/batch_update for 5+ creates/updates. When find returns multiple records, collect ALL IDs and use batch operations. NO "findOne" operation - use "find" with limit=1 instead.',
        },
        where: {
          type: 'object',
          description:
            'Filter conditions for find/update/delete operations. Supports operators such as _eq,_neq,_gt,_gte,_lt,_lte,_like,_ilike,_contains,_starts_with,_ends_with,_between,_in,_not_in,_is_null,_is_not_null as well as nested logical blocks (_and,_or,_not).',
        },
        fields: {
          type: 'string',
          description:
            'Fields to return. Use get_fields for available fields, then specify ONLY needed fields (e.g., "id" for count, "id,name" for list). Supports wildcards like "columns.*", "relations.*". CRITICAL: For create/update operations, always specify minimal fields (e.g., "id" or "id,name") to save tokens. Do NOT use "*" or omit fields parameter - this returns all fields and wastes tokens.',
        },
        limit: {
          type: 'number',
          description:
            'Max records to return. 0 = no limit (fetch all), > 0 = specified number. Default: 10. For COUNT queries, use limit=1 with meta="totalCount" (much faster than limit=0).',
        },
        sort: {
          type: 'string',
          description:
            'Sort field(s). Format: "fieldName" (ascending) or "-fieldName" (descending). Multi-field: comma-separated, e.g., "name,-createdAt". Examples: "createdAt", "-createdAt", "name,-price". Default: "id".',
        },
        meta: {
          type: 'string',
          description:
            'Include metadata in response. Values: "totalCount" (total records in table), "filterCount" (records matching filter), "*" (all metadata). CRITICAL: For count queries ("how many?"), use meta="totalCount" with limit=1 (NOT limit=0) for 100x faster performance.',
        },
        data: {
          type: 'object',
          description:
            'Data for create/update operations. For creating tables, include: name, description, columns, relations, uniques, indexes.',
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
          description:
            'Array of update objects for batch_update operation. Each object must have {id: string|number, data: object}.',
        },
        ids: {
          type: 'array',
          items: {
            type: ['string', 'number'],
          },
          description: 'Array of IDs for batch_delete operation.',
        },
        skipPermissionCheck: {
          type: 'boolean',
          description:
            'Optional. Set to true ONLY for metadata operations (*_definition tables). CRITICAL: For business tables (non-metadata), you MUST call check_permission first. Default: false.',
          default: false,
        },
      },
      required: ['table', 'operation'],
    },
  },
];

function toAnthropicFormat(tools: ToolDefinition[]) {
  return tools.map((tool) => ({
    name: tool.name,
    description: tool.description,
    input_schema: tool.parameters,
  }));
}

function toOpenAIFormat(tools: ToolDefinition[]) {
  return tools.map((tool) => ({
    type: 'function' as const,
    function: {
      name: tool.name,
      description: tool.description,
      parameters: tool.parameters,
    },
  }));
}

export function getTools(provider: string = 'OpenAI') {
  if (provider === 'Anthropic') {
    return toAnthropicFormat(COMMON_TOOLS);
  }
  return toOpenAIFormat(COMMON_TOOLS);
}

