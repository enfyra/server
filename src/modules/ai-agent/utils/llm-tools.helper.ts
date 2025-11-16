interface ToolDefinition {
  name: string;
  description: string;
  parameters: {
    type: string;
    properties: Record<string, any>;
    required?: string[];
  };
}

export const TOOL_BINDS_TOOL: ToolDefinition = {
  name: 'tool_binds',
  description: `Purpose → select which tools are needed for the current request.

Use this tool FIRST to determine which tools should be available for this conversation turn.

**CRITICAL - Complete Tool Selection:**
- You MUST include ALL tools that might be needed for the request, even if they seem optional
- If the LLM might need a tool (e.g., get_fields for field validation, get_table_details for schema check), include it in the toolNames array
- It's better to bind a few extra tools than to miss a required tool
- If you're uncertain, include the tool - unused tools don't cause problems, but missing tools cause errors

**Common mistakes to avoid:**
- Missing get_fields when LLM might need to check field names
- Missing get_table_details when LLM might need schema information
- Missing get_hint when creating/updating tables (always include for schema operations)
- Only binding one tool when multiple might be needed (e.g., get_table_details + create_record for create operations)

Inputs:
- toolNames (required): array of tool names to bind for this request

Available tools:
- list_tables: Get list of all tables
- get_table_details: Get full schema and optionally table data
- get_fields: Get field list for reads (include if LLM might need to check field names)
- find_records: Find/query records in a table
- count_records: Count records in a table (with or without filter)
- create_record: Create a single record in a table
- update_record: Update a single record by ID
- delete_record: Delete a single record by ID
- batch_create_records: Create multiple records (2+ records)
- batch_update_records: Update multiple records (2+ records)
- batch_delete_records: Delete multiple records (2+ records)
- create_table: Create new table schema
- update_table: Update existing table schema
- delete_table: Delete/drop table
- get_hint: Get comprehensive guidance when uncertain

Examples:
- Simple greeting: {"toolNames": []}
- Need guidance: {"toolNames": ["get_hint"]}

**CRUD Operations:**
- Find/view records: {"toolNames": ["find_records"]}
- Count records: {"toolNames": ["count_records"]}
- Find with schema check: {"toolNames": ["get_table_details", "find_records"]}
- Create single record: {"toolNames": ["get_table_details", "create_record"]}
- Update single record: {"toolNames": ["get_table_details", "update_record"]}
- Delete single record: {"toolNames": ["delete_record"]}
- Batch create (2+ records): {"toolNames": ["get_table_details", "batch_create_records"]}
- Batch update (2+ records): {"toolNames": ["get_table_details", "batch_update_records"]}
- Batch delete (2+ records): {"toolNames": ["batch_delete_records"]}

**Schema Operations:**
- Create table: {"toolNames": ["create_table", "get_hint"]}
- Update table: {"toolNames": ["update_table", "get_hint"]}
- Delete table: {"toolNames": ["find_records", "delete_table"]}
- View full schema: {"toolNames": ["get_table_details"]}
- Field names only: {"toolNames": ["get_fields"]}
- When uncertain about fields: {"toolNames": ["get_fields", "find_records"]}

**Table Discovery:**
- List all tables (metadata list, no filter): {"toolNames": ["list_tables"]}
- Filter tables (with condition): {"toolNames": ["find_records"]}
- Count records in specific table ("how many X", "có bao nhiêu X"): {"toolNames": ["count_records"]}
- Query records in table: {"toolNames": ["find_records"]}
`,
  parameters: {
    type: 'object',
    properties: {
      toolNames: {
        type: 'array',
        items: {
          type: 'string',
                 enum: [
                   'list_tables',
                   'get_table_details',
                   'get_fields',
                   'find_records',
                   'count_records',
                   'create_record',
                   'update_record',
                   'delete_record',
                   'batch_create_records',
                   'batch_update_records',
                   'batch_delete_records',
                   'create_table',
                   'update_table',
                   'delete_table',
                   'get_hint',
                 ],
        },
        description: 'Array of tool names to bind for this request. Use empty array [] if no tools are needed.',
      },
    },
    required: ['toolNames'],
  },
};

export const COMMON_TOOLS: ToolDefinition[] = [
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
{
  tables: Array<{name: string; description?: string; isSingleRecord?: boolean}>;
  tablesList: string[];
}`,
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
- Need to get actual table data by ID or name - set getData=true AND provide either id or name parameter (both must be arrays). CRITICAL: If getData=true but no id/name provided, the tool will fail.
- CRITICAL: When you need table ID or table data, use this tool instead of querying table_definition with find_records

Skip when:
- You only need field names → prefer get_fields
- You already called get_table_details for this table earlier in this response - reuse the previous result

Inputs:
- tableName (required) → array of table names (case-sensitive). For single table, use array with 1 element: ["user_definition"]
- forceRefresh (optional) true to reload metadata
- getData (optional) true to fetch actual table data. CRITICAL: If getData=true, you MUST provide either id or name parameter. If you only need schema metadata (columns, relations, etc.), omit getData parameter.
- id (optional) array of table record IDs to fetch. REQUIRED if getData=true. Array length must match tableName length. For single value, use array with 1 element: [123]
- name (optional) array of table record names to fetch. REQUIRED if getData=true (and id is not provided). Array length must match tableName length. For single value, use array with 1 element: ["table_name"]. Searches by name column

Response format:
{
  [tableName: string]: {
    name: string;
    description?: string;
    isSingleRecord?: boolean;
    id?: number;
    columns: Array<{
      name: string;
      type: string; 
      isNullable: boolean;
      isPrimary: boolean;
      isGenerated?: boolean; 
      defaultValue?: any;
      isUnique?: boolean;
      description?: string;
      options?: Record<string, any>; 
    }>;
    relations: Array<{
      propertyName: string; 
      type: "one-to-one" | "one-to-many" | "many-to-one" | "many-to-many";
      targetTableName: string; 
      foreignKeyColumn?: string; 
      description?: string;
      isNullable: boolean;
      inversePropertyName?: string; 
      cascade?: string; 
    }>;
    data?: any | null; 
  };
  _errors?: string[];
}

CRITICAL - Relations Format for Create/Update:
- When creating or updating records with relations, you MUST use propertyName from relations array
- Format for SQL databases (PostgreSQL, MySQL, etc.): {[propertyName]: {id: value}}
  Example: {"customer": {id: 1}} or {"category": {id: 19}}
- Format for MongoDB: {[propertyName]: {_id: value}}
  Example: {"customer": {_id: "507f1f77bcf86cd799439011"}}
- NEVER use foreignKeyColumn directly (e.g., "customer_id", "customerId") - it's only for reference
- NEVER use simple ID value (e.g., {"customer": 1}) - always use object format with id/_id
- For many-to-many relations, use array format: {[propertyName]: [{id: 1}, {id: 2}]} or {[propertyName]: [{_id: "..."}, {_id: "..."}]}
- Example: If relations array shows {propertyName: "customer", type: "many-to-one", targetTableName: "customers"}, use {"customer": {id: 1}} NOT {"customer_id": 1} or {"customerId": 1} or {"customer": 1}

Note: To check if a table has relations, check if relations array is empty [] or missing → no relations; if has items → has relations.
CRITICAL: Always check relations array to see available propertyName values before creating/updating records with relations.

Examples:
Single table: {"tableName":["user_definition"]}
Single table + data: {"tableName":["user_definition"],"getData":true,"id":[123]}
Single table + data by name: {"tableName":["user_definition"],"getData":true,"name":["admin"]}
Multiple tables: {"tableName":["product","category","order","order_item","customer"]}
Multiple tables + data: {"tableName":["product","category"],"getData":true,"id":[1,2]}
Multiple tables + data by name: {"tableName":["product","category"],"getData":true,"name":["laptop","electronics"]}
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
          description: 'Optional. Set to true to fetch actual table data. CRITICAL: If getData=true, you MUST provide either id or name parameter. If you only need schema metadata (columns, relations, etc.), omit this parameter or set to false. Default: false.',
          default: false,
        },
        id: {
          type: 'array',
          items: {
            oneOf: [{ type: 'string' }, { type: 'number' }],
          },
          description: 'REQUIRED if getData=true (and name is not provided). Array of table record IDs to fetch. Array length must match tableName length. Each ID corresponds to the table at the same index. For single value, use array with 1 element: [123]',
        },
        name: {
          type: 'array',
          items: { type: 'string' },
          description: 'REQUIRED if getData=true (and id is not provided). Array of table record names to fetch. Array length must match tableName length. Each name corresponds to the table at the same index. For single value, use array with 1 element: ["table_name"]. Searches by name column (exact match).',
        },
      },
      required: ['tableName'],
    },
  },
  {
    name: 'get_fields',
    description: `Purpose → list valid field names for a table (lightweight).

Use when:
- Before find_records to avoid invalid field selections
- You know the table but forgot exact column names

Skip when:
- You need types, relations, or constraints → use get_table_details

Inputs:
- tableName (required)

Response:
{
  table: string;
  fields: string[];
}

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
    description: `Purpose → General guidance for system operations. Use for general workflows, not tool-specific rules.

CRITICAL - Call ONCE per tool loop iteration:
- Call get_hint ONLY ONCE per tool loop iteration, even if you need multiple categories
- Use the category array parameter to get multiple categories in ONE call: {"category":["table_operations","error_handling"]}
- NEVER call get_hint multiple times in the same response or in the same tool loop iteration - this wastes tokens and causes infinite loops
- If you already called get_hint in this iteration, DO NOT call it again - reuse the result you already received
- After calling get_hint ONCE, wait for the result, analyze it, then proceed with your next action (which should NOT be calling get_hint again)
- If you find yourself wanting to call get_hint again after already calling it, STOP - you already have the information you need, use it

Use when:
- Need general guidance about system operations (table operations, database type, error handling)
- Unsure about table discovery or complex workflows
Available categories:
- table_operations → Table creation/update operations (create_table, update_table tools)
- field_optimization → General field selection and query optimization (NOT tool-specific)
- database_type → Database-specific context
- error_handling → General error handling protocols
- table_discovery → Finding tables
- complex_workflows → Step-by-step workflows


Input: category (string) or categories (array) or omit for all
Example: {"category":"table_operations"} or {"category":["table_operations","error_handling"]}

Returns:
{
  dbType: string;
  idField: string;
  hints: Array<{category: string; title: string; content: string}>;
  availableCategories: string[];
}`,
    parameters: {
      type: 'object',
      properties: {
        category: {
          oneOf: [
            {
              type: 'string',
              description:
                'Single hint category: database_type, field_optimization, table_operations, error_handling, table_discovery, complex_workflows.',
            },
            {
              type: 'array',
              items: {
                type: 'string',
                enum: ['database_type', 'field_optimization', 'table_operations', 'error_handling', 'table_discovery', 'complex_workflows'],
              },
              description:
                'Multiple hint categories to retrieve at once. Useful when you need guidance on multiple topics (e.g., ["table_operations", "error_handling"]).',
            },
          ],
          description:
            'Hint category (string) or categories (array of strings). Available: database_type, field_optimization, table_operations, error_handling, table_discovery, complex_workflows. Omit for all hints.',
        },
      },
    },
  },
  {
    name: 'create_table',
    description: `Purpose → Create a new table.

CRITICAL - ONE Table Per Response:
- Call create_table ONLY ONCE per response
- If you need to create multiple tables, call create_table ONE BY ONE in separate responses
- Wait for the result of the first create_table call before calling the next one
- NEVER call create_table multiple times in the same response (e.g., creating 5 tables at once) - this causes errors and violates the one-tool-per-response rule

Create with Relations:
- Include relations in the initial create_table call. Do NOT create table first then update with relations
- Include all relations in data.relations array from the start - this saves tool calls and time
- CRITICAL: Do NOT create FK columns manually - the system auto-generates them from relations
- For M2O/O2O relations: Create target tables FIRST, then create source tables with relations
- Example: To create order with customer relation, create customers table first, then create orders table with relation to customers

System Table Safety:
- Do not remove/edit built-in columns/relations in system tables (*_definition tables)
- Only add new columns/relations when extending system tables
- createdAt/updatedAt are auto-generated; do not include in columns array

Inputs:
- name (required): Table name (snake_case, lowercase)
- description (optional): Table description
- columns (required): Array of column definition objects. MUST include id column with isPrimary=true. CRITICAL: For SQL databases, use type="int" (PREFERRED). For MongoDB, use type="uuid" (REQUIRED). CRITICAL: Do NOT include createdAt/updatedAt. Do NOT include FK columns

Column Schema:
{
  name: string; 
  type: "int" | "varchar" | "boolean" | "text" | "date" | "float" | "simple-json" | "enum" | "uuid"; 
  isPrimary?: boolean; 
  isGenerated?: boolean; 
  isNullable?: boolean; 
  default?: any; 
  isUnique?: boolean; 
  description?: string; 
  isHidden?: boolean; 
  isUpdatable?: boolean; 
  isSystem?: boolean; 
  values?: string[]; 
  index?: boolean; 
}

Example columns:
[{"name":"id","type":"int","isPrimary":true,"isGenerated":true},{"name":"name","type":"varchar","isNullable":false},{"name":"email","type":"varchar","isNullable":false,"isUnique":true}]

- relations (optional): Array of relation definition objects. CRITICAL: Include relations here, not in separate update_table call. Do NOT include FK columns in columns array

Relation Schema:
{
  propertyName: string; 
  type: "one-to-one" | "one-to-many" | "many-to-one" | "many-to-many"; 
  targetTable: {id: number}; 
  inversePropertyName?: string; 
  isNullable?: boolean; 
  description?: string; 
  onDelete?: "CASCADE" | "RESTRICT" | "SET NULL"; 
  isEager?: boolean; 
  index?: boolean; 
}

Example relations:
[{"propertyName":"user","type":"many-to-one","targetTable":{"id":271},"isNullable":false},{"propertyName":"categories","type":"many-to-many","targetTable":{"id":270},"inversePropertyName":"products"}]

- uniques (optional): Array of unique constraints (e.g., [["slug"], ["email", "username"]])
- indexes (optional): Array of index definitions

Output:
{
  success: boolean;
  result?: object;
  errors?: Array<{step: string; error: string; retryable: boolean}>;
  stopReason?: string;
}

For detailed examples and validation rules, call get_hint(category="table_operations") ONCE, then proceed`,
    parameters: {
      type: 'object',
      properties: {
        name: {
          type: 'string',
        },
        description: {
          type: 'string',
        },
        columns: {
          type: 'array',
          items: {
            type: 'object',
          },
        },
        relations: {
          type: 'array',
          items: {
            type: 'object',
          },
        },
        uniques: {
          type: 'array',
          items: {
            type: 'array',
            items: {
              type: 'string',
            },
          },
        },
        indexes: {
          type: 'array',
          items: {
            type: 'object',
          },
        },
      },
      required: ['name', 'columns'],
    },
  },
  {
    name: 'update_table',
    description: `Purpose → Update an existing table.

System Table Safety:
- Do not remove/edit built-in columns/relations in system tables (*_definition tables)
- Only add new columns/relations when extending system tables
- createdAt/updatedAt are auto-generated; do not include in columns array

Example:
update_table({
  "tableName": "product",
  "columns": [{"name":"stock","type":"int"}],
  "relations": [{"propertyName":"supplier","type":"many-to-one","targetTable":{"id":2}}]
})

Inputs:
- tableName (required): Table name to update (must exist)
- tableId (optional): Table ID for faster lookup
- description (optional): New table description
- columns (optional): Array of column definition objects to add/update. Merged by name. CRITICAL: Do NOT include createdAt/updatedAt. Do NOT include FK columns

Column Schema (same as create_table):
{
  name: string; 
  type: "int" | "varchar" | "boolean" | "text" | "date" | "float" | "simple-json" | "enum" | "uuid"; 
  isPrimary?: boolean; 
  isGenerated?: boolean; 
  isNullable?: boolean; 
  default?: any; 
  isUnique?: boolean; 
  description?: string; 
  isHidden?: boolean; 
  isUpdatable?: boolean; 
  isSystem?: boolean; 
  values?: string[]; 
  index?: boolean; 
}

- relations (optional): Array of relation definition objects to add/update. Merged by propertyName

Relation Schema (same as create_table):
{
  propertyName: string; 
  type: "one-to-one" | "one-to-many" | "many-to-one" | "many-to-many"; 
  targetTable: {id: number}; 
  inversePropertyName?: string; 
  isNullable?: boolean; 
  description?: string; 
  onDelete?: "CASCADE" | "RESTRICT" | "SET NULL"; 
  isEager?: boolean; 
  index?: boolean; 
}

- uniques (optional): Array of unique constraints (replaces existing)
- indexes (optional): Array of index definitions (replaces existing)

Output:
{
  success: boolean;
  result?: object;
  errors?: Array<{step: string; error: string; retryable: boolean}>;
  stopReason?: string;
}

For detailed examples and workflows, call get_hint(category="table_operations")`,
    parameters: {
      type: 'object',
      properties: {
        tableName: {
          type: 'string',
        },
        tableId: {
          type: 'number',
        },
        description: {
          type: 'string',
        },
        columns: {
          type: 'array',
          items: {
            type: 'object',
          },
        },
        relations: {
          type: 'array',
          items: {
            type: 'object',
          },
        },
        uniques: {
          type: 'array',
          items: {
            type: 'array',
            items: {
              type: 'string',
            },
          },
        },
        indexes: {
          type: 'array',
          items: {
            type: 'object',
          },
        },
      },
      required: ['tableName'],
    },
  },
  {
    name: 'delete_table',
    description: `Purpose → Delete a table by its ID. This tool permanently removes the table structure and all its data.

CRITICAL - Find Table ID First:
- This tool ONLY accepts table ID (number), NOT table name
- BEFORE calling this tool, you MUST find the table ID first using one of these methods:
  1. Using find_records: find_records({"table":"table_definition","where":{"name":{"_eq":"table_name"}},"fields":"id,name","limit":1})
  2. Using get_table_details: {"tableName":["table_name"],"getData":false} (check the id field in result)
- Extract the id (number) from the result and use it in this tool
- NEVER use table name as id - this tool only accepts numeric id

Workflow:
1. Find table ID: find_records({"table":"table_definition","where":{"name":{"_eq":"table_name"}},"fields":"id,name","limit":1})
2. Get id from result (e.g., id: 123)
3. Delete table: delete_table({"id":123})

Example:
Step 1: find_records({"table":"table_definition","where":{"name":{"_eq":"categories"}},"fields":"id,name","limit":1})
Step 2: delete_table({"id":123}) ← Use the id from step 1

Inputs:
- id (required): Table ID (number) from table_definition. Must be found first using find_records or get_table_details.

Output:
{
  success: boolean;
  id: number;
  name: string;
  message: string;
}

For multiple tables: Delete them ONE BY ONE sequentially (not in parallel) to avoid deadlocks.`,
    parameters: {
      type: 'object',
      properties: {
        id: {
          type: 'number',
          description: 'Table ID (number) from table_definition. Must be found first using find_records({"table":"table_definition","where":{"name":{"_eq":"table_name"}},"fields":"id,name","limit":1}) or get_table_details.',
        },
      },
      required: ['id'],
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
{
  success: boolean;
  task: {
    id: number | string;
    conversationId: number | string;
    type: "create_table" | "update_table" | "delete_table" | "custom";
    status: "pending" | "in_progress" | "completed" | "cancelled" | "failed";
    data?: any;
    result?: any;
    error?: string;
    priority?: number;
    createdAt?: string;
    updatedAt?: string;
  };
}`,
    parameters: {
      type: 'object',
      properties: {
        conversationId: {
          oneOf: [{ type: 'string' }, { type: 'number' }],
        },
        type: {
          type: 'string',
          enum: ['create_table', 'update_table', 'delete_table', 'custom'],
        },
        status: {
          type: 'string',
          enum: ['pending', 'in_progress', 'completed', 'cancelled', 'failed'],
        },
        data: {
          type: 'object',
        },
        result: {
          type: 'object',
        },
        error: {
          type: 'string',
        },
        priority: {
          type: 'number',
          default: 0,
        },
      },
      required: ['conversationId', 'type', 'status'],
    },
  },
  {
    name: 'find_records',
    description: `Purpose → Find/query records in a table.

Permission: Checked automatically for business tables. Operation will fail with clear error if permission is denied.

CRITICAL - Field Names Check:
- BEFORE using fields parameter: You MUST call get_table_details or get_fields FIRST to verify field names
- Field names must match exactly (snake_case vs camelCase)
- Use get_fields for lightweight field list, or get_table_details for full schema

Basic examples:
- Find records: find_records({"table":"order","fields":"id,total","limit":10})
- Find with filter: find_records({"table":"product","where":{"price":{"_gt":100}},"fields":"id,name,price","limit":10})
- Find single record: find_records({"table":"customer","where":{"id":{"_eq":1}},"fields":"id,name,email","limit":1})

Metadata tables (*_definition) can skip permission:
- find_records({"table":"table_definition","skipPermissionCheck":true})

For detailed workflows and best practices, call get_hint(category=["table_operations", "field_optimization"])`,
    parameters: {
      type: 'object',
      properties: {
        table: {
          type: 'string',
          description: 'Name of the table to query.',
        },
        where: {
          type: 'object',
          description:
            'Filter conditions. Supports operators such as _eq,_neq,_gt,_gte,_lt,_lte,_like,_ilike,_contains,_starts_with,_ends_with,_between,_in,_not_in,_is_null,_is_not_null as well as nested logical blocks (_and,_or,_not).',
        },
        fields: {
          type: 'string',
          description:
            'Fields to return. Use get_fields for available fields, then specify ONLY needed fields (e.g., "id" for count, "id,name" for list). Supports wildcards like "columns.*", "relations.*". CRITICAL: Specify ONLY needed fields to save tokens. Do NOT use "*" or omit fields parameter - this returns all fields and wastes tokens.',
        },
        limit: {
          type: 'number',
          description:
            'Max records to return. 0 = no limit (fetch all), > 0 = specified number. Default: 10.',
        },
        sort: {
          type: 'string',
          description:
            'Sort field(s). Format: "fieldName" (ascending) or "-fieldName" (descending). Multi-field: comma-separated, e.g., "name,-createdAt". Examples: "createdAt", "-createdAt", "name,-price". Default: "id".',
        },
        skipPermissionCheck: {
          type: 'boolean',
          description:
            'Optional. Set to true ONLY for metadata operations (*_definition tables). Default: false.',
          default: false,
        },
      },
      required: ['table'],
    },
  },
  {
    name: 'count_records',
    description: `Purpose → Count records in a table (with or without filter).

Permission: Checked automatically for business tables. Operation will fail with clear error if permission is denied.

CRITICAL - COUNT Queries:
- To count TOTAL number of records in a table (no filter):
  * Use: where=null (or omit), meta="totalCount"
  * Read the totalCount value from the response metadata
- To count records WITH a filter (e.g., "how many tables have isSystem=true?"):
  * Use: where={filter conditions}, meta="filterCount"
  * Read the filterCount value from the response metadata
- NEVER use limit=0 just to count - always use limit=1 with appropriate meta parameter

Basic examples:
- Count all records: count_records({"table":"product","meta":"totalCount"})
- Count with filter: count_records({"table":"table_definition","where":{"isSystem":{"_eq":false}},"meta":"filterCount"})

Metadata tables (*_definition) can skip permission:
- count_records({"table":"table_definition","where":{"isSystem":{"_eq":false}},"meta":"filterCount","skipPermissionCheck":true})

For detailed workflows and best practices, call get_hint(category=["table_operations", "field_optimization"])`,
    parameters: {
      type: 'object',
      properties: {
        table: {
          type: 'string',
          description: 'Name of the table to count records in.',
        },
        where: {
          type: 'object',
          description:
            'Optional filter conditions. Omit or set to null to count all records. Supports operators such as _eq,_neq,_gt,_gte,_lt,_lte,_like,_ilike,_contains,_starts_with,_ends_with,_between,_in,_not_in,_is_null,_is_not_null as well as nested logical blocks (_and,_or,_not).',
        },
        meta: {
          type: 'string',
          enum: ['totalCount', 'filterCount'],
          description:
            'REQUIRED. "totalCount" for counting all records (no filter), "filterCount" for counting records matching filter conditions.',
        },
        skipPermissionCheck: {
          type: 'boolean',
          description:
            'Optional. Set to true ONLY for metadata operations (*_definition tables). Default: false.',
          default: false,
        },
      },
      required: ['table', 'meta'],
    },
  },
  {
    name: 'create_record',
    description: `Purpose → Create a single record in a table.

Permission: Checked automatically for business tables. Operation will fail with clear error if permission is denied.

CRITICAL - Schema Check Required:
- BEFORE create operations: You MUST call get_table_details FIRST to check the table schema
- Schema check is MANDATORY to ensure:
  * All required fields (not-null, non-generated) are included in data
  * Column names match exactly (snake_case vs camelCase)
  * Relations format is correct (propertyName, not FK column names)
  * Data types match column types
- Workflow: get_table_details → analyze schema → prepare data → create_record
- DO NOT skip schema check - missing required fields will cause errors

CRITICAL - Check Unique Constraints Before Create:
- BEFORE create operations: You MUST check if records with unique field values already exist
- For tables with unique constraints (e.g., name, email, slug), check existence FIRST using find_records
- Workflow: get_table_details (to identify unique columns) → find_records (to check existence) → create only if not exists
- Example: Before creating category with name="Electronics", check: find_records({"table":"category","where":{"name":{"_eq":"Electronics"}},"fields":"id","limit":1})
- If record exists, skip creation or use update_record instead - duplicate unique values will cause errors
- This applies to ALL unique constraints: single-column (name) and multi-column (email+username)

CRITICAL - Relations Format:
- For relations (many-to-one, one-to-one): Use propertyName from get_table_details result.relations[] with the ID value directly
- Format: {"propertyName": <id_value>} where propertyName is from relations[].propertyName and id_value is a number or string
- Example: If get_table_details shows relation with propertyName="customer", use {"customer": 1} NOT {"customer_id": 1} or {"customerId": 1}
- The system automatically converts propertyName to the correct FK column - you MUST use propertyName, never FK column names
- Check get_table_details result.relations[] to see available propertyName values and their foreignKeyColumn (for reference only, do not use foreignKeyColumn directly)

CRITICAL - DO NOT Include ID in Create Operations:
- NEVER include "id" field in data
- Including id in create operations will cause errors
- Example CORRECT: create_record({"table":"product","data":{"name":"Product 1","price":100},"fields":"id"})
- Example WRONG: create_record({"table":"product","data":{"id":123,"name":"Product 1","price":100}})

Critical rules:
1. Execute ONE operation at a time (sequential, not parallel)
2. ALWAYS call get_table_details FIRST to check schema
3. ALWAYS check unique constraints before create

Basic examples:
- Create: create_record({"table":"product","data":{"name":"Product 1","price":100},"fields":"id"})

For detailed workflows and best practices, call get_hint(category=["table_operations", "field_optimization"])`,
    parameters: {
      type: 'object',
      properties: {
        table: {
          type: 'string',
          description: 'Name of the table to create record in.',
        },
        data: {
          type: 'object',
          description:
            'Data for the new record. CRITICAL: DO NOT include "id" field. Including id will cause errors.',
        },
        fields: {
          type: 'string',
          description:
            'Fields to return. ALWAYS specify minimal fields (e.g., "id" or "id,name") to save tokens. Do NOT use "*" or omit fields parameter - this returns all fields and wastes tokens.',
        },
      },
      required: ['table', 'data'],
    },
  },
  {
    name: 'update_record',
    description: `Purpose → Update a single record by ID.

Permission: Checked automatically for business tables. Operation will fail with clear error if permission is denied.

CRITICAL - Schema Check Required:
- BEFORE update operations: You MUST call get_table_details FIRST to check the table schema
- Schema check is MANDATORY to ensure:
  * Column names match exactly (snake_case vs camelCase)
  * Relations format is correct (propertyName, not FK column names)
  * Data types match column types
- Workflow: get_table_details → analyze schema → prepare data → update_record
- DO NOT skip schema check - invalid field names or types will cause errors

CRITICAL - Check Record Exists Before Update:
- BEFORE update operations: You MUST check if the record exists using find_records
- Workflow: find_records (to verify record exists) → update_record
- Example: Before updating customer with id=1, check: find_records({"table":"customer","where":{"id":{"_eq":1}},"fields":"id","limit":1})
- If record does not exist, skip update or report error

CRITICAL - Relations Format:
- For relations (many-to-one, one-to-one): Use propertyName from get_table_details result.relations[] with the ID value directly
- Format: {"propertyName": <id_value>} where propertyName is from relations[].propertyName and id_value is a number or string
- Example: If get_table_details shows relation with propertyName="customer", use {"customer": 1} NOT {"customer_id": 1} or {"customerId": 1}
- The system automatically converts propertyName to the correct FK column - you MUST use propertyName, never FK column names
- Check get_table_details result.relations[] to see available propertyName values and their foreignKeyColumn (for reference only, do not use foreignKeyColumn directly)

Critical rules:
1. Execute ONE operation at a time (sequential, not parallel)
2. ALWAYS call get_table_details FIRST to check schema
3. ALWAYS check record exists before update

Basic examples:
- Update: update_record({"table":"customer","id":1,"data":{"name":"New Name"},"fields":"id"})

For detailed workflows and best practices, call get_hint(category=["table_operations", "field_optimization"])`,
    parameters: {
          type: 'object',
      properties: {
        table: {
          type: 'string',
          description: 'Name of the table to update record in.',
        },
        id: {
          oneOf: [{ type: 'string' }, { type: 'number' }],
          description: 'ID of the record to update.',
        },
        data: {
          type: 'object',
          description: 'Data to update. Only include fields that need to be changed.',
        },
        fields: {
          type: 'string',
          description:
            'Fields to return. ALWAYS specify minimal fields (e.g., "id" or "id,name") to save tokens. Do NOT use "*" or omit fields parameter - this returns all fields and wastes tokens.',
        },
      },
      required: ['table', 'id', 'data'],
    },
  },
  {
    name: 'delete_record',
    description: `Purpose → Delete a single record by ID.

Permission: Checked automatically for business tables. Operation will fail with clear error if permission is denied.

CRITICAL - Check Record Exists Before Delete:
- BEFORE delete operations: You MUST check if the record exists using find_records
- Workflow: find_records (to verify record exists) → delete_record
- Example: Before deleting order with id=1, check: find_records({"table":"order","where":{"id":{"_eq":1}},"fields":"id","limit":1})
- If record does not exist, skip delete or report error

CRITICAL - Deleting Tables (NOT Data):
- To DELETE/DROP/REMOVE a TABLE (not data records), you MUST use the delete_table tool:
  1. Find the table_definition record: find_records({"table":"table_definition","where":{"name":{"_eq":"table_name"}},"fields":"id,name","limit":1})
  2. Get the id (number) from the result
  3. Delete the table using delete_table tool: delete_table({"id":<id_from_step_1>})
- NEVER use delete_record to delete tables - use delete_table tool instead
- NEVER use delete_record on data tables (categories, products, etc.) to delete tables - this only deletes data records, not the table itself
- NEVER use table name as id value - delete_table only accepts numeric id
- Example CORRECT for deleting table: 
  Step 1: find_records({"table":"table_definition","where":{"name":{"_eq":"categories"}},"fields":"id,name","limit":1})
  Step 2: delete_table({"id":123}) ← Use the id from step 1
- Example WRONG: delete_record({"table":"categories","id":"categories"}) ← This is INCORRECT

Critical rules:
1. Execute ONE operation at a time (sequential, not parallel)
2. ALWAYS check record exists before delete

Basic examples:
- Delete: delete_record({"table":"order_item","id":1})

For detailed workflows and best practices, call get_hint(category=["table_operations", "field_optimization"])`,
    parameters: {
      type: 'object',
      properties: {
        table: {
          type: 'string',
          description: 'Name of the table to delete record from.',
        },
        id: {
          oneOf: [{ type: 'string' }, { type: 'number' }],
          description: 'ID of the record to delete.',
        },
      },
      required: ['table', 'id'],
    },
  },
  {
    name: 'batch_create_records',
    description: `Purpose → Create multiple records in a table (2+ records).

Permission: Checked automatically for business tables. Operation will fail with clear error if permission is denied.

CRITICAL - When to Use:
- Use for 2+ records to create
- When find_records returns multiple records that need to be created, use this tool

CRITICAL - Metadata Tables:
- Always process metadata tables (table_definition/column_definition/relation_definition) sequentially, one at a time
- NEVER use batch operations on metadata tables - process them one by one

CRITICAL - Schema Check Required:
- BEFORE batch_create operations: You MUST call get_table_details FIRST to check the table schema
- Schema check is MANDATORY to ensure:
  * All required fields (not-null, non-generated) are included in data
  * Column names match exactly (snake_case vs camelCase)
  * Relations format is correct (propertyName, not FK column names)
  * Data types match column types
- Workflow: get_table_details → analyze schema → prepare data → batch_create_records
- DO NOT skip schema check - missing required fields will cause errors

CRITICAL - Check Unique Constraints Before Batch Create:
- BEFORE batch_create operations: You MUST check if records with unique field values already exist
- For tables with unique constraints (e.g., name, email, slug), check existence FIRST using find_records or get_table_details with getData=true
- Workflow: get_table_details (to identify unique columns) → check existing records (find_records or get_table_details with getData=true) → filter out existing records → batch_create only new records
- Example: Before batch creating categories, check existing: find_records({"table":"category","fields":"id,name","limit":0}) or get_table_details({"tableName":["category"],"getData":true})
- Filter dataArray to exclude records that already exist (compare unique field values)
- If all records exist, skip batch_create - duplicate unique values will cause errors
- This applies to ALL unique constraints: single-column (name) and multi-column (email+username)

CRITICAL - Relations Format:
- For relations (many-to-one, one-to-one): Use propertyName from get_table_details result.relations[] with the ID value directly
- Format: {"propertyName": <id_value>} where propertyName is from relations[].propertyName and id_value is a number or string
- Example: If get_table_details shows relation with propertyName="customer", use {"customer": 1} NOT {"customer_id": 1} or {"customerId": 1}
- The system automatically converts propertyName to the correct FK column - you MUST use propertyName, never FK column names
- Check get_table_details result.relations[] to see available propertyName values and their foreignKeyColumn (for reference only, do not use foreignKeyColumn directly)

CRITICAL - DO NOT Include ID in Batch Create:
- NEVER include "id" field in any data object
- Including id in batch_create will cause errors
- Example CORRECT: batch_create_records({"table":"product","dataArray":[{"name":"P1","price":100},{"name":"P2","price":200}],"fields":"id"})
- Example WRONG: batch_create_records({"table":"product","dataArray":[{"id":1,"name":"P1"}],"fields":"id"})

CRITICAL - Fields Parameter is MANDATORY:
- ALWAYS specify fields parameter to reduce response size
- Use minimal fields like "id" or "id,name" - do NOT use "*" or omit fields

Basic examples:
- Batch Create: batch_create_records({"table":"product","dataArray":[{"name":"P1","price":100},{"name":"P2","price":200}],"fields":"id"})

Metadata tables (*_definition) can skip permission:
- batch_create_records({"table":"table_definition","dataArray":[...],"skipPermissionCheck":true,"fields":"id"})

For detailed workflows and best practices, call get_hint(category=["table_operations", "field_optimization"])`,
    parameters: {
      type: 'object',
      properties: {
        table: {
          type: 'string',
          description: 'Name of the table to create records in.',
        },
        dataArray: {
          type: 'array',
          items: {
            type: 'object',
          },
          description: 'Array of data objects. Each object represents one record to create. REQUIRED. CRITICAL: DO NOT include "id" field in any object. Example: [{"name":"P1","price":100},{"name":"P2","price":200}].',
        },
        fields: {
          type: 'string',
          description:
            'REQUIRED. Fields to return. Use minimal fields like "id" or "id,name" - do NOT use "*" or omit fields.',
        },
        skipPermissionCheck: {
          type: 'boolean',
          description:
            'Optional. Set to true ONLY for metadata operations (*_definition tables). Default: false.',
          default: false,
        },
      },
      required: ['table', 'dataArray', 'fields'],
    },
  },
  {
    name: 'batch_update_records',
    description: `Purpose → Update multiple records in a table (2+ records).

Permission: Checked automatically for business tables. Operation will fail with clear error if permission is denied.

CRITICAL - When to Use:
- Use for 2+ records to update
- When find_records returns multiple records that need to be updated, collect ALL IDs and use this tool

CRITICAL - Metadata Tables:
- Always process metadata tables (table_definition/column_definition/relation_definition) sequentially, one at a time
- NEVER use batch operations on metadata tables - process them one by one

CRITICAL - Schema Check Required:
- BEFORE batch_update operations: You MUST call get_table_details FIRST to check the table schema
- Schema check is MANDATORY to ensure:
  * Column names match exactly (snake_case vs camelCase)
  * Relations format is correct (propertyName, not FK column names)
  * Data types match column types
- Workflow: get_table_details → analyze schema → prepare data → batch_update_records
- DO NOT skip schema check - invalid field names or types will cause errors

CRITICAL - Check Records Exist Before Batch Update:
- BEFORE batch_update operations: You MUST check if the records exist using find_records
- Workflow: find_records (to verify records exist) → batch_update_records
- Example: Before batch updating customers, check: find_records({"table":"customer","where":{"id":{"_in":[1,2,3]}},"fields":"id","limit":0})
- If any record does not exist, skip that record or report error

CRITICAL - Relations Format:
- For relations (many-to-one, one-to-one): Use propertyName from get_table_details result.relations[] with the ID value directly
- Format: {"propertyName": <id_value>} where propertyName is from relations[].propertyName and id_value is a number or string
- Example: If get_table_details shows relation with propertyName="customer", use {"customer": 1} NOT {"customer_id": 1} or {"customerId": 1}
- The system automatically converts propertyName to the correct FK column - you MUST use propertyName, never FK column names
- Check get_table_details result.relations[] to see available propertyName values and their foreignKeyColumn (for reference only, do not use foreignKeyColumn directly)

CRITICAL - Fields Parameter is MANDATORY:
- ALWAYS specify fields parameter to reduce response size
- Use minimal fields like "id" or "id,name" - do NOT use "*" or omit fields

Basic examples:
- Batch Update: batch_update_records({"table":"customer","updates":[{"id":1,"data":{"name":"New Name 1"}},{"id":2,"data":{"name":"New Name 2"}}],"fields":"id"})

Metadata tables (*_definition) can skip permission:
- batch_update_records({"table":"table_definition","updates":[...],"skipPermissionCheck":true,"fields":"id"})

For detailed workflows and best practices, call get_hint(category=["table_operations", "field_optimization"])`,
    parameters: {
      type: 'object',
      properties: {
        table: {
          type: 'string',
          description: 'Name of the table to update records in.',
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
            'Array of update objects. Each object must have {id: string|number, data: object}. REQUIRED.',
        },
        fields: {
          type: 'string',
          description:
            'REQUIRED. Fields to return. Use minimal fields like "id" or "id,name" - do NOT use "*" or omit fields.',
        },
        skipPermissionCheck: {
          type: 'boolean',
          description:
            'Optional. Set to true ONLY for metadata operations (*_definition tables). Default: false.',
          default: false,
        },
      },
      required: ['table', 'updates', 'fields'],
    },
  },
  {
    name: 'batch_delete_records',
    description: `Purpose → Delete multiple records in a table (2+ records).

Permission: Checked automatically for business tables. Operation will fail with clear error if permission is denied.

CRITICAL - When to Use:
- Use for 2+ records to delete
- When find_records returns multiple records that need to be deleted, collect ALL IDs and use this tool

CRITICAL - Check Records Exist Before Batch Delete:
- BEFORE batch_delete operations: You MUST check if the records exist using find_records
- Workflow: find_records (to verify records exist) → batch_delete_records
- Example: Before batch deleting orders, check: find_records({"table":"order","where":{"id":{"_in":[1,2,3]}},"fields":"id","limit":0})
- If any record does not exist, skip that record or report error

CRITICAL - Deleting Tables (NOT Data):
- To DELETE/DROP/REMOVE a TABLE (not data records), you MUST use the delete_table tool:
  1. Find the table_definition record: find_records({"table":"table_definition","where":{"name":{"_eq":"table_name"}},"fields":"id,name","limit":1})
  2. Get the id (number) from the result
  3. Delete the table using delete_table tool: delete_table({"id":<id_from_step_1>})
- NEVER use batch_delete_records to delete tables - use delete_table tool instead
- NEVER use batch_delete_records on data tables (categories, products, etc.) to delete tables - this only deletes data records, not the table itself

Basic examples:
- Batch Delete: batch_delete_records({"table":"order_item","ids":[1,2,3]})

Metadata tables (*_definition) can skip permission:
- batch_delete_records({"table":"table_definition","ids":[1,2,3],"skipPermissionCheck":true})

For detailed workflows and best practices, call get_hint(category=["table_operations", "field_optimization"])`,
    parameters: {
      type: 'object',
      properties: {
        table: {
          type: 'string',
          description: 'Name of the table to delete records from.',
        },
        ids: {
          type: 'array',
          items: {
            type: ['string', 'number'],
          },
          description: 'Array of IDs to delete. REQUIRED.',
        },
        skipPermissionCheck: {
          type: 'boolean',
          description:
            'Optional. Set to true ONLY for metadata operations (*_definition tables). Default: false.',
          default: false,
        },
      },
      required: ['table', 'ids'],
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

