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
    name: 'get_table_details',
    description: `Purpose → load the full schema (columns, relations, indexes, constraints), table ID, AND optionally table data for one or multiple tables.

CRITICAL - Call ONCE per table:
- Call get_table_details ONLY ONCE for each table you need schema for
- After calling, REUSE the schema information for all subsequent operations on that table
- Do NOT call get_table_details multiple times for the same table in the same response
- If you already called get_table_details for "product", do NOT call it again - reuse the previous result
- This tool returns LARGE amounts of data - calling it multiple times wastes tokens

CRITICAL - NEVER Query All Tables, ALWAYS Filter First:
- **NEVER call get_table_details for all tables** - ALWAYS filter first using find_records
- If user asks for "non-system tables" or "user tables" → FIRST call find_records({"table":"table_definition","fields":"name,isSystem","where":{"isSystem":{"_eq":false}},"limit":0}) → filter tables with isSystem=false → call get_table_details ONLY for filtered tables
- If user asks for "all tables" → STILL filter first: call find_records({"table":"table_definition","fields":"name,isSystem","limit":0}) → decide which tables are relevant → call get_table_details only for relevant subset
- **NEVER pass all tables to get_table_details** - this returns large amounts of data (50KB+) and wastes tokens
- Example workflow: User says "show me non-system tables" → find_records({"table":"table_definition","fields":"name,isSystem","where":{"isSystem":{"_eq":false}},"limit":0}) → filter tables with isSystem=false (e.g., 3 tables) → call get_table_details({"tableName":["table1","table2","table3"]})
- This tool returns FULL schema (columns, relations, indexes) for each table - querying many tables can return 50KB+ of data
- **Rule of thumb**: If you need to query more than 5 tables, reconsider - maybe you can filter more specifically

Use when:
- Preparing create/update payloads that must match schema exactly (call ONCE, then reuse)
- Investigating relation structure or constraints (call ONCE, then reuse)
- Need to compare multiple tables' schemas (call once with array of all table names, but filter first if user specified criteria)
- Need to check if tables have relations (call ONCE, then reuse)
- Need to get table ID (e.g., for relations, updates) - the metadata includes the table ID field
- Need to get actual table data by ID or name - set getData=true AND provide either id or name parameter (both must be arrays). CRITICAL: If getData=true but no id/name provided, the tool will fail.
- CRITICAL: When you need table ID or table data, use this tool instead of querying table_definition with find_records

Skip when:
- You only need field names → prefer get_fields
- You already called get_table_details for this table earlier in this response - reuse the previous result
- User asks for "non-system tables" but you haven't filtered yet → call find_records({"table":"table_definition","fields":"name,isSystem","where":{"isSystem":{"_eq":false}},"limit":0}) first to filter

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

**Important Field Definitions:**

**Table-level fields:**
- isSystem (boolean): If true, this is a system table (e.g., table_definition, column_definition, relation_definition). System tables are critical for system operation and should NOT be deleted or heavily modified. If false, this is a user-created table that can be safely deleted.
- isSingleRecord (boolean): If true, this table is designed to store only ONE record (singleton pattern). Examples: system settings, global configuration. If false or missing, table can store multiple records.

**Column-level fields:**
- isPrimary (boolean): If true, this column is the primary key (unique identifier for each record). Each table MUST have exactly one column with isPrimary=true. Primary key values are unique and cannot be null.
- isNullable (boolean): If true, this column can contain NULL values. If false, this column is REQUIRED and cannot be null (NOT NULL constraint).
- isGenerated (boolean): If true, this column value is auto-generated by the database (e.g., auto-increment ID, UUID, timestamp). You should NOT provide values for generated columns when creating records.
- isUnique (boolean): If true, this column has a unique constraint - each value must be unique across all records in the table. Cannot have duplicate values.
- defaultValue (any): The default value assigned to this column if no value is provided when creating a record. If isGenerated=true, the database generates the value automatically.

**Relation-level fields:**
- isNullable (boolean): If true, the foreign key column can be NULL (relation is optional). If false, the foreign key is REQUIRED (relation is mandatory).
- type: Relation type - "many-to-one" (many records in this table reference one record in target), "one-to-many" (one record in this table has many in target), "one-to-one" (one-to-one relationship), "many-to-many" (many-to-many via junction table).
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

CRITICAL - Check System Prompt First:
- BEFORE calling this tool, check if your system prompt contains a "RELEVANT WORKFLOWS & RULES" section
- If "RELEVANT WORKFLOWS & RULES" section exists in your system prompt, DO NOT call this tool - all necessary guidance is already provided
- Only call this tool if you do NOT see "RELEVANT WORKFLOWS & RULES" in your system prompt and you need additional guidance

CRITICAL - Call ONCE per tool loop iteration (only if needed):
- Call get_hint ONLY ONCE per tool loop iteration, even if you need multiple categories
- Use the category array parameter to get multiple categories in ONE call: {"category":["table_operations","error_handling"]}
- NEVER call get_hint multiple times in the same response or in the same tool loop iteration - this wastes tokens and causes infinite loops
- If you already called get_hint in this iteration, DO NOT call it again - reuse the result you already received
- After calling get_hint ONCE, wait for the result, analyze it, then proceed with your next action (which should NOT be calling get_hint again)
- If you find yourself wanting to call get_hint again after already calling it, STOP - you already have the information you need, use it

Use when:
- System prompt does NOT contain "RELEVANT WORKFLOWS & RULES" section AND you need general guidance about system operations
- Unsure about table discovery or complex workflows and hints are not already provided
Available categories:
- table_operations → Table creation/update operations (create_tables, update_tables tools)
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
    name: 'create_tables',
    description: `Purpose → Create one or multiple tables. Processes tables sequentially internally to avoid deadlocks.

Create with Relations:
- Include relations in the initial create_tables call. Do NOT create table first then update with relations
- Include all relations in data.relations array from the start - this saves tool calls and time
- CRITICAL: Do NOT create FK columns manually - the system auto-generates them from relations
- For M2O/O2O relations: Create target tables FIRST, then create source tables with relations
- Example: To create order with customer relation, create customers table first, then create orders table with relation to customers

System Table Safety:
- Do not remove/edit built-in columns/relations in system tables (*_definition tables)
- Only add new columns/relations when extending system tables
- createdAt/updatedAt are auto-generated; do not include in columns array

Inputs:
- tables (required): Array of table definition objects. For single table, use array with 1 element.

Each table object:
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

- relations (optional): Array of relation definition objects. CRITICAL: Include relations here, not in separate update_tables call. Do NOT include FK columns in columns array

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
  total: number;
  succeeded: number;
  failed: number;
  results: Array<{success: boolean; tableName: string; tableId?: number; error?: string; message?: string}>;
  errors?: Array<{index: number; tableName: string; error: string; message: string}>;
  reloadAdminUI: boolean;
  summary: string;
  message: string;
}

Example for single table:
create_tables({"tables":[{"name":"categories","columns":[{"name":"id","type":"int","isPrimary":true,"isGenerated":true},{"name":"name","type":"varchar"}]}]})

Example for multiple tables:
create_tables({
  "tables": [
    {"name": "categories", "columns": [{"name": "id", "type": "int", "isPrimary": true, "isGenerated": true}, {"name": "name", "type": "varchar"}]},
    {"name": "products", "columns": [{"name": "id", "type": "int", "isPrimary": true, "isGenerated": true}, {"name": "name", "type": "varchar"}]}
  ]
})

Detailed workflows and validation rules are provided in the "RELEVANT WORKFLOWS & RULES" section of your system prompt if available.`,
    parameters: {
      type: 'object',
      properties: {
        tables: {
          type: 'array',
          items: {
            type: 'object',
            properties: {
              name: { type: 'string' },
              description: { type: 'string' },
              columns: { type: 'array', items: { type: 'object' } },
              relations: { type: 'array', items: { type: 'object' } },
              uniques: { type: 'array', items: { type: 'array', items: { type: 'string' } } },
              indexes: { type: 'array', items: { type: 'object' } },
            },
            required: ['name', 'columns'],
          },
        },
      },
      required: ['tables'],
    },
  },
  {
    name: 'update_tables',
    description: `Purpose → Update one or multiple existing tables. Processes tables sequentially internally to avoid deadlocks.

System Table Safety:
- Do not remove/edit built-in columns/relations in system tables (*_definition tables)
- Only add new columns/relations when extending system tables
- createdAt/updatedAt are auto-generated; do not include in columns array

Inputs:
- tables (required): Array of table update objects. For single table, use array with 1 element.

Each table object:
- tableName (required): Table name to update (must exist)
- tableId (optional): Table ID for faster lookup
- description (optional): New table description
- columns (optional): Array of column definition objects to add/update. Merged by name. CRITICAL: Do NOT include createdAt/updatedAt. Do NOT include FK columns

Column Schema (same as create_tables):
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

Relation Schema (same as create_tables):
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
  total: number;
  succeeded: number;
  failed: number;
  results: Array<{success: boolean; tableName: string; tableId?: number; updated?: string; error?: string; message?: string}>;
  errors?: Array<{index: number; tableName?: string; tableId?: number; error: string; message: string}>;
  reloadAdminUI: boolean;
  summary: string;
  message: string;
}

Example for single table:
update_tables({
  "tables": [{
    "tableName": "product",
    "columns": [{"name":"stock","type":"int"}],
    "relations": [{"propertyName":"supplier","type":"many-to-one","targetTable":{"id":2}}]
  }]
})

Example for multiple tables:
update_tables({
  "tables": [
    {"tableName": "categories", "description": "Updated description"},
    {"tableName": "products", "columns": [{"name": "price", "type": "float"}]}
  ]
})

Detailed workflows are provided in the "RELEVANT WORKFLOWS & RULES" section of your system prompt if available.`,
    parameters: {
      type: 'object',
      properties: {
        tables: {
          type: 'array',
          items: {
            type: 'object',
            properties: {
              tableId: { type: 'number' },
              tableName: { type: 'string' },
              description: { type: 'string' },
              columns: { type: 'array', items: { type: 'object' } },
              relations: { type: 'array', items: { type: 'object' } },
              uniques: { type: 'array', items: { type: 'array', items: { type: 'string' } } },
              indexes: { type: 'array', items: { type: 'object' } },
            },
            required: ['tableName'],
          },
        },
      },
      required: ['tables'],
    },
  },
  {
    name: 'delete_tables',
    description: `Purpose → Delete one or multiple tables by their IDs. This tool permanently removes the table structure and all its data. Processes tables sequentially internally.

CRITICAL - Find Table IDs First:
- This tool ONLY accepts table IDs (array of numbers), NOT table names
- BEFORE calling this tool, you MUST find the table IDs first using one of these methods:
  1. Using find_records: find_records({"table":"table_definition","where":{"name":{"_eq":"table_name"}},"fields":"id,name","limit":1})
  2. Using get_table_details: {"tableName":["table_name"],"getData":false} (check the id field in result)
- Extract the id(s) (number) from the result and use them in this tool
- NEVER use table name as id - this tool only accepts numeric ids

Workflow for single table:
1. Find table ID: find_records({"table":"table_definition","where":{"name":{"_eq":"table_name"}},"fields":"id,name","limit":1})
2. Get id from result (e.g., id: 123)
3. Delete table: delete_tables({"ids":[123]})

Workflow for multiple tables:
1. Find all table IDs: find_records({"table":"table_definition","where":{"name":{"_in":["table1","table2"]}},"fields":"id,name","limit":0})
2. Extract ids from result (e.g., [123, 124])
3. Delete tables: delete_tables({"ids":[123,124]})

Example:
Step 1: find_records({"table":"table_definition","where":{"name":{"_eq":"categories"}},"fields":"id,name","limit":1})
Step 2: delete_tables({"ids":[123]}) ← Use the id from step 1 (single table: array with 1 element)

For multiple tables:
Step 1: find_records({"table":"table_definition","where":{"name":{"_in":["categories","products"]}},"fields":"id,name","limit":0})
Step 2: delete_tables({"ids":[123,124]}) ← Use all ids from step 1

Inputs:
- ids (required): Array of table IDs (numbers) from table_definition. Must be found first using find_records or get_table_details. For single table, use array with 1 element: [123].

Output:
{
  success: boolean;
  total: number;
  succeeded: number;
  failed: number;
  results: Array<{success: boolean; id: number; name?: string; error?: string; message?: string}>;
  errors?: Array<{index: number; id: number; error: string; message: string}>;
  summary: string;
  message: string;
  reloadAdminUI: boolean;
}`,
    parameters: {
      type: 'object',
      properties: {
        ids: {
          type: 'array',
          items: { type: 'number' },
          description: 'Array of table IDs (numbers) from table_definition. Must be found first using find_records({"table":"table_definition","where":{"name":{"_eq":"table_name"}},"fields":"id,name","limit":1}) or get_table_details. For single table, use array with 1 element: [123].',
        },
      },
      required: ['ids'],
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
- create_tables: Creating new tables
- update_tables: Updating existing tables
- delete_tables: Deleting tables
- custom: Other custom tasks

Task status flow:
- pending → in_progress → completed/failed/cancelled

Inputs:
- conversationId (required): Current conversation ID
- type (required): Task type (create_tables|update_tables|delete_tables|custom)
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
    type: "create_tables" | "update_tables" | "delete_tables" | "custom";
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
          enum: ['create_tables', 'update_tables', 'delete_tables', 'custom'],
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

CRITICAL - NEVER Select All, ALWAYS Filter and Specify Fields:
- **NEVER use "*" or omit fields parameter** - ALWAYS specify minimal fields needed (e.g., "id", "id,name", "id,name,email")
- **NEVER call without where/filter** - ALWAYS specify conditions to limit results (e.g., where={"isSystem":{"_eq":false}}, where={"price":{"_gt":100}})
- **NEVER query all records without filter** - ALWAYS use where parameter to filter results
- Examples:
  * WRONG: find_records({"table":"table_definition","fields":"*"}) → Returns all fields, wastes tokens
  * WRONG: find_records({"table":"product"}) → No filter, no fields, returns all products with all fields
  * CORRECT: find_records({"table":"table_definition","where":{"isSystem":{"_eq":false}},"fields":"id,name","limit":0})
  * CORRECT: find_records({"table":"product","where":{"price":{"_gt":100}},"fields":"id,name,price","limit":10})

Permission: Checked automatically for business tables. Operation will fail with clear error if permission is denied.

CRITICAL - Field Names Check:
- BEFORE using fields parameter: You MUST call get_table_details or get_fields FIRST to verify field names
- Field names must match exactly (snake_case vs camelCase)
- Use get_fields for lightweight field list, or get_table_details for full schema

Basic examples:
- Find records: find_records({"table":"order","where":{"status":{"_eq":"pending"}},"fields":"id,total","limit":10})
- Find with filter: find_records({"table":"product","where":{"price":{"_gt":100}},"fields":"id,name,price","limit":10})
- Find single record: find_records({"table":"customer","where":{"id":{"_eq":1}},"fields":"id,name,email","limit":1})

Metadata tables (*_definition) can skip permission:
- find_records({"table":"table_definition","where":{"isSystem":{"_eq":false}},"fields":"id,name","skipPermissionCheck":true,"limit":0})

Detailed workflows and best practices are provided in the "RELEVANT WORKFLOWS & RULES" section of your system prompt if available.`,
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
            'REQUIRED. Fields to return. Use get_fields for available fields, then specify ONLY needed fields (e.g., "id" for count, "id,name" for list). Supports wildcards like "columns.*", "relations.*". CRITICAL: NEVER use "*" or omit fields parameter - this returns all fields and wastes tokens. ALWAYS specify minimal fields needed.',
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
      required: ['table', 'fields'],
    },
  },
  {
    name: 'count_records',
    description: `Purpose → Count records in a table (with or without filter).

CRITICAL - NEVER Select All, ALWAYS Specify Fields:
- **NEVER use "*" or omit fields parameter** - ALWAYS specify minimal fields (e.g., "id")
- **ALWAYS use where parameter when possible** - Even for "count all", prefer to add a filter if user specified criteria
- Examples:
  * WRONG: count_records({"table":"table_definition","meta":"totalCount"}) → No fields specified
  * CORRECT: count_records({"table":"table_definition","fields":"id","meta":"totalCount"})
  * CORRECT: count_records({"table":"table_definition","where":{"isSystem":{"_eq":false}},"fields":"id","meta":"filterCount"})

Permission: Checked automatically for business tables. Operation will fail with clear error if permission is denied.

CRITICAL - COUNT Queries:
- To count TOTAL number of records in a table (no filter):
  * Use: where=null (or omit), meta="totalCount", fields="id"
  * Read the totalCount value from the response metadata
- To count records WITH a filter (e.g., "how many tables have isSystem=true?"):
  * Use: where={filter conditions}, meta="filterCount", fields="id"
  * Read the filterCount value from the response metadata
- NEVER use limit=0 just to count - always use limit=1 with appropriate meta parameter

Basic examples:
- Count all records: count_records({"table":"product","fields":"id","meta":"totalCount"})
- Count with filter: count_records({"table":"table_definition","where":{"isSystem":{"_eq":false}},"fields":"id","meta":"filterCount"})

Metadata tables (*_definition) can skip permission:
- count_records({"table":"table_definition","where":{"isSystem":{"_eq":false}},"fields":"id","meta":"filterCount","skipPermissionCheck":true})

**Important Field Definitions for Metadata Tables:**
- isSystem (boolean, in table_definition): If true, this is a system table (e.g., table_definition, column_definition, relation_definition, route_definition). System tables are critical for system operation and should NOT be deleted or heavily modified. If false, this is a user-created table that can be safely deleted. Use this field to filter: where={"isSystem":{"_eq":false}} to find only user-created tables.
- isNullable (boolean, in column_definition): If true, this column can contain NULL values. If false, this column is REQUIRED (NOT NULL constraint).
- isPrimary (boolean, in column_definition): If true, this column is the primary key. Each table MUST have exactly one column with isPrimary=true.
- isGenerated (boolean, in column_definition): If true, this column value is auto-generated by the database. You should NOT provide values for generated columns when creating records.

Detailed workflows and best practices are provided in the "RELEVANT WORKFLOWS & RULES" section of your system prompt if available.`,
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
        fields: {
          type: 'string',
          description:
            'REQUIRED. Fields to return. Use minimal fields like "id" - do NOT use "*" or omit fields.',
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
      required: ['table', 'meta', 'fields'],
    },
  },
  {
    name: 'create_records',
    description: `Purpose → Create one or multiple records in a table. Processes records sequentially internally.

Permission: Checked automatically for business tables. Operation will fail with clear error if permission is denied.

CRITICAL - Schema Check Required:
- BEFORE create operations: You MUST call get_table_details FIRST to check the table schema
- Schema check is MANDATORY to ensure:
  * All required fields (not-null, non-generated) are included in data
  * Column names match exactly (snake_case vs camelCase)
  * Relations format is correct (propertyName, not FK column names)
  * Data types match column types
- Workflow: get_table_details → analyze schema → prepare data → create_records
- DO NOT skip schema check - missing required fields will cause errors

CRITICAL - Check Unique Constraints Before Create:
- BEFORE create operations: You MUST check if records with unique field values already exist
- For tables with unique constraints (e.g., name, email, slug), check existence FIRST using find_records
- Workflow: get_table_details (to identify unique columns) → find_records (to check existence) → create only if not exists
- Example: Before creating category with name="Electronics", check: find_records({"table":"category","where":{"name":{"_eq":"Electronics"}},"fields":"id","limit":1})
- If record exists, skip creation or use update_records instead - duplicate unique values will cause errors
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

Inputs:
- table (required): Name of the table to create records in
- dataArray (required): Array of data objects. For single record, use array with 1 element. CRITICAL: DO NOT include "id" field in any data object.
- fields (optional): Fields to return. ALWAYS specify minimal fields (e.g., "id" or "id,name") to save tokens.

Output:
{
  success: boolean;
  total: number;
  succeeded: number;
  failed: number;
  results: Array<{success: boolean; index: number; id?: number; error?: string; message?: string}>;
  errors?: Array<{index: number; error: string; message: string; data?: any}>;
  summary: string;
  message: string;
}

Example for single record:
create_records({"table":"product","dataArray":[{"name":"Product 1","price":100}],"fields":"id"})

Example for multiple records:
create_records({"table":"product","dataArray":[{"name":"Product 1","price":100},{"name":"Product 2","price":200}],"fields":"id"})

Detailed workflows and best practices are provided in the "RELEVANT WORKFLOWS & RULES" section of your system prompt if available.`,
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
          description: 'Array of data objects for new records. CRITICAL: DO NOT include "id" field in any data object.',
        },
        fields: {
          type: 'string',
          description:
            'Fields to return. ALWAYS specify minimal fields (e.g., "id" or "id,name") to save tokens. Do NOT use "*" or omit fields parameter - this returns all fields and wastes tokens.',
        },
      },
      required: ['table', 'dataArray'],
    },
  },
  {
    name: 'update_records',
    description: `Purpose → Update one or multiple records by ID. Processes records sequentially internally.

Permission: Checked automatically for business tables. Operation will fail with clear error if permission is denied.

CRITICAL - Schema Check Required:
- BEFORE update operations: You MUST call get_table_details FIRST to check the table schema
- Schema check is MANDATORY to ensure:
  * Column names match exactly (snake_case vs camelCase)
  * Relations format is correct (propertyName, not FK column names)
  * Data types match column types
- Workflow: get_table_details → analyze schema → prepare data → update_records
- DO NOT skip schema check - invalid field names or types will cause errors

CRITICAL - Check Record Exists Before Update:
- BEFORE update operations: You MUST check if the record exists using find_records
- Workflow: find_records (to verify record exists) → update_records
- Example: Before updating customer with id=1, check: find_records({"table":"customer","where":{"id":{"_eq":1}},"fields":"id","limit":1})
- If record does not exist, skip update or report error

CRITICAL - Relations Format:
- For relations (many-to-one, one-to-one): Use propertyName from get_table_details result.relations[] with the ID value directly
- Format: {"propertyName": <id_value>} where propertyName is from relations[].propertyName and id_value is a number or string
- Example: If get_table_details shows relation with propertyName="customer", use {"customer": 1} NOT {"customer_id": 1} or {"customerId": 1}
- The system automatically converts propertyName to the correct FK column - you MUST use propertyName, never FK column names
- Check get_table_details result.relations[] to see available propertyName values and their foreignKeyColumn (for reference only, do not use foreignKeyColumn directly)

Inputs:
- table (required): Name of the table to update records in
- updates (required): Array of update objects. For single record, use array with 1 element. Each object: {id: number|string, data: object}
- fields (optional): Fields to return. ALWAYS specify minimal fields (e.g., "id" or "id,name") to save tokens.

Output:
{
  success: boolean;
  total: number;
  succeeded: number;
  failed: number;
  results: Array<{success: boolean; index: number; id?: number|string; error?: string; message?: string}>;
  errors?: Array<{index: number; id?: number|string; error: string; message: string}>;
  summary: string;
  message: string;
}

Example for single record:
update_records({"table":"customer","updates":[{"id":1,"data":{"name":"New Name"}}],"fields":"id"})

Example for multiple records:
update_records({"table":"customer","updates":[{"id":1,"data":{"name":"New Name 1"}},{"id":2,"data":{"name":"New Name 2"}}],"fields":"id"})

Detailed workflows and best practices are provided in the "RELEVANT WORKFLOWS & RULES" section of your system prompt if available.`,
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
                oneOf: [{ type: 'string' }, { type: 'number' }],
              },
              data: {
                type: 'object',
              },
            },
            required: ['id', 'data'],
          },
          description: 'Array of update objects. Each object: {id: number|string, data: object}',
        },
        fields: {
          type: 'string',
          description:
            'Fields to return. ALWAYS specify minimal fields (e.g., "id" or "id,name") to save tokens. Do NOT use "*" or omit fields parameter - this returns all fields and wastes tokens.',
        },
      },
      required: ['table', 'updates'],
    },
  },
  {
    name: 'delete_records',
    description: `Purpose → Delete one or multiple records by ID. Processes records sequentially internally.

Permission: Checked automatically for business tables. Operation will fail with clear error if permission is denied.

CRITICAL - Check Record Exists Before Delete:
- BEFORE delete operations: You MUST check if the record exists using find_records
- Workflow: find_records (to verify record exists) → delete_records
- Example: Before deleting order with id=1, check: find_records({"table":"order","where":{"id":{"_eq":1}},"fields":"id","limit":1})
- If record does not exist, skip delete or report error

CRITICAL - Deleting Tables (NOT Data):
- To DELETE/DROP/REMOVE a TABLE (not data records), you MUST use the delete_tables tool:
  1. Find the table_definition record: find_records({"table":"table_definition","where":{"name":{"_eq":"table_name"}},"fields":"id,name","limit":1})
  2. Get the id (number) from the result
  3. Delete the table using delete_tables tool: delete_tables({"ids":[<id_from_step_1>]})
- NEVER use delete_records to delete tables - use delete_tables tool instead
- NEVER use delete_records on data tables (categories, products, etc.) to delete tables - this only deletes data records, not the table itself
- NEVER use table name as id value - delete_tables only accepts numeric ids in array
- Example CORRECT for deleting table: 
  Step 1: find_records({"table":"table_definition","where":{"name":{"_eq":"categories"}},"fields":"id,name","limit":1})
  Step 2: delete_tables({"ids":[123]}) ← Use the id from step 1 (single table: array with 1 element)
- Example WRONG: delete_records({"table":"categories","ids":["categories"]}) ← This is INCORRECT

Inputs:
- table (required): Name of the table to delete records from
- ids (required): Array of record IDs. For single record, use array with 1 element.

Output:
{
  success: boolean;
  total: number;
  succeeded: number;
  failed: number;
  results: Array<{success: boolean; index: number; id?: number|string; error?: string; message?: string}>;
  errors?: Array<{index: number; id?: number|string; error: string; message: string}>;
  summary: string;
  message: string;
}

Example for single record:
delete_records({"table":"order_item","ids":[1]})

Example for multiple records:
delete_records({"table":"order_item","ids":[1,2,3]})

Detailed workflows and best practices are provided in the "RELEVANT WORKFLOWS & RULES" section of your system prompt if available.`,
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
            oneOf: [{ type: 'string' }, { type: 'number' }],
          },
          description: 'Array of record IDs to delete.',
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

