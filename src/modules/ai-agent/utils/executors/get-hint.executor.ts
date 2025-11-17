import { Logger } from '@nestjs/common';
import { QueryBuilderService } from '../../../../infrastructure/query-builder/query-builder.service';
import { TDynamicContext } from '../../../../shared/interfaces/dynamic-context.interface';

const logger = new Logger('GetHintExecutor');

export interface GetHintExecutorDependencies {
  queryBuilder: QueryBuilderService;
}

export interface HintContent {
  category: string;
  title: string;
  content: string;
  tools?: string[];
}

export function buildHintContent(dbType: string, idFieldName: string, categories?: string[]): HintContent[] {
  const isMongoDB = dbType === 'mongodb';

  const allHints: HintContent[] = [];

  const dbTypeContent = `Database context:
- Engine: ${dbType}
- ID field: ${isMongoDB ? '"_id"' : '"id"'}
- New table ID type → ${isMongoDB ? '"uuid" (REQUIRED for MongoDB)' : '"int" (PREFERRED for SQL, auto increment) or "uuid"'}
- CRITICAL: For SQL databases, ALWAYS use type="int" for id column unless you have a specific reason to use uuid
- CRITICAL: For MongoDB, you MUST use type="uuid" for _id column
- Relation payload → {${isMongoDB ? '"_id"' : '"id"'}: value}`;

  const dbTypeHint: HintContent = {
    category: 'database_type',
    title: 'Database Type Information',
    content: dbTypeContent,
    tools: [],
  };

  const fieldOptContent = `**Field Selection & Query Optimization**

**MANDATORY WORKFLOW - Field Validation Before Query:**
Step 1: ALWAYS call get_table_details FIRST before ANY query
get_table_details({"tableName": ["product"]})
→ WAIT for result - DO NOT proceed until you have the schema

Step 2: Extract field names from schema
- Look at result.columns[].name to see ALL available column names
- Look at result.relations[].propertyName to see available relation properties
- Use ONLY field names that exist in result.columns[].name
- For relations, use propertyName from result.relations[].propertyName

Step 3: Use verified fields ONLY in your query
- Use ONLY fields you verified exist in Step 2
- NEVER guess or invent field names

**Basic Rules:**
- **MANDATORY**: ALWAYS call get_table_details BEFORE querying to know available fields
- **MANDATORY**: WAIT for get_table_details result before proceeding
- **FORBIDDEN**: NEVER guess field names - always verify they exist in schema
- get_table_details supports single or multiple tables: {"tableName": "post"} or {"tableName": ["post", "category"]}
- For multiple tables, use array format to get all schemas in ONE call: {"tableName": ["table1", "table2", "table3"]}

**Field Parameter Examples:**
Read operations (after verifying fields in schema):
Step 1: get_table_details({"tableName": ["product"]}) → WAIT
Step 2: Verify "name", "price" exist in result.columns[].name
Step 3: Query with verified fields:
- List with names: {"fields": "${idFieldName},name", "limit": 10}
- Count records: {"fields": "${idFieldName}", "limit": 1, "meta": "totalCount"}
- With relations: {"fields": "${idFieldName},name,category.name"} (verify "category" exists in result.relations[].propertyName)
- Multiple relations: {"fields": "${idFieldName},order.customer.name,order.items.product.name"}

Write operations (always specify minimal fields):
- After create: {"fields": "${idFieldName}"}
- After update: {"fields": "${idFieldName},name"}

**Limit Parameter:**
- limit=0: Fetch ALL records (use when user wants "all records")
- limit>0: Fetch specified number (default: 10)
- For COUNT: limit=1 with meta="totalCount" (no filter) or meta="filterCount" (with filter)
- IMPORTANT: If you call find with limit=10 and get results, DO NOT call again with limit=20 - reuse result or use limit=0 from start

**COUNT Query Examples:**
Count total records: count_records({"table":"product","fields":"${idFieldName}","meta":"totalCount"})
→ Read totalCount from response metadata

Count with filter: count_records({"table":"product","fields":"${idFieldName}","where":{"price":{"_gt":100}},"meta":"filterCount"})
→ Read filterCount from response metadata

**CRITICAL - Schema Check Before Create/Update:**
1. Call get_table_details FIRST: get_table_details({"tableName":["product"]})
2. Check required fields: Look for isNullable=false AND no default value
3. Common required: id (auto), createdAt/updatedAt (auto), but ALWAYS check for others (slug, stock, etc.)
4. Prepare data with ALL required fields
5. If you get constraint errors → call get_table_details again to see all required fields

**CRITICAL - Check Unique Constraints Before Create:**
1. Call get_table_details to see which columns have isUnique=true
2. For unique fields (name, email, slug), check existence FIRST:
   Example: find_records({"table":"category","where":{"name":{"_eq":"Electronics"}},"fields":"${idFieldName}","limit":1})
3. If record exists → skip creation or use update instead
4. For batch_create: Check all records first, filter out existing ones

**Relations Format Examples:**
- {"category": 19} (simple ID - preferred)
- {"category": {"id": 19}} (object format - also works)
- {"customer": 1, "category": 19} (multiple relations)
- Always use relation propertyName, not FK columns or camelCase variants, and always reference related records by ID

**Nested Query Examples:**
- {"table":"order","operation":"find","fields":"${idFieldName},total,customer.name","where":{"customer":{"name":{"_eq":"John"}}}}
- {"table":"route_definition","operation":"find","fields":"${idFieldName},path,roles.name","where":{"roles":{"name":{"_eq":"Admin"}}}}`;

  const fieldOptHint: HintContent = {
    category: 'field_optimization',
    title: 'Field & Query Optimization',
    content: fieldOptContent,
    tools: ['get_table_details', 'get_fields', 'find_records', 'count_records'],
  };

  const tableSchemaOpsContent = `**Table Schema Operations (Create & Update) - Complete Workflow**

**WORKFLOW FOR CREATING TABLES WITHOUT RELATIONS:**
Step 1: Check if table exists
find_records({"table":"table_definition","where":{"name":{"_eq":"products"}},"fields":"${idFieldName},name","limit":1})

Step 2: If not exists, create table
create_tables({
  "tables": [{
    "name": "products",
    "description": "Product catalog",
    "columns": [
      {"name": "${idFieldName}", "type": "${isMongoDB ? 'uuid' : 'int'}", "isPrimary": true, "isGenerated": true},
      {"name": "name", "type": "varchar", "isNullable": false},
      {"name": "price", "type": "float", "isNullable": false}
    ]
  }]
})

**WORKFLOW FOR CREATING TABLES WITH RELATIONS (CRITICAL - FOLLOW EXACTLY):**
Step 1: Check if main table exists
find_records({"table":"table_definition","where":{"name":{"_eq":"products"}},"fields":"${idFieldName},name","limit":1})

Step 2: For EACH relation, find target table ID FIRST (MANDATORY)
1. Find target table ID: find_records({"table":"table_definition","where":{"name":{"_eq":"categories"}},"fields":"${idFieldName},name","limit":1})
2. Verify result: Check that result.data[0].${idFieldName} exists and is a valid number
3. Use the ID from result: targetTable: {"id": result.data[0].${idFieldName}}

Step 3: Create table with relations
create_tables({
  "tables": [{
    "name": "products",
    "columns": [
      {"name": "${idFieldName}", "type": "${isMongoDB ? 'uuid' : 'int'}", "isPrimary": true, "isGenerated": true},
      {"name": "name", "type": "varchar", "isNullable": false}
    ],
    "relations": [{
      "propertyName": "category",
      "type": "many-to-one",
      "targetTable": {"id": 19}
    }]
  }]
})

Critical errors to avoid:
- Using hardcoded ID without verification → Always use find_records to get actual ID
- Not finding target table ID first → Always use find_records to get actual ID
- Using target table name instead of ID → Must use {"id": number}
- Creating table with relation before target table exists → Create target table FIRST

**WORKFLOW FOR MULTI-TABLE CREATION WITH RELATIONS:**
Step 1: Create all base tables FIRST (without relations)
create_tables({
  "tables": [
    {"name": "categories", "columns": [...]},
    {"name": "instructors", "columns": [...]}
  ]
})

Step 2: Find IDs of created tables
find_records({"table":"table_definition","where":{"name":{"_in":["categories","instructors"]}},"fields":"${idFieldName},name","limit":0})

Step 3: Create dependent tables with relations using IDs from Step 2
create_tables({
  "tables": [{
    "name": "courses",
    "columns": [...],
    "relations": [
      {"propertyName": "category", "type": "many-to-one", "targetTable": {"id": <ID from categories>}},
      {"propertyName": "instructor", "type": "many-to-one", "targetTable": {"id": <ID from instructors>}}
    ]
  }]
})

**WORKFLOW FOR UPDATING TABLES:**
Step 1: Find table (if needed)
find_records({"table":"table_definition","where":{"name":{"_eq":"products"}},"fields":"${idFieldName},name","limit":1})

Step 2: Update table schema
update_tables({
  "tables": [{
    "tableName": "products",
    "columns": [{"name": "stock", "type": "int", "isNullable": true, "default": 0}]
  }]
})

Add relation (MUST find target table ID first):
1. Find target table ID: find_records({"table":"table_definition","where":{"name":{"_eq":"categories"}},"fields":"${idFieldName}","limit":1})
2. Verify ID is valid (exists in result)
3. Add relation: update_tables({
  "tables": [{
    "tableName": "products",
    "relations": [{
      "propertyName": "category",
      "type": "many-to-one",
      "targetTable": {"id": <ID from find_records result>}
    }]
  }]
})

**MULTIPLE TABLES:**
create_tables({
  "tables": [
    {"name": "products", "columns": [...]},
    {"name": "categories", "columns": [...]}
  ]
})

update_tables({
  "tables": [
    {"tableName": "products", "columns": [...]},
    {"tableName": "categories", "columns": [...]}
  ]
})

**CRITICAL RULES:**
- Always check table existence before create
- Always include ${idFieldName} column with correct type
- **CRITICAL - NEVER include createdAt/updatedAt columns:**
  * createdAt and updatedAt are ALWAYS auto-generated by the system
  * DO NOT include them in the columns array - this will cause validation errors
  * If you see these fields in schema, IGNORE them when creating/updating tables
  * Example: If schema shows "createdAt" and "updatedAt", do NOT add them to columns
- Use relations array for foreign keys, not FK columns
- For relations: ALWAYS find target table ID first using find_records
- Never use hardcoded IDs without verification
- Create target tables BEFORE creating tables that reference them
- Batch tools process sequentially internally`;

  const tableSchemaOpsHint: HintContent = {
    category: 'table_schema_operations',
    title: 'Table Schema Operations (Create & Update)',
    content: tableSchemaOpsContent,
    tools: ['find_records', 'create_tables', 'update_tables'],
  };

  const tableDeletionContent = `**Table Deletion - Complete Workflow**

**WORKFLOW FOR DELETING TABLES:**
Step 1: Find table ID
find_records({"table":"table_definition","where":{"name":{"_eq":"products"}},"fields":"${idFieldName},name","limit":1})

Step 2: Delete table(s)
delete_tables({"ids": [19]})

**MULTIPLE TABLES:**
1. Find all table IDs: find_records({"table":"table_definition","where":{"name":{"_in":["products","categories"]}},"fields":"${idFieldName},name","limit":0})
2. Delete all: delete_tables({"ids": [19, 20]})

**CRITICAL RULES:**
- ALWAYS find table ID first (cannot use table name)
- Use delete_tables for table structure, NOT delete_records (which deletes data)
- Tool processes tables sequentially internally (one by one)
- For single table, use array with 1 element: delete_tables({"ids":[123]})
- NEVER use delete_records for table_definition table

**Common mistakes to avoid:**
- delete_records deletes data and should not be used for table structure
- delete_tables requires numeric IDs, not table names
- Never target table_definition with delete_records`;

  const tableDeletionHint: HintContent = {
    category: 'table_deletion',
    title: 'Table Deletion Operations',
    content: tableDeletionContent,
    tools: ['find_records', 'delete_tables'],
  };

  const crudWriteOpsContent = `**CRUD Write Operations (Create & Update Records) - Complete Workflow**

**WORKFLOW FOR CREATING RECORDS:**
Step 1: Get schema
get_table_details({"tableName": ["product"]})

Step 2: Check unique constraints (if any)
find_records({"table":"product","where":{"name":{"_eq":"Laptop"}},"fields":"${idFieldName}","limit":1})

Step 3: Prepare data with ALL required fields (EXCLUDE auto-generated fields)
- DO NOT include ${idFieldName} (auto-generated)
- DO NOT include createdAt (auto-generated)
- DO NOT include updatedAt (auto-generated)
- Only include user-defined fields from schema
{
  "name": "Laptop",
  "price": 999.99,
  "category": 19
}

Step 4: Create record
create_records({
  "table": "product",
  "dataArray": [{"name": "Laptop", "price": 999.99, "category": 19}],
  "fields": "${idFieldName}"
})

**WORKFLOW FOR UPDATING RECORDS:**
Step 1: Get schema
get_table_details({"tableName": ["product"]})

Step 2: Check if record exists
find_records({"table":"product","where":{"${idFieldName}":{"_eq":1}},"fields":"${idFieldName}","limit":1})

Step 3: Update
update_records({
  "table": "product",
  "updates": [{"id": 1, "data": {"price": 899.99}}],
  "fields": "${idFieldName}"
})

**MULTIPLE RECORDS:**
create_records({
  "table": "product",
  "dataArray": [
    {"name": "Laptop", "price": 999.99},
    {"name": "Mouse", "price": 29.99}
  ],
  "fields": "${idFieldName}"
})

update_records({
  "table": "product",
  "updates": [
    {"id": 1, "data": {"price": 899.99}},
    {"id": 2, "data": {"price": 24.99}}
  ],
  "fields": "${idFieldName}"
})

**CRITICAL RULES:**
- ALWAYS call get_table_details FIRST to check required fields
- ALWAYS check unique constraints before create
- Use propertyName from relations (e.g., "category": 19), NOT FK columns
- **CRITICAL - NEVER include auto-generated fields in create/update data:**
  * DO NOT include ${idFieldName} (auto-generated)
  * DO NOT include createdAt (auto-generated)
  * DO NOT include updatedAt (auto-generated)
  * These fields are managed by the system - including them will cause validation errors
- Batch tools process sequentially internally and report detailed results`;

  const crudWriteOpsHint: HintContent = {
    category: 'crud_write_operations',
    title: 'CRUD Write Operations (Create & Update Records)',
    content: crudWriteOpsContent,
    tools: ['get_table_details', 'find_records', 'create_records', 'update_records'],
  };

  const crudDeleteOpsContent = `**CRUD Delete Operations - Complete Workflow**

**WORKFLOW FOR DELETING RECORDS:**
Step 1: Verify record exists
find_records({"table":"product","where":{"${idFieldName}":{"_eq":1}},"fields":"${idFieldName}","limit":1})

Step 2: Delete record
delete_records({
  "table": "product",
  "ids": [1]
})

**BATCH DELETION (2+ records):**
delete_records({
  "table": "product",
  "ids": [1, 2, 3]
})

**CRITICAL RULES:**
- ALWAYS verify record exists before delete
- Use delete_records for DATA records, NOT for table structure
- For table deletion, use delete_tables tool instead
- Batch tool processes sequentially and reports detailed results

**Common mistakes to avoid:**
- Use delete_tables for schema changes; delete_records only removes data
- Always verify record existence before deletion
- Never run delete_records on table_definition`;

  const crudDeleteOpsHint: HintContent = {
    category: 'crud_delete_operations',
    title: 'CRUD Delete Operations',
    content: crudDeleteOpsContent,
    tools: ['find_records', 'delete_records'],
  };

  const crudQueryOpsContent = `**CRUD Query Operations (Find & Count) - Complete Workflow**

**WORKFLOW FOR FINDING RECORDS (WHEN TABLE NAME IS KNOWN):**
Step 1: Get table schema FIRST (MANDATORY - DO NOT SKIP THIS STEP)
get_table_details({"tableName": ["product"]})
→ WAIT for result, DO NOT proceed until you have the schema

Step 2: Extract field names from schema result
- Look at result.columns[].name to see ALL available column names
- Look at result.relations[].propertyName to see available relation properties
- Use ONLY field names that exist in schema.columns[].name
- For relations, use propertyName from result.relations[].propertyName
- NEVER guess or invent field names
- NEVER use field names you haven't verified in the schema

Step 3: Query records with verified fields ONLY
find_records({
  "table": "product",
  "fields": "${idFieldName},name,price",
  "where": {"price": {"_gt": 100}},
  "limit": 10,
  "sort": "-price"
})
→ Use ONLY fields that you verified exist in Step 2

**WORKFLOW FOR COUNTING RECORDS:**
Step 1: Get table schema FIRST (MANDATORY)
get_table_details({"tableName": ["product"]})
→ Verify ${idFieldName} field exists in schema

Step 2: Count total records
count_records({
  "table": "product",
  "fields": "${idFieldName}",
  "meta": "totalCount"
})
→ Read totalCount from response metadata

Step 3: Count with filter (verify filter fields exist in schema first)
count_records({
  "table": "product",
  "fields": "${idFieldName}",
  "where": {"price": {"_gt": 100}},
  "meta": "filterCount"
})
→ Read filterCount from response metadata
→ Use ONLY fields in "where" that exist in schema.columns[].name

**ADVANCED QUERIES:**
With relations (verify relation propertyName exists in schema first):
Step 1: get_table_details({"tableName": ["order"]})
Step 2: Check result.relations[].propertyName to find "customer" relation
Step 3: Query with verified relation:
find_records({
  "table": "order",
  "fields": "${idFieldName},total,customer.name",
  "where": {"customer": {"name": {"_eq": "John"}}}
})

Multiple filters with _in operator:
Step 1: get_table_details({"tableName": ["table_definition"]})
Step 2: Verify "name" field exists in schema
Step 3: Query:
find_records({
  "table": "table_definition",
  "where": {"name": {"_in": ["products", "categories"]}},
  "fields": "${idFieldName},name",
  "limit": 0
})

**QUERY OPERATORS (for where parameter in find_records and count_records):**
Comparison: _eq (equals), _neq (not equals), _gt (greater than), _gte (greater than or equal), _lt (less than), _lte (less than or equal)
String: _contains (contains substring, case-insensitive, accent-insensitive), _starts_with (starts with substring, case-insensitive, accent-insensitive), _ends_with (ends with substring, case-insensitive, accent-insensitive), _like (SQL LIKE pattern matching, case-sensitive, supports % and _ wildcards)
Array: _in (value is in array), _not_in (value is not in array)
Range: _between (value is between two values, inclusive, requires array with exactly 2 elements [min, max])
Null: _is_null (field is NULL), _is_not_null (field is not NULL)
Logical: _and (all conditions must be true), _or (at least one condition must be true), _not (condition must be false)

Example with nested _and, _or and multiple operators:
find_records({
  "table": "product",
  "where": {
    "_and": [
      {"price": {"_gte": 100, "_lte": 500}},
      {"_or": [
        {"name": {"_contains": "laptop"}},
        {"name": {"_contains": "computer"}}
      ]},
      {"status": {"_in": ["active", "pending"]}}
    ]
  },
  "fields": "id,name,price",
  "limit": 10
})

Always run get_table_details first; skipping the schema check causes "column does not exist" errors`;

  const crudQueryOpsHint: HintContent = {
    category: 'crud_query_operations',
    title: 'CRUD Query Operations (Find & Count)',
    content: crudQueryOpsContent,
    tools: ['get_table_details', 'get_fields', 'find_records', 'count_records'],
  };

  const systemWorkflowsContent = `**System Workflows (Multi-Step Operations) - Complete Workflow**

**WORKFLOW FOR MULTI-STEP OPERATIONS:**
Step 1: Create task
update_task({
  "conversationId": <conversationId>,
  "type": "create_table",
  "status": "in_progress",
  "data": {"tableNames": ["products", "categories"]}
})

Step 2: Execute operations sequentially
- Create tables: create_tables({...})
- Add data: create_records({...})
- Update relations: update_tables({...})

Step 3: Update task status
update_task({
  "conversationId": <conversationId>,
  "type": "create_table",
  "status": "completed",
  "result": {...}
})

**WORKFLOW FOR SYSTEM SETUP:**
Example: "Create backend system with 5 tables and add data"

1. Create task: update_task({type: "create_tables", status: "in_progress"})
2. Create tables: create_tables({tables: [...]})
3. Get table details: get_table_details({"tableName": [...]})
4. Add sample data: create_records({...})
5. Update task: update_task({status: "completed"})

**CRITICAL RULES:**
- ALWAYS create task FIRST for multi-step operations
- Update task status as you progress
- Execute operations sequentially (one at a time)
- Continue automatically without stopping
- If error occurs, update task with status="failed" and error message

**TASK MANAGEMENT:**
- Start: update_task({status: "in_progress", data: {...}})
- Progress: update_task({status: "in_progress", data: {...updatedData}})
- Complete: update_task({status: "completed", result: {...}})
- Failed: update_task({status: "failed", error: "..."})`;

  const systemWorkflowsHint: HintContent = {
    category: 'system_workflows',
    title: 'System Workflows (Multi-Step Operations)',
    content: systemWorkflowsContent,
    tools: ['update_task', 'create_tables', 'update_tables', 'delete_tables', 'get_table_details', 'create_records', 'update_records'],
  };

  const errorContent = `CRITICAL - Sequential Execution (PREVENTS ERRORS):
- ALWAYS execute tools ONE AT A TIME, step by step
- Do NOT call multiple tools simultaneously in a single response
- Execute first tool → wait for result → analyze → proceed to next
- If you call multiple tools at once and one fails, you'll have to retry all, causing duplicates and wasted tokens
- Example workflow: find_records → wait → delete_records → wait → continue
- This prevents errors, duplicate operations, and ensures proper error handling

Error handling:
- If tool returns error=true → stop workflow and report error to user
- Tools have automatic retry logic - let them handle retries
- Report exact error message from tool result to user
- If you encounter errors after calling multiple tools at once, execute them sequentially instead
- Permission errors: When errorCode="PERMISSION_DENIED", inform user clearly and do NOT retry`;

  const errorHint: HintContent = {
    category: 'error_handling',
    title: 'Error Handling Protocol',
    content: errorContent,
    tools: [],
  };

  const metadataOpsContent = `**Metadata Operations (Table Discovery & Schema) - Complete Workflow**

**WORKFLOW FOR LISTING TABLES:**
Step 1: List all tables
find_records({"table":"table_definition","fields":"name,isSystem","limit":0})
→ Returns array of table names with isSystem field

Step 2: Filter non-system tables
find_records({"table":"table_definition","fields":"name,isSystem","where":{"isSystem":{"_eq":false}},"limit":0})
→ Returns only user-created tables

**WORKFLOW FOR GETTING TABLE DETAILS:**
Step 1: Get single table schema
get_table_details({"tableName": ["product"]})

Step 2: Get multiple table schemas (efficient)
get_table_details({"tableName": ["product", "category", "order"]})

**WORKFLOW FOR GETTING FIELD NAMES:**
Step 1: Get field list
get_fields({"tableName": "product"})
→ Returns array of field names only

**CRITICAL RULES:**
- Use get_table_details with array for multiple tables (ONE call instead of multiple)
- Use get_fields when you only need field names (lighter than get_table_details)

**EXAMPLES:**
- Need schemas for post, category, user → get_table_details({"tableName": ["post", "category", "user_definition"]})`;

  const naturalLanguageDiscoveryContent = `**Natural Language Table Name Discovery - Complete Workflow**

**WORKFLOW FOR NATURAL LANGUAGE QUERIES (GUESSING TABLE NAMES):**
When user asks about resources in natural language (e.g., "show me routes", "list users", "what products are available", "which routes have method post", "what routes exist in the system"), you MUST follow these steps STRICTLY:

**Step 1: Get ALL table names first (MANDATORY - ALWAYS do this first)**
find_records({"table":"table_definition","fields":"name","limit":0})
→ WAIT for result
→ This returns ALL table names in the system (e.g., ["route_definition", "user_definition", "product", "category"])

**Step 2: Guess table name from user query**
- User says "routes" or "route" → Look for table name containing "route" → "route_definition"
- User says "users" or "user" → Look for table name containing "user" → "user_definition" or "users"
- User says "products" or "product" → Look for table name containing "product" → "product" or "products"
- Match user's natural language term to table names from Step 1
- Common patterns: plural/singular forms, with/without "_definition" suffix

**Step 3: Get table schema for guessed table(s) (MANDATORY - DO NOT SKIP)**
get_table_details({"tableName": ["route_definition"]})
→ WAIT for result - DO NOT proceed until you have the schema
→ This returns full schema including columns, relations, and metadata

**After Step 3: Now you have the schema, read the helper for find_records/count_records tools to know how to query**
- The schema shows you available columns (result.columns[].name) and relations (result.relations[].propertyName)
- Use the helper documentation for find_records/count_records to construct your query
- Use ONLY field names that exist in result.columns[].name
- For relations, use propertyName from result.relations[].propertyName

Always run get_table_details first; skipping the schema check causes "column does not exist" errors`;

  const naturalLanguageDiscoveryHint: HintContent = {
    category: 'natural_language_discovery',
    title: 'Natural Language Table Name Discovery',
    content: naturalLanguageDiscoveryContent,
    tools: ['find_records', 'get_table_details'],
  };

  const metadataOpsHint: HintContent = {
    category: 'metadata_operations',
    title: 'Metadata Operations (Table Discovery & Schema)',
    content: metadataOpsContent,
    tools: ['find_records', 'get_table_details', 'get_fields'],
  };

  allHints.push(
    dbTypeHint,
    fieldOptHint,
    tableSchemaOpsHint,
    tableDeletionHint,
    crudWriteOpsHint,
    crudDeleteOpsHint,
    crudQueryOpsHint,
    metadataOpsHint,
    naturalLanguageDiscoveryHint,
    systemWorkflowsHint,
    errorHint
  );

  let filteredHints = allHints;
  if (categories && categories.length > 0) {
    filteredHints = allHints.filter(h => categories.includes(h.category));
  }

  return filteredHints;
}

export function getHintContentString(hints: HintContent[]): string {
  if (hints.length === 0) {
    return '';
  }

  return hints.map(hint => {
    let content = `**${hint.title}**\n\n${hint.content}`;
    if (hint.tools && hint.tools.length > 0) {
      content += `\n\n**Required Tools:** ${hint.tools.join(', ')}`;
    }
    return content;
  }).join('\n\n---\n\n');
}

export function getHintTools(hints: HintContent[]): string[] {
  const toolsSet = new Set<string>();
  hints.forEach(hint => {
    if (hint.tools) {
      hint.tools.forEach(tool => {
        if (tool !== 'get_hint') {
          toolsSet.add(tool);
        }
      });
    }
  });
  return Array.from(toolsSet);
}

export async function executeGetHint(
  args: { category?: string | string[] },
  context: TDynamicContext,
  deps: GetHintExecutorDependencies,
): Promise<any> {
  const { queryBuilder } = deps;
  const dbType = queryBuilder.getDbType();
  const isMongoDB = dbType === 'mongodb';
  const idFieldName = isMongoDB ? '_id' : 'id';

  const categories = args.category ? (Array.isArray(args.category) ? args.category : [args.category]) : undefined;
  const allHints = buildHintContent(dbType, idFieldName);
  const filteredHints = categories ? allHints.filter(h => categories.includes(h.category)) : allHints;


  return {
    dbType,
    isMongoDB,
    idField: idFieldName,
    hints: filteredHints,
    count: filteredHints.length,
    availableCategories: [
      'database_type',
      'field_optimization',
      'table_schema_operations',
      'table_deletion',
      'crud_write_operations',
      'crud_delete_operations',
      'crud_query_operations',
      'metadata_operations',
      'natural_language_discovery',
      'system_workflows',
      'error_handling'
    ],
  };
}

