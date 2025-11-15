import { Logger } from '@nestjs/common';
import { QueryBuilderService } from '../../../../infrastructure/query-builder/query-builder.service';
import { TDynamicContext } from '../../../../shared/interfaces/dynamic-context.interface';

const logger = new Logger('GetHintExecutor');

export interface GetHintExecutorDependencies {
  queryBuilder: QueryBuilderService;
}

export async function executeGetHint(
  args: { category?: string | string[] },
  context: TDynamicContext,
  deps: GetHintExecutorDependencies,
): Promise<any> {
  logger.debug(`[get_hint] Called with category=${JSON.stringify(args.category)}`);
  const { queryBuilder } = deps;
  const dbType = queryBuilder.getDbType();
  const isMongoDB = dbType === 'mongodb';
  const idFieldName = isMongoDB ? '_id' : 'id';

  const allHints = [];

  const dbTypeContent = `Database context:
- Engine: ${dbType}
- ID field: ${isMongoDB ? '"_id"' : '"id"'}
- New table ID type → ${isMongoDB ? '"uuid" (REQUIRED for MongoDB)' : '"int" (PREFERRED for SQL, auto increment) or "uuid"'}
- CRITICAL: For SQL databases, ALWAYS use type="int" for id column unless you have a specific reason to use uuid
- CRITICAL: For MongoDB, you MUST use type="uuid" for _id column
- Relation payload → {${isMongoDB ? '"_id"' : '"id"'}: value}`;

  const dbTypeHint = {
    category: 'database_type',
    title: 'Database Type Information',
    content: dbTypeContent,
  };

  const fieldOptContent = `Field & limit checklist:
- Call get_fields or get_table_details before querying
- get_table_details supports single table (string) or multiple tables (array): {"tableName": "post"} or {"tableName": ["post", "category", "user_definition"]}
- When comparing multiple tables or need schemas for multiple tables, use array format to get all in one call: {"tableName": ["table1", "table2", "table3"]}
- Count queries: fields="${idFieldName}", limit=1, meta="totalCount"
- Name lists: fields="${idFieldName},name", pick limit as needed
- Use limit=0 only when you truly need every row (default limit is 10)
- CRITICAL: For create/update operations, ALWAYS specify minimal fields parameter (e.g., "fields": "${idFieldName}" or "fields": "${idFieldName},name") to save tokens. This is MANDATORY - do NOT omit fields parameter in create/update calls.
- Read operations: Specify only needed fields (e.g., "id,name" for lists, "id" for counts). Supports wildcards like "columns.*", "relations.*".
- Write operations: Always specify minimal fields (e.g., "id" or "id,name") to save tokens. Do NOT use "*" or omit fields parameter.

CRITICAL - Schema Check Before Create/Update:
- BEFORE creating or updating records, you MUST call get_table_details to get the full schema
- Check which columns are required (isNullable=false) and have default values
- Ensure your data object includes ALL required fields (not-null constraints)
- Common required fields: id (auto-generated), createdAt/updatedAt (auto-generated), but ALWAYS check for others like slug, stock, order_number, unit_price, etc.
- If you get constraint errors, you MUST call get_table_details to see all required fields and fix your data

CRITICAL - Check Unique Constraints Before Create:
- BEFORE creating records, you MUST check if records with unique field values already exist
- Use get_table_details to identify which columns have unique constraints (isUnique=true)
- For tables with unique constraints (e.g., name, email, slug), check existence FIRST using dynamic_repository.find
- Workflow: get_table_details (to identify unique columns) → dynamic_repository.find (to check existence) → create only if not exists
- Example: Before creating category with name="Electronics", check: dynamic_repository({"table":"category","operation":"find","where":{"name":{"_eq":"Electronics"}},"fields":"id","limit":1})
- If record exists, skip creation or use update instead - duplicate unique values will cause "duplicate key value violates unique constraint" errors
- For batch_create: Check all records first, filter out existing ones, then create only new records
- This applies to ALL unique constraints: single-column (name) and multi-column (email+username)

Workflow for create/update:
1. Call get_table_details with tableName to get schema (required fields, types, defaults, relations)
2. For relations: Use propertyName from result.relations[] (e.g., "category", "customer"), NOT FK columns (e.g., "category_id", "customerId")
   - Format: {"category": 19} OR {"category": {"id": 19}} (both work, but simple ID is preferred)
   - NEVER use FK column names - system auto-generates them from propertyName
   - Check result.relations[] to see propertyName and foreignKeyColumn (for reference only)
3. Prepare data object with ALL required fields
4. Call dynamic_repository with create/update operation (permission is automatically checked)

Nested relations & query optimization:
- fields → use "relation.field" or "relation.*" (multi-level like "routePermissions.role.name")
- where → nest objects {"roles":{"name":{"_eq":"Admin"}}}
- Prefer one nested query instead of multiple separate calls
- Select only the fields you need (avoid broad "*")

Sample nested query:
{"table":"route_definition","operation":"find","fields":"id,path,roles.name","where":{"roles":{"name":{"_eq":"Admin"}}}}`;

  const fieldOptHint = {
    category: 'field_optimization',
    title: 'Field & Query Optimization',
    content: fieldOptContent,
  };

  const tableOpsContent = `Table operations - use tools for automatic validation & error handling:

Creating tables:
- Use create_table tool (automatically checks existence, validates, handles errors)
- Check if table exists first: {"table":"table_definition","operation":"find","where":{"name":{"_eq":"table_name"}},"fields":"${idFieldName},name","limit":1}
- CRITICAL: Every table MUST have "${idFieldName}" column with isPrimary=true, type="int" (SQL - PREFERRED) or "uuid" (MongoDB - REQUIRED)
- CRITICAL: NEVER include createdAt/updatedAt in columns - system auto-generates them
- Include ALL columns in one create call (excluding createdAt/updatedAt)

Updating tables:
- Use update_table tool (automatically loads current data, validates, merges, checks FK conflicts)
- Columns merged by name, relations merged by propertyName
- System columns (id, createdAt, updatedAt) automatically preserved

Relations:
- CRITICAL: When creating/updating relations, type field is REQUIRED. Must be one of: "one-to-one", "many-to-one", "one-to-many", "many-to-many"
- Format: {"propertyName": "user", "type": "many-to-one", "targetTable": {"id": <REAL_ID>}, "inversePropertyName": "orders"} (for O2M/M2M)
- Use update_table tool to add relations (recommended - handles everything automatically)
- Find target table ID first, then: {"tableName": "post", "relations": [{"propertyName": "categories", "type": "many-to-many", "targetTable": {"id": <REAL_ID>}, "inversePropertyName": "posts"}]}
- Create on ONE side only - system handles inverse automatically
- O2M and M2M MUST include inversePropertyName
- targetTable.id MUST be REAL ID from find result (never use IDs from history)

Batch operations:
- Metadata tables (table_definition): Process sequentially, NO batch operations. CRITICAL: When deleting tables, delete ONE BY ONE sequentially (not batch_delete) to avoid deadlocks
- Data tables: Use batch_delete for 2+ deletes, batch_create/batch_update for 2+ creates/updates
- When find returns multiple records, collect ALL IDs and use batch operations (except table deletion - must be sequential)

Best practices:
- Use get_metadata to discover table names
- Schema changes target *_definition tables only
- Use _in filter to find multiple tables in one call
- Always specify minimal fields parameter to save tokens`;

  const tableOpsHint = {
    category: 'table_operations',
    title: 'Table Creation & Management',
    content: tableOpsContent,
  };

  const complexWorkflowsContent = `Complex workflows - use tools for automatic handling:

Recreate tables with relations:
1. Find existing tables: {"table":"table_definition","operation":"find","where":{"name":{"_in":["post","category"]}},"fields":"${idFieldName},name","limit":0}
2. Delete ONE BY ONE sequentially (not batch_delete) to avoid deadlocks: {"table":"table_definition","operation":"delete","id":<id1>}, then {"table":"table_definition","operation":"delete","id":<id2>}, etc.
3. Use create_table tool to create new tables (validates automatically)
4. Find new table IDs, then use update_table tool to add relations (merges automatically)

Common mistakes:
❌ Creating tables without id column
❌ Including createdAt/updatedAt in columns
❌ Including FK columns (customer_id, customerId, etc.) in columns array - system auto-generates from relations
❌ Updating both sides of relation
❌ Multiple find calls instead of _in filter
❌ Not using batch operations for multiple deletes

Efficiency:
✅ Use _in filter for multiple tables
✅ Use create_table/update_table tools (automatic validation)
✅ Use batch operations for data tables (not metadata tables)`;

  const complexWorkflowsHint = {
    category: 'complex_workflows',
    title: 'Complex Task Workflows',
    content: complexWorkflowsContent,
  };

  const errorContent = `CRITICAL - Sequential Execution (PREVENTS ERRORS):
- ALWAYS execute tools ONE AT A TIME, step by step
- Do NOT call multiple tools simultaneously in a single response
- Execute first tool → wait for result → analyze → proceed to next
- If you call multiple tools at once and one fails, you'll have to retry all, causing duplicates and wasted tokens
- Example workflow: dynamic_repository find → wait → dynamic_repository delete → wait → continue
- This prevents errors, duplicate operations, and ensures proper error handling

Error handling:
- If tool returns error=true → stop workflow and report error to user
- Tools have automatic retry logic - let them handle retries
- Report exact error message from tool result to user
- If you encounter errors after calling multiple tools at once, execute them sequentially instead
- Permission errors: When errorCode="PERMISSION_DENIED", inform user clearly and do NOT retry`;

  const errorHint = {
    category: 'error_handling',
    title: 'Error Handling Protocol',
    content: errorContent,
  };

  const discoveryContent = `Table discovery:
- Never guess table names from user phrasing
- Use get_metadata to list tables and pick the closest match
- Need structure? call get_table_details
- Need multiple table structures? Use get_table_details with array: {"tableName": ["table1", "table2"]}

Examples:
- "route" → get_metadata → choose "route_definition"
- "users" → get_metadata → choose "user_definition"
- Need schemas for post, category, and user → get_table_details with {"tableName": ["post", "category", "user_definition"]}`;

  const discoveryHint = {
    category: 'table_discovery',
    title: 'Table Discovery Rules',
    content: discoveryContent,
  };

  allHints.push(dbTypeHint, fieldOptHint, tableOpsHint, errorHint, discoveryHint, complexWorkflowsHint);

  let filteredHints = allHints;
  if (args.category) {
    const categories = Array.isArray(args.category) ? args.category : [args.category];
    filteredHints = allHints.filter(h => categories.includes(h.category));
    logger.debug(`[get_hint] Filtered to ${filteredHints.length} hints for categories: ${categories.join(', ')}`);
  } else {
    logger.debug(`[get_hint] Returning all ${allHints.length} hints`);
  }

  return {
    dbType,
    isMongoDB,
    idField: idFieldName,
    hints: filteredHints,
    count: filteredHints.length,
    availableCategories: ['database_type', 'field_optimization', 'table_operations', 'error_handling', 'table_discovery', 'complex_workflows'],
  };
}

