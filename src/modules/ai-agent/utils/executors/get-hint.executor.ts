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

  const fieldOptContent = `**Field Selection & Query Optimization**

**Basic Rules:**
- Always call get_fields or get_table_details BEFORE querying to know available fields
- get_table_details supports single or multiple tables: {"tableName": "post"} or {"tableName": ["post", "category"]}
- For multiple tables, use array format to get all schemas in ONE call: {"tableName": ["table1", "table2", "table3"]}

**Field Parameter Examples:**
✅ CORRECT - Read operations:
- List with names: {"fields": "${idFieldName},name", "limit": 10}
- Count records: {"fields": "${idFieldName}", "limit": 1, "meta": "totalCount"}
- With relations: {"fields": "${idFieldName},name,category.name"}
- Multiple relations: {"fields": "${idFieldName},order.customer.name,order.items.product.name"}

✅ CORRECT - Write operations (ALWAYS specify minimal fields):
- After create: {"fields": "${idFieldName}"}
- After update: {"fields": "${idFieldName},name"}

❌ WRONG:
- {"fields": "*"} → wastes tokens
- Omitting fields parameter → returns all fields, wastes tokens

**Limit Parameter:**
- limit=0: Fetch ALL records (use when user wants "all records")
- limit>0: Fetch specified number (default: 10)
- For COUNT: limit=1 with meta="totalCount" (no filter) or meta="filterCount" (with filter)
- IMPORTANT: If you call find with limit=10 and get results, DO NOT call again with limit=20 - reuse result or use limit=0 from start

**COUNT Query Examples:**
✅ Count total records: count_records({"table":"product","fields":"${idFieldName}","meta":"totalCount"})
→ Read totalCount from response metadata

✅ Count with filter: count_records({"table":"product","fields":"${idFieldName}","where":{"price":{"_gt":100}},"meta":"filterCount"})
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
✅ CORRECT:
- {"category": 19} (simple ID - preferred)
- {"category": {"id": 19}} (object format - also works)
- {"customer": 1, "category": 19} (multiple relations)

❌ WRONG:
- {"category_id": 19} → Use propertyName, not FK column
- {"categoryId": 19} → Use propertyName, not camelCase
- {"category": {"name": "Electronics"}} → Use ID, not name

**Nested Query Examples:**
✅ Get order with customer name: {"table":"order","operation":"find","fields":"${idFieldName},total,customer.name","where":{"customer":{"name":{"_eq":"John"}}}}
✅ Multi-level: {"table":"route_definition","operation":"find","fields":"${idFieldName},path,roles.name","where":{"roles":{"name":{"_eq":"Admin"}}}}`;

  const fieldOptHint = {
    category: 'field_optimization',
    title: 'Field & Query Optimization',
    content: fieldOptContent,
  };

  const tableOpsContent = `**Table Operations - Step by Step Examples**

**Creating Tables:**
1. Check if table exists: find_records({"table":"table_definition","where":{"name":{"_eq":"products"}},"fields":"${idFieldName},name","limit":1})
2. If not exists, create table:
   ✅ CORRECT Example:
   create_table({
     "name": "products",
     "description": "Product catalog",
     "columns": [
       {"name": "${idFieldName}", "type": "${isMongoDB ? 'uuid' : 'int'}", "isPrimary": true, "isGenerated": true},
       {"name": "name", "type": "varchar", "isNullable": false},
       {"name": "price", "type": "float", "isNullable": false}
     ]
   })
   
   ❌ WRONG:
   - Missing ${idFieldName} column
   - Including createdAt/updatedAt (auto-generated)
   - Including FK columns (use relations instead)

**Updating Tables:**
✅ Example - Add new column:
update_table({
  "tableName": "products",
  "columns": [{"name": "stock", "type": "int", "isNullable": true, "default": 0}]
})

✅ Example - Add relation:
1. Find target table ID: find_records({"table":"table_definition","where":{"name":{"_eq":"categories"}},"fields":"${idFieldName}","limit":1})
2. Add relation: update_table({
  "tableName": "products",
  "relations": [{
    "propertyName": "category",
    "type": "many-to-one",
    "targetTable": {"id": 19}
  }]
})

**Deleting Tables (NOT Data):**
✅ CORRECT Workflow:
1. Find table ID: find_records({"table":"table_definition","where":{"name":{"_eq":"products"}},"fields":"${idFieldName},name","limit":1})
2. Delete table: delete_table({"id": 19})

❌ WRONG:
- delete_record({"table":"products","id":1}) → This deletes DATA, not the table structure
- delete_table({"id": "products"}) → Must use numeric ID, not table name

**Batch Operations:**
✅ For data tables (2+ records):
- batch_create: batch_create_records({"table":"product","dataArray":[{"name":"P1"},{"name":"P2"}],"fields":"${idFieldName}"})
- batch_update: batch_update_records({"table":"product","updates":[{"id":1,"data":{"price":100}}],"fields":"${idFieldName}"})
- batch_delete: batch_delete_records({"table":"product","ids":[1,2,3]})

❌ For metadata tables (table_definition):
- NEVER use batch operations
- Delete ONE BY ONE sequentially to avoid deadlocks`;

  const tableOpsHint = {
    category: 'table_operations',
    title: 'Table Creation & Management',
    content: tableOpsContent,
  };

  const dynamicRepoContent = `**CRUD Operations - Complete Workflows with Examples**

**CREATE Workflow:**
Step 1: Get schema
get_table_details({"tableName": ["product"]})

Step 2: Check unique constraints (if any)
find_records({"table":"product","where":{"name":{"_eq":"Laptop"}},"fields":"${idFieldName}","limit":1})

Step 3: Prepare data with ALL required fields
{
  "name": "Laptop",
  "price": 999.99,
  "category": 19
}

Step 4: Create record
create_record({
  "table": "product",
  "data": {"name": "Laptop", "price": 999.99, "category": 19},
  "fields": "${idFieldName}"
})

**UPDATE Workflow:**
Step 1: Get schema
get_table_details({"tableName": ["product"]})

Step 2: Check if record exists
find_records({"table":"product","where":{"${idFieldName}":{"_eq":1}},"fields":"${idFieldName}","limit":1})

Step 3: Update
update_record({
  "table": "product",
  "id": 1,
  "data": {"price": 899.99},
  "fields": "${idFieldName}"
})

**DELETE Workflow (for DATA records):**
Step 1: Verify exists
find_records({"table":"product","where":{"${idFieldName}":{"_eq":1}},"fields":"${idFieldName}","limit":1})

Step 2: Delete
delete_record({
  "table": "product",
  "id": 1
})

**CRITICAL - Deleting TABLES (not data):**
❌ WRONG: delete_record({"table":"products","id":1}) → This deletes DATA, not table

✅ CORRECT:
1. Find table ID: find_records({"table":"table_definition","where":{"name":{"_eq":"products"}},"fields":"${idFieldName},name","limit":1})
2. Delete table: delete_table({"id": 19})

**FIND Workflow:**
Step 1: Get field names (if needed)
get_fields({"tableName": "product"})

Step 2: Query
find_records({
  "table": "product",
  "fields": "${idFieldName},name,price",
  "where": {"price": {"_gt": 100}},
  "limit": 10,
  "sort": "-price"
})

**Common Mistakes:**
❌ Missing schema check before create/update → constraint errors
❌ Not checking unique constraints → duplicate key errors
❌ Using FK column names instead of propertyName → errors
❌ Including id in create operations → errors
❌ Using delete_record to delete tables → wrong tool

**Best Practices:**
✅ Always call get_table_details FIRST for create/update
✅ Always check unique constraints before create
✅ Use propertyName from relations array, not FK columns
✅ Specify minimal fields parameter to save tokens
✅ Execute ONE operation at a time (sequential)`;

  const dynamicRepoHint = {
    category: 'crud_operations',
    title: 'CRUD Operations Complete Workflows',
    content: dynamicRepoContent,
  };

  const complexWorkflowsContent = `**Complex Workflows - Step by Step**

**Recreate Tables with Relations:**
1. Find existing tables: find_records({"table":"table_definition","where":{"name":{"_in":["post","category"]}},"fields":"${idFieldName},name","limit":0})
2. Delete ONE BY ONE (not batch): 
   - delete_table({"id": 1})
   - delete_table({"id": 2})
3. Create new tables: create_table({...})
4. Find new IDs and add relations: update_table({"tableName": "post", "relations": [...]})

**Common Mistakes:**
❌ Creating tables without ${idFieldName} column
❌ Including createdAt/updatedAt in columns
❌ Including FK columns in columns array
❌ Using batch_delete for table deletion
❌ Multiple find calls instead of _in filter

**Efficiency Tips:**
✅ Use _in filter for multiple tables
✅ Use create_table/update_table tools (auto-validation)
✅ Use batch operations for data tables only`;

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
- Example workflow: find_records → wait → delete_record → wait → continue
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

  allHints.push(dbTypeHint, fieldOptHint, tableOpsHint, errorHint, discoveryHint, complexWorkflowsHint, dynamicRepoHint);

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
    availableCategories: ['database_type', 'field_optimization', 'table_operations', 'error_handling', 'table_discovery', 'complex_workflows', 'crud_operations'],
  };
}

