import { Logger } from '@nestjs/common';
import { QueryBuilderService } from '../../../../infrastructure/query-builder/query-builder.service';
import { TDynamicContext } from '../../../../shared/interfaces/dynamic-context.interface';
import { GetHintExecutorDependencies, HintContent } from '../types';

const logger = new Logger('GetHintExecutor');

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

**MANDATORY Workflow:**
1. get_table_details({"tableName":["product"]}) → WAIT for schema
2. Extract fields from result.columns[].name and result.relations[].propertyName
3. Use ONLY verified fields - NEVER guess

**If field NOT FOUND:**
- Check columns → NOT FOUND → check relations → NOT FOUND → **STOP**
- Do NOT retry or guess - inform user field doesn't exist

**Key Rules:**
- Specify minimal fields: "id,name" NOT "*"
- limit=0: all records, limit>0: specified, limit=1 + meta: count
- Relations: {"category":{"id":19}}`;

  const fieldOptHint: HintContent = {
    category: 'field_optimization',
    title: 'Field & Query Optimization',
    content: fieldOptContent,
    tools: ['get_table_details', 'find_records'],
  };

  const tableSchemaOpsContent = `**Table Schema Operations**

**CREATE Tables:**
Structure: create_tables({"tables":[{"name":"products","columns":[{"name":"${idFieldName}","type":"${isMongoDB ? 'uuid' : 'int'}","isPrimary":true,"isGenerated":true},{"name":"name","type":"varchar"}]}]})

**With Relations** (CRITICAL):
1. Find target table ID FIRST: find_records({"table":"table_definition","where":{"name":{"_eq":"categories"}},"fields":"${idFieldName}"})
2. Use ID in relation: {"propertyName":"category","type":"many-to-one","targetTable":{"id":19}}
3. NEVER use table name or hardcoded ID without verification

**Multi-table with Relations:**
1. Create base tables first (no relations)
2. Find their IDs: find_records({"table":"table_definition","where":{"name":{"_in":["categories","instructors"]}}})
3. Create dependent tables with relations using IDs from step 2

**UPDATE Tables:**
update_tables({"tables":[{"tableName":"products","columns":[{"name":"stock","type":"int"}]}]})

**Update Relations (CRITICAL LIMITATIONS):**
- You CAN update relation properties: propertyName, targetTable, isNullable, description...
- You CANNOT update relation type (many-to-one, one-to-many, one-to-one, many-to-many)
- To change relation type: MUST delete existing relation first, then create new one with different type
- Workflow to change type: delete_tables (to remove relation) → create_tables or update_tables (to add new relation with new type)
- Add new relation: Find target ID first, then update_tables with relations array

**CRITICAL:**
- Table name: snake_case, lowercase, not start with "_"
- NEVER include createdAt/updatedAt (auto-generated)
- ALWAYS find target table ID before creating relations
- Use relations array, NOT FK columns
- Relation type is IMMUTABLE - cannot be updated, only deleted and recreated`;

  const tableSchemaOpsHint: HintContent = {
    category: 'table_schema_operations',
    title: 'Table Schema Operations (Create & Update)',
    content: tableSchemaOpsContent,
    tools: ['find_records', 'create_tables', 'update_tables'],
  };

  const tableDeletionContent = `**Table Deletion**

**Workflow:**
1. Find ID: find_records({"table":"table_definition","where":{"name":{"_eq":"products"}},"fields":"${idFieldName}"})
2. Delete: delete_tables({"ids":[19]})
Multiple: find_records with name._in, then delete_tables({"ids":[19,20]})

**CRITICAL:**
- delete_tables = table structure, delete_records = data
- ALWAYS find ID first (cannot use table name)
- NEVER use delete_records on table_definition
- Confirmation required for destructive ops: state table(s)/ids/count before delete; if scope unclear or conflicts with current task, ask brief confirmation then proceed`;

  const tableDeletionHint: HintContent = {
    category: 'table_deletion',
    title: 'Table Deletion Operations',
    content: tableDeletionContent,
    tools: ['find_records', 'delete_tables'],
  };

  const crudWriteOpsContent = `**CRUD Write Operations**

**CREATE Records - Workflow:**
1. get_table_details({"tableName":["product"]}) → Check required fields & relations
2. If required relations exist (isNullable=false): Query related table for valid IDs
   find_records({"table":"categories","fields":"${idFieldName},name","limit":10})
3. Check unique constraints: find_records with where clause
4. Create: create_records({"table":"product","dataArray":[{"name":"Laptop","price":999.99,"category":{"id":19}}],"fields":"${idFieldName}"})

**BATCHING for Multiple Records (CRITICAL):**
- When creating/updating MANY records (e.g., 500 records), MUST split into batches of 100 records per call
- Example: 500 records → 5 separate calls with 100 records each
- create_records({"table":"product","dataArray":[/* 100 records */],"fields":"${idFieldName}"})
- Then repeat for next 100 records, and so on
- NEVER call create_records/update_records with all records in one call if > 100 records

**UPDATE Records:**
1. Check exists: find_records({"table":"product","where":{"${idFieldName}":{"_eq":1}}})
2. Update: update_records({"table":"product","updates":[{"id":1,"data":{"price":899.99}}],"fields":"${idFieldName}"})
- Same batching rule applies: if updating > 100 records, split into batches of 100

**CRITICAL:**
- NEVER include ${idFieldName}, createdAt, updatedAt (auto-generated)
- For relations: Use {"propertyName":{"id":19}}, NOT FK columns
- ALWAYS query related tables for valid IDs - NEVER hardcode
- BATCHING: Always split large operations (>100 records) into batches of 100 records per call`;

  const crudWriteOpsHint: HintContent = {
    category: 'crud_write_operations',
    title: 'CRUD Write Operations (Create & Update Records)',
    content: crudWriteOpsContent,
    tools: ['get_table_details', 'find_records', 'create_records', 'update_records'],
  };

  const crudDeleteOpsContent = `**CRUD Delete Operations**

**Workflow:**
1. Verify exists: find_records({"table":"product","where":{"${idFieldName}":{"_eq":1}},"fields":"${idFieldName}"})
2. Delete: delete_records({"table":"product","ids":[1]})
Batch: delete_records({"table":"product","ids":[1,2,3]})

**CRITICAL:**
- delete_records = data, delete_tables = structure
- ALWAYS verify before delete
- Confirmation required for destructive ops: state table/ids/count; if unclear or conflicting with current task, ask brief confirmation then proceed`;

  const crudDeleteOpsHint: HintContent = {
    category: 'crud_delete_operations',
    title: 'CRUD Delete Operations',
    content: crudDeleteOpsContent,
    tools: ['find_records', 'delete_records'],
  };

  const crudQueryOpsContent = `**CRUD Query Operations**

**MANDATORY Workflow:**
1. get_table_details({"tableName":["product"]}) → WAIT for result
2. Extract fields from result.columns[].name and result.relations[].propertyName
3. Use ONLY verified fields in query - NEVER guess field names

**Find Records:**
find_records({"table":"product","fields":"${idFieldName},name,price","where":{"price":{"_gt":100}},"limit":10,"sort":"-price"})

**Count Records:**
- Total: find_records({"table":"product","fields":"${idFieldName}","limit":1,"meta":"totalCount"})
- With filter: find_records({"table":"product","fields":"${idFieldName}","where":{"price":{"_gt":100}},"limit":1,"meta":"filterCount"})
Read count from response metadata

**Relations:**
find_records({"table":"order","fields":"${idFieldName},customer.name","where":{"customer":{"name":{"_eq":"John"}}}})

**Operators:** _eq, _neq, _gt, _gte, _lt, _lte, _contains, _in, _between, _is_null, _and, _or`;

  const crudQueryOpsHint: HintContent = {
    category: 'crud_query_operations',
    title: 'CRUD Query Operations (Find & Count)',
    content: crudQueryOpsContent,
    tools: ['get_table_details', 'find_records'],
  };

  const systemWorkflowsContent = `**System Workflows (Multi-Step)**

**Workflow:**
0. Check existing task: get_task({"conversationId":<id>}); if pending/in_progress → continue/update instead of duplicating
1. Start: update_task({"conversationId":<id>,"type":"create_table","status":"in_progress","data":{...}})
2. Execute sequentially: create_tables → update_tables → create_records / update_records (one tool at a time)
3. Complete: update_task({"conversationId":<id>,"status":"completed","result":{...}})
4. On error: update_task({"status":"failed","error":"..."}); if fixable, retry only failed items

**Rules:**
- ALWAYS create task first for multi-step operations
- Continue automatically; do not stop to ask unless missing user-only info
- Retry strategy: when a batch partially fails (e.g., create_tables), fix naming/IDs only for failed items and resend ONLY those
- Progress reporting: update_task in-progress messages like "Creating table 4/5", "Adding sample data...", then mark completed
- Intent shift: if user switches request mid-flow, pause current task (update_task status='paused' or 'cancelled' with reason) and confirm before resuming or abandoning`;

  const systemWorkflowsHint: HintContent = {
    category: 'system_workflows',
    title: 'System Workflows (Multi-Step Operations)',
    content: systemWorkflowsContent,
    tools: ['get_task', 'update_task', 'create_tables', 'update_tables', 'delete_tables', 'get_table_details', 'create_records', 'update_records'],
  };

  const errorContent = `CRITICAL - Sequential Execution (PREVENTS ERRORS):
- ALWAYS execute tools ONE AT A TIME, step by step
- Do NOT call multiple tools simultaneously
- Execute → wait → analyze → proceed

Error handling:
- If tool returns error=true → STOP and report to user
- Permission errors: errorCode="PERMISSION_DENIED" → inform user, do NOT retry

**CRITICAL - Field Not Found → STOP:**
- If field not in columns AND not in relations → **STOP IMMEDIATELY**
- Do NOT retry with different table names unless explicitly different
- Do NOT try alternative field names or guesses
- Inform user: "Field '[name]' not found in table '[table]'. Available: [list]"`;

  const errorHint: HintContent = {
    category: 'error_handling',
    title: 'Error Handling Protocol',
    content: errorContent,
    tools: [],
  };

  const metadataOpsContent = `**Metadata Operations**

**List Tables:**
- All: find_records({"table":"table_definition","fields":"name,isSystem","limit":0})
- User tables only: find_records({"table":"table_definition","where":{"isSystem":{"_eq":false}},"fields":"name","limit":0})

**Get Schema:**
- Single: get_table_details({"tableName":["product"]})
- Multiple (efficient): get_table_details({"tableName":["product","category","order"]})`;

  const naturalLanguageDiscoveryContent = `**Natural Language Table Name Discovery**

**Step 1: Get ALL table names**
find_records({"table":"table_definition","fields":"name","limit":0}) → WAIT

**Step 2: Match table name**
Match user term to table names (e.g., "courses" → "courses", "danh mục" → "categories")

**Step 3: Field discovery - STRICT LIMIT**
**A) Finding specific field:**
- Get full schema: get_table_details({"tableName":["courses"]}) → Check columns.data and relations.data
- If field NOT FOUND in columns → Check relations.data → If NOT FOUND: **STOP**
- Get related schema: get_table_details({"tableName":["categories"]}) → construct filter

**CRITICAL - STOP CONDITIONS:**
- After checking columns + relations (2 calls) → If NOT FOUND → **STOP IMMEDIATELY**
- If you tried 2 different tables → **STOP IMMEDIATELY**
- **MAX 4 tool calls total** - if exceeded → inform user and STOP
- NEVER retry same check - results won't change`;

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
    tools: ['find_records', 'get_table_details'],
  };

  const routesEndpointsContent = `**Routes & Endpoints Discovery**

**CRITICAL - NEVER invent routes. ALWAYS query route_definition table first.**

**IMPORTANT - Routes Can Be Customized:**
- Routes paths can be CUSTOMIZED by users - they are NOT always "/table_name"
- Default path format: "/table_name" (e.g., "/products", "/users")
- But users can change paths to anything (e.g., "/api/v1/products", "/custom-path")
- You MUST query route_definition to find the ACTUAL path - NEVER assume

**Find Available Routes:**
1. Query all enabled routes: find_records({"table":"route_definition","where":{"isEnabled":{"_eq":true}},"fields":"path,mainTable.name","limit":0})
2. Filter by table: find_records({"table":"route_definition","where":{"isEnabled":{"_eq":true},"mainTable.name":{"_eq":"products"}},"fields":"path","limit":0})
3. Search by path: find_records({"table":"route_definition","where":{"isEnabled":{"_eq":true},"path":{"_icontains":"product"}},"fields":"path,mainTable.name","limit":10})

**Get Route Details:**
- Get full schema: get_table_details({"tableName":["route_definition"]})
- Query specific route: find_records({"table":"route_definition","where":{"path":{"_eq":"/products"},"isEnabled":{"_eq":true}},"fields":"path,mainTable.name,publishedMethods.method","limit":1})

**CRITICAL Rules:**
- NEVER guess or invent route paths - ALWAYS query route_definition first
- Only routes with isEnabled=true are active
- Route paths are CUSTOMIZABLE - may not match table name
- Use find_records to discover actual routes before suggesting test URLs
- **When providing routes to users:**
  * ALWAYS prefix route path with base API URL from system prompt
  * Example: route path "/users" → provide full URL like "https://api.enfyra.io/users"
  * NEVER provide just "/users" - always include full URL
- **If route not found in route_definition:**
  * Inform user: "Route not found in route_definition table"
  * **MUST tell user**: "If you have customized the route path, it may not be discoverable by table name. Please check your route_definition table or provide the custom path."
  * Do NOT suggest paths that don't exist in route_definition`;

  const routesEndpointsHint: HintContent = {
    category: 'routes_endpoints',
    title: 'Routes & Endpoints Discovery',
    content: routesEndpointsContent,
    tools: ['find_records', 'get_table_details'],
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
    errorHint,
    routesEndpointsHint
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
      'error_handling',
      'routes_endpoints'
    ],
  };
}

