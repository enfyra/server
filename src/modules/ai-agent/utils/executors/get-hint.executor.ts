import { Logger } from '@nestjs/common';
import { TDynamicContext } from '../../../../shared/types';
import { GetHintExecutorDependencies, HintContent } from '../../types';

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
1. Find target table ID FIRST: find_records({"table":"table_definition","filter":{"name":{"_eq":"categories"}},"fields":"${idFieldName}"})
2. Use ID in relation: {"propertyName":"category","type":"many-to-one","targetTable":{"id":19}}
3. NEVER use table name or hardcoded ID without verification

**Multi-table with Relations:**
1. Create base tables first (no relations)
2. Find their IDs: find_records({"table":"table_definition","filter":{"name":{"_in":["categories","instructors"]}}})
3. Create dependent tables with relations using IDs from step 2

**UPDATE Tables:**
update_tables({"tables":[{"tableName":"products","columns":[{"name":"stock","type":"int"}]}]})

**Update Relations (CRITICAL LIMITATIONS):**
- You CAN update relation properties: propertyName, targetTable, isNullable, description...
- You CANNOT update relation type (many-to-one, one-to-many, one-to-one, many-to-many)
- To change relation type: MUST delete existing relation first, then create new one with different type
- Workflow to change type: delete_tables (to remove relation) → create_tables or update_tables (to add new relation with new type)
- Add new relation: Find target ID first, then update_tables with relations array

**CRITICAL - create_tables AUTO-CREATES route:**
- When create_tables succeeds, route /{table_name} is AUTO-CREATED (path, mainTable, isEnabled:true)
- Do NOT create route_definition for the new table - it already exists
- To add handler/hook: find_records route_definition filter path._eq, then create pre_hook/post_hook or route_handler_definition

**CRITICAL:**
- Table name: snake_case, lowercase, not start with "_"
- NEVER include createdAt/updatedAt (auto-generated)
- ALWAYS find target table ID before creating relations
- Use relations array, NOT FK columns
- Relation type is IMMUTABLE - cannot be updated, only deleted and recreated

**Confirmation (schema change):** For create_tables, update_tables: state what you will create/update, ask in user's language, proceed after confirm.

**SCHEMA LOCK - ONE TABLE PER CALL (CRITICAL):**
- create_tables and update_tables: pass exactly 1 table per call, wait for result, then next table
- Passing multiple tables causes "Schema is being updated" errors
- Example: 3 tables → create_tables({"tables":[t1]}), wait → create_tables({"tables":[t2]}), wait → create_tables({"tables":[t3]})`;

  const tableSchemaOpsHint: HintContent = {
    category: 'table_schema_operations',
    title: 'Table Schema Operations (Create & Update)',
    content: tableSchemaOpsContent,
    tools: ['find_records', 'create_tables', 'update_tables'],
  };

  const tableDeletionContent = `**Table Deletion**

**Workflow:**
1. Find ID: find_records({"table":"table_definition","filter":{"name":{"_eq":"products"}},"fields":"${idFieldName}"})
2. Delete: delete_tables({"ids":[19]})
Multiple tables: find_records with name._in, then delete ONE at a time: delete_tables({"ids":[19]}), wait → delete_tables({"ids":[20]}), etc.

**CRITICAL:**
- delete_tables = table structure, delete_records = data
- ALWAYS find ID first (cannot use table name)
- NEVER use delete_records on table_definition
- Confirmation required (schema change): state table(s)/ids/count before delete_tables; ask in user's language; proceed after user confirms

**SCHEMA LOCK - ONE TABLE PER CALL (CRITICAL):**
- delete_tables: pass exactly 1 id per call, wait for result, then next table
- Passing multiple ids causes "Schema is being updated" errors - most deletes will fail
- Example: 10 tables → delete_tables({"ids":[1]}), wait → delete_tables({"ids":[2]}), etc.
- If any delete fails with schema lock error: retry that table alone after a moment`;

  const tableDeletionHint: HintContent = {
    category: 'table_deletion',
    title: 'Table Deletion Operations',
    content: tableDeletionContent,
    tools: ['find_records', 'delete_tables'],
  };

  const crudWriteOpsContent = `**CRUD Write Operations**

**CASCADE-first (create/update from root):**
- Prefer create/update parent with nested child relations over separate child-table calls. Fewer tools, atomic. Example: update route_definition with handlers: [{method:{id},logic}], targetTables: [{id}], publishedMethods: [{id}] in one update_records – do NOT create route_handler_definition separately then update route.

**CREATE Records - Workflow:**
1. get_table_details({"tableName":["product"]}) → Check required fields & relations
2. If required relations exist (isNullable=false): Query related table for valid IDs
   find_records({"table":"categories","fields":"${idFieldName},name","limit":10})
3. Check unique constraints: find_records with filter clause
4. Create: create_records({"table":"product","dataArray":[{"name":"Laptop","price":999.99,"category":{"id":19}}],"fields":"${idFieldName}"}) — relations: use propertyName (category) with {id}, NOT categoryId

**BATCHING for Multiple Records (CRITICAL):**
- When creating/updating MANY records (e.g., 500 records), MUST split into batches of 100 records per call
- Example: 500 records → 5 separate calls with 100 records each
- create_records({"table":"product","dataArray":[/* 100 records */],"fields":"${idFieldName}"})
- Then repeat for next 100 records, and so on
- NEVER call create_records/update_records with all records in one call if > 100 records

**UPDATE Records:**
1. Check exists: find_records({"table":"product","filter":{"${idFieldName}":{"_eq":1}}})
2. Update: update_records({"table":"product","updates":[{"id":1,"data":{"price":899.99}}],"fields":"${idFieldName}"})
- Same batching rule applies: if updating > 100 records, split into batches of 100

**CRITICAL:**
- NEVER include ${idFieldName}, createdAt, updatedAt (auto-generated)
- NEVER include columns with defaultValue or isGenerated - they are auto-filled
- NEVER include isSystem, isRootAdmin in request body for user-facing APIs (registration, signup) - server sets these with safe defaults
- For relations: Use propertyName from relations[].propertyName with {"propertyName":{"id":19}}, NEVER use foreignKeyColumn (roleId, categoryId)
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
1. Verify exists: find_records({"table":"product","filter":{"${idFieldName}":{"_eq":1}},"fields":"${idFieldName}"})
2. Delete: delete_records({"table":"product","ids":[1]})
Batch: delete_records({"table":"product","ids":[1,2,3]})

**CRITICAL:**
- delete_records = data, delete_tables = structure
- ALWAYS verify before delete
- delete_records = data only, no confirmation required. For delete_tables (schema), confirmation required.`;

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
find_records({"table":"product","fields":"${idFieldName},name,price","filter":{"price":{"_gt":100}},"limit":10,"sort":"-price"})

**Count Records:**
- Total: find_records({"table":"product","fields":"${idFieldName}","limit":1,"meta":"totalCount"})
- With filter: find_records({"table":"product","fields":"${idFieldName}","filter":{"price":{"_gt":100}},"limit":1,"meta":"filterCount"})
Read count from response metadata

**Relations:**
find_records({"table":"order","fields":"${idFieldName},customer.name","filter":{"customer":{"name":{"_eq":"John"}}}})

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
- User tables only: find_records({"table":"table_definition","filter":{"isSystem":{"_eq":false}},"fields":"name","limit":0})

**Get Schema:**
- Single: get_table_details({"tableName":["product"]})
- Multiple (efficient): get_table_details({"tableName":["product","category","order"]})`;

  const naturalLanguageDiscoveryContent = `**Natural Language Table Name Discovery**

**Step 1: Get ALL table names**
find_records({"table":"table_definition","fields":"name","limit":0}) → WAIT

**Step 2: Match table name**
Match user term to table names (e.g., "courses" → "courses", "category" → "categories")

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

**CRITICAL - create_tables AUTO-CREATES route /{table_name}:**
- When a table is created via create_tables, route /{table_name} is AUTO-CREATED (path, mainTable, isEnabled)
- Do NOT create route_definition for /{table_name} - it already exists. Just add handler/hook if needed.

**CRITICAL - NEVER invent routes. ALWAYS query route_definition table first.**

**$repos in route handler – targetTables only, NEVER mainTable:**
- Agent MUST NEVER set mainTable. create_records/update_records route_definition: use targetTables ONLY. NEVER include mainTable.
- Handler uses #<table_name> from targetTables. Prefer #table_name over #main.

**CASCADE-first for routes:** Update route_definition with nested handlers, targetTables, publishedMethods, preHooks, postHooks in one update_records. Do NOT create route_handler_definition, pre_hook_definition, post_hook_definition separately.

**Making route PUBLIC:** update_records route_definition with data: {publishedMethods: [{id: getMethodId}, ...existing]}. Do NOT create route_permission_definition.

**IMPORTANT - Routes Can Be Customized:**
- Routes paths can be CUSTOMIZED by users - they are NOT always "/table_name"
- Default path format: "/table_name" (e.g., "/products", "/users")
- But users can change paths to anything (e.g., "/api/v1/products", "/custom-path")
- You MUST query route_definition to find the ACTUAL path - NEVER assume

**Find Available Routes:**
1. Query all enabled routes: find_records({"table":"route_definition","filter":{"isEnabled":{"_eq":true}},"fields":"path,mainTable.name","limit":0})
2. Filter by table: find_records({"table":"route_definition","filter":{"isEnabled":{"_eq":true},"mainTable.name":{"_eq":"products"}},"fields":"path","limit":0})
3. Search by path: find_records({"table":"route_definition","filter":{"isEnabled":{"_eq":true},"path":{"_contains":"product"}},"fields":"path,mainTable.name","limit":10})

**Get Route Details:**
- Get full schema: get_table_details({"tableName":["route_definition"]})
- Query specific route: find_records({"table":"route_definition","filter":{"path":{"_eq":"/products"},"isEnabled":{"_eq":true}},"fields":"path,mainTable.name,publishedMethods.method","limit":1})

**Delete route (workflow):**
1. find_records route_definition filter path._eq → routeId
2. find_records route_handler_definition filter route.id._eq routeId → handler ids
3. delete_records route_handler_definition with ids:[...] (delete handlers first)
4. delete_records route_definition with ids:[routeId]. Use ids NOT filter.

**CRITICAL Rules:**
- NEVER guess or invent route paths - ALWAYS query route_definition first
- Only routes with isEnabled=true are active
- Route paths are CUSTOMIZABLE - may not match table name
- Use find_records to discover actual routes before suggesting test URLs
- **When providing API path to users:** Use format {YOUR_APP_URL}/api/{path}. Example: route path "/foo-baz" → enfyra.io/api/foo-baz or https://your-domain.com/api/foo-baz. Do NOT hardcode URL.
- **If route not found in route_definition:**
  * Inform user: "Route not found in route_definition table"
  * **MUST tell user**: "If you have customized the route path, it may not be discoverable by table name. Please check your route_definition table or provide the custom path."
  * Do NOT suggest paths that don't exist in route_definition`;

  const routesEndpointsHint: HintContent = {
    category: 'routes_endpoints',
    title: 'Routes & Endpoints Discovery',
    content: routesEndpointsContent,
    tools: ['find_records', 'get_table_details', 'update_records', 'delete_records'],
  };

  const handlerOpsContent = `**Route Handler Operations**

**CRITICAL - Agent MUST NEVER set mainTable:**
- NEVER include mainTable in create_records or update_records for route_definition. Agent has NO permission.
- Link tables handler needs via targetTables ONLY: targetTables: [{id: tableId}]. Example: find_records table_definition filter name._eq "user_definition" → tableId → targetTables: [{id: tableId}].
- WRONG: create_records route_definition with mainTable. RIGHT: create_records with path, targetTables, isEnabled - NO mainTable.

**CRITICAL - Use #table_name, NOT #main:**
- ALWAYS use #<table_name> (e.g. #user_definition, #products) in handler logic. NEVER use #main.
- #main is only valid if route.mainTable equals the table you need - you must verify mainTable first. When in doubt, use #table_name.
- Example: "return #user_definition.find({ filter: { email: @BODY.email }, limit: 1 });" NOT "return #main.find(...)"

**Context available (route handler only):**
- @BODY: request body | @PARAMS: route params | @QUERY: query (filter, fields, limit, sort, page, meta). Use @QUERY.filter (not where) | @USER: logged-in user
- #<table_name>: repos from route.targetTables. API: .find({ filter, limit }) → { data: [...] }; .create({ data: {...} }) → { data: [record], count?: 1 }; .update({ id, data: {...} }) → { data: [record] }; .delete({ id }). CRITICAL: create/update require { data: object }, NOT raw object. To get single record: created?.data?.[0], updated?.data?.[0].
- @HELPERS: bcrypt via @HELPERS.$bcrypt.hash(plain), @HELPERS.$bcrypt.compare(plain, hash). Use @HELPERS.$bcrypt - NEVER $bcrypt. Rate limiting via @HELPERS.$rateLimit.byIp({maxRequests:100, perSeconds:60}), .byUser, .byRoute, .check(key, options). Returns {allowed, remaining, retryAfter}.
- @RES: response | @CACHE | @THROW4xx/5xx | @UPLOADED_FILE

**PREFER template syntax:** @BODY, #user_definition, @THROW404 instead of $ctx.$body, $ctx.$repos.main, etc.

**CRITICAL - Test BEFORE writing/saving ANY custom code (handler, hook, websocket):**
- MUST run_handler_test BEFORE create/update for route_handler_definition, pre_hook_definition, post_hook_definition, bootstrap_script_definition, websocket handlers.
- Order: 1) Write draft code. 2) run_handler_test FIRST to verify. 3) If error: use fixGuidance + nextSteps from response to fix, retry. 4) After success: cleanup test records. 5) Show code to user. 6) create/update (no confirmation needed).
- NEVER create/update any handler/hook/ws record without run_handler_test success + cleanup. Always show code to user before create/update.

**CRITICAL - For update/delete handlers: create new record first to test:**
- NEVER test update or delete on existing records. Create a new record via create_records, then test update/delete on that new record, then delete_records to cleanup.

**Workflow - PREFER CASCADE (update from route, not separate child tables):**
- Update route_definition with nested handlers, preHooks, postHooks, targetTables, publishedMethods in one update_records. Do NOT create route_handler_definition/pre_hook_definition separately.
- Example: update_records route_definition updates: [{id: routeId, data: {handlers: [{method: {id: postMethodId}, logic: "..."}], publishedMethods: [{id: getMethodId}], targetTables: [{id: tableId}]}}]

**Workflow - NEW table + handler (e.g. posts with RLS):**
1. create_tables for table (route AUTO-CREATED). 2. find_records route_definition path._eq "/posts" → routeId. 3. find_records method_definition (GET, POST). 4. run_handler_test. 5. update_records route_definition with data: {handlers: [{method:{id}, logic}], preHooks: [{...}]} – CASCADE from route.

**Workflow - EXISTING table + handler:**
1. find_records route_definition path._eq or mainTable.name._eq. 2. find_records method_definition. 3. run_handler_test. 4. update_records route_definition with nested handlers/hooks – CASCADE from route.

**Workflow - CUSTOM route (path different from /{table_name}, e.g. /register):**
- Only create route_definition when path is NOT /{table_name}. For /{table_name}, route is auto-created - Do NOT create.
- When creating: path, targetTables: [{id: tableId}], isEnabled: true. NEVER mainTable. Example: find_records table_definition filter name._eq "user_definition" → id → create_records route_definition dataArray: [{ path: "/register", targetTables: [{id: tableId}], isEnabled: true }].

**Logic structure:** return { data: ... } or throw. Example: return #user_definition.find({ filter: {}, limit: 10 }); Use #table_name, never #main.

**Delete route + handler:** 1) find_records route_definition filter path._eq → routeId. 2) find_records route_handler_definition filter route.id._eq routeId → handler ids. 3) delete_records route_handler_definition ids:[...]. 4) delete_records route_definition ids:[routeId]. Use ids, NOT filter.

**Route + public access:** When creating route with handler, if route should be public (no auth): add publishedMethods to route_definition (e.g. icon, publishedMethods). For "make public" later: update_records route_definition with publishedMethods (add method id). Do NOT create route_permission_definition for public.

**Testing handler before deploy (run_handler_test) – MANDATORY before create/update:**
- MUST run_handler_test before done. After test: cleanup. Then show code to user and create/update.
- For update/delete: create_records first → run_handler_test on new record → delete_records to cleanup.
- Workflow: 1) get_table_details. 2) Write handler. 3) run_handler_test. 4) If error: read fixGuidance and nextSteps, fix code, retry. 5) Cleanup. 6) Show code to user → create/update.
- Example: run_handler_test({"table":"products","handlerCode":"return await #products.find({ limit: 5 });"})

**run_handler_test error response – fix strategy:**
- When success=false, response includes: errorKind, fixGuidance, nextSteps, location?, codeContext?.
- errorKind → fix: SYNTAX_ERROR (brackets, quotes, typos) | REFERENCE_ERROR (#table typo, @BODY typo) | TYPE_ERROR (.find/.create args, null check) | SCRIPT_TIMEOUT (reduce limit, increase timeoutMs) | BUSINESS_LOGIC (fix validation) | RESOURCE_NOT_FOUND (create test record) | VALIDATION_ERROR (check schema, body) | DATABASE_QUERY (filter operators) | HELPER_NOT_FOUND (@HELPERS.$bcrypt) | TABLE_NOT_FOUND (verify table name).
- ALWAYS follow nextSteps to fix, then retry run_handler_test.

**Registration/Signup handlers (CRITICAL - Security):**
- NEVER read isSystem, isRootAdmin from @BODY - set server-side: isSystem: false, isRootAdmin: false (or from safe defaults)
- For relations: Use propertyName (e.g. role) NOT foreignKeyColumn (roleId). Example: role: @BODY.role ? { id: @BODY.role } : null
- Request body for registration should only include: email, role (optional, as id). Exclude isSystem, isRootAdmin
- Hash password: const hashed = await @HELPERS.$bcrypt.hash(@BODY.password). NEVER use $bcrypt - use @HELPERS.$bcrypt
- Registration example: const { email, password } = @BODY; const res = await #user_definition.find({ filter: { email: { _eq: email } }, limit: 1 }); if (res?.data?.length) @THROW400("Email already exists"); const hashed = await @HELPERS.$bcrypt.hash(password); const created = await #user_definition.create({ data: { email, password: hashed, isSystem: false, isRootAdmin: false } }); return { data: created?.data?.[0] ?? {} };

**Handler packages ($ctx.$pkgs) – lodash, axios, etc.:**
- Handlers access npm packages via $ctx.$pkgs.packagename (e.g. $ctx.$pkgs.lodash, $ctx.$pkgs.axios).
- CRITICAL: Before writing handler that uses a package, check if installed: find_records({"table":"package_definition","filter":{"type":{"_eq":"Server"},"name":{"_eq":"lodash"},"isEnabled":{"_eq":true}},"fields":"id,name","limit":1})
- If NOT found: Tell user "Package [name] is required. Please install: Settings → Packages → Install Package → Server Package → install [name]. Then retry." Agent CANNOT install.
- Only use $ctx.$pkgs in code AFTER confirming package exists.`;

  const hookOpsContent = `**Pre/Post Hooks** – Ctx: $body,$params,$query,$user,$repos,$res,$api,$cache,$helpers,$throw. Pre: no $data. Post: +$data,$api.response. Pre return → short-circuit.

**CRITICAL – @QUERY uses filter, NOT where:**
- API query params: filter, fields, limit, sort, page, meta, aggregate, deep
- To modify query in pre-hook: use @QUERY.filter (not @QUERY.where)
- Example RLS (only author sees own posts): @QUERY.filter = { ...(@QUERY.filter || {}), author: { id: { _eq: @USER.id } } };

**CRITICAL – methods is REQUIRED:** Hook runs ONLY when request method (GET/POST/etc.) matches hook's methods. If methods is empty, hook NEVER runs.

**CRITICAL – Test before save:** MUST run_handler_test before create/update hook. Use table from route's targetTables. On error: follow fixGuidance + nextSteps, fix, retry.

**Rate Limiting (pre-hook protection):**
- Use @HELPERS.$rateLimit to protect APIs from abuse. Returns {allowed, remaining, retryAfter, limit, window}.
- Templates: byIp (per IP per route), byUser (per user per route), byRoute (global per route), byIpGlobal (per IP all routes), byUserGlobal (per user all routes), check(key, options) (custom key).
- Options: {maxRequests: number, perSeconds: number}
- Example (limit 100 req/min by IP):
  const result = await @HELPERS.$rateLimit.byIp({maxRequests:100, perSeconds:60});
  if (!result.allowed) { @THROW['429']("Rate limit exceeded. Try again in " + result.retryAfter + "s"); }
- Example (skip for admins):
  if (!@USER?.isRootAdmin) { const r = await @HELPERS.$rateLimit.byIp({maxRequests:100, perSeconds:60}); if (!r.allowed) @THROW['429']("Rate limit exceeded"); }
- Example (login protection):
  const result = await @HELPERS.$rateLimit.byIp({maxRequests:5, perSeconds:60});
  if (!result.allowed) { @THROW['429']("Too many login attempts. Try again in " + result.retryAfter + "s"); }

**RLS pre-hook example (filter by current user):**
if (!@USER) return { error: "Unauthorized" };
@QUERY.filter = { ...(@QUERY.filter || {}), author: { id: { _eq: @USER.id } } };

**Workflow - PREFER CASCADE:** update_records route_definition with data: {preHooks: [{methods:[{id}], code, isEnabled:true}], postHooks: [...]} – nested from route. If separate: create_records pre_hook_definition with route:{id}, methods:[{id}], code.

**Delete hook:** find_records pre_hook_definition (or post_hook_definition) filter route.id._eq routeId → ids → delete_records with ids:[...]. Use ids, NOT filter.

**Hook packages ($ctx.$pkgs):** Same rule as handlers. Before using lodash, axios, etc.: find_records package_definition filter type._eq "Server", name._eq "packagename". If not installed, tell user to install via Settings → Packages → Server Package. Agent cannot install. Only use after confirming.`;

  const hookOpsHint: HintContent = {
    category: 'hook_operations',
    title: 'Pre/Post Hook Operations',
    content: hookOpsContent,
    tools: ['find_records', 'create_records', 'update_records', 'delete_records', 'run_handler_test', 'get_table_details'],
  };

  const bootstrapOpsContent = `**Bootstrap Script Operations** – Runs on app startup (no HTTP request).

**Context (no HTTP):**
- #<table_name>: $repos with all init'd tables (e.g. #products, #users) – query/insert at startup
- @CACHE | @HELPERS (autoSlug) | @THROW | @LOGS
- No: @BODY, @USER, @REQ, @RES, @PARAMS, @QUERY, @UPLOADED_FILE, @SOCKET

**CRITICAL – Test before save:** MUST run_handler_test before create/update bootstrap. Pass table from logic (e.g. #role_definition → table:"role_definition"). On error: fix per fixGuidance, retry.

**Workflow:** 1) Write logic. 2) run_handler_test to verify. 3) On error: fix. 4) Show logic to user. 5) create_records bootstrap_script_definition: name, logic, priority, isEnabled:true.

**Fields:** name, description, logic (code), timeout (ms), priority (int), isEnabled (default true).

**Logic pattern:** Use #<table_name>. .find({ filter, limit }) → { data }; .create({ data: {...} }) → { data: [record] }; use created?.data?.[0] for single record.`;

  const bootstrapOpsHint: HintContent = {
    category: 'bootstrap_operations',
    title: 'Bootstrap Script Operations',
    content: bootstrapOpsContent,
    tools: ['create_records', 'update_records', 'find_records', 'run_handler_test', 'get_table_details'],
  };

  const websocketOpsContent = `**WebSocket Handler Operations** – Real-time bi-directional communication via Socket.IO.

**Schema - websocket_definition (Gateway):**
- path: Namespace path (e.g., "/chat", "/notifications"). Must start with "/".
- requireAuth: boolean. If true, clients must provide JWT token in auth.
- connectionHandlerScript: Code runs when client connects.
- connectionHandlerTimeout: Timeout in ms (default 30000).
- isEnabled: boolean.

**Schema - websocket_event_definition (Event):**
- gateway: Relation to websocket_definition (required).
- eventName: Event name client emits (e.g., "send_message", "typing").
- handlerScript: Code runs when event received.
- timeout: Timeout in ms (default 30000).
- isEnabled: boolean.

**@SOCKET Methods (different behavior by handler type):**

Connection Handler (connectionHandlerScript):
- @SOCKET.emit(event, data) → Send to THIS client only
- @SOCKET.to(room).emit(event, data) → Send to room (not this client)

Event Handler (handlerScript):
- @SOCKET.emit(event, data) → Broadcast to ALL clients in namespace
- @SOCKET.send(event, data) → Send to THIS client only
- @SOCKET.to(room).emit(event, data) → Send to room (not this client)

**Context Variables:**
- Connection: @BODY = {id: socketId, ip, headers}, @USER = {id} if auth, @SOCKET
- Event: @BODY = payload from client, @USER = {id} if auth, @SOCKET
- Both: $repos = {} (empty), use @HELPERS.$api for external calls if needed.

**Code Examples:**

Connection handler (log connection + join user room):
\`\`\`
// @BODY = {id: "socket123", ip: "127.0.0.1", headers: {...}}
if (@USER) {
  // User already auto-joined room "user_{userId}" by system
}
// Send welcome to this client
@SOCKET.emit("connected", {message: "Welcome!", socketId: @BODY.id});
\`\`\`

Event handler (chat message):
\`\`\`
// @BODY = {text: "Hello", roomId: "general"}
if (!@BODY.text || !@BODY.roomId) {
  @THROW400("text and roomId required");
}
const message = {
  text: @BODY.text,
  userId: @USER.id,
  timestamp: Date.now()
};
// Broadcast to all in room
@SOCKET.to(@BODY.roomId).emit("message", message);
// Confirm to sender
@SOCKET.send("message_sent", message);
\`\`\`

**Workflow - Create Gateway:**
1. get_table_details({tableName: ["websocket_definition"]})
2. create_records({table: "websocket_definition", dataArray: [{path: "/chat", requireAuth: true, connectionHandlerScript: "...", isEnabled: true}]})

**Workflow - Create Event:**
1. find_records({table: "websocket_definition", filter: {path: {_eq: "/chat"}}, fields: "id"})
2. create_records({table: "websocket_event_definition", dataArray: [{gateway: {id: GATEWAY_ID}, eventName: "send_message", handlerScript: "...", isEnabled: true}]})

**Workflow - Update Gateway/Event:**
1. find_records to get id
2. update_records({table: "websocket_definition", updates: [{id: ID, data: {connectionHandlerScript: "..."}}]})

**Workflow - Delete Event:**
1. find_records to get id
2. delete_records({table: "websocket_event_definition", ids: [ID]})

**Client Connection (JavaScript):**
\`\`\`
import { io } from "socket.io-client";
const socket = io("http://localhost:1105/chat", {auth: {token: "JWT_TOKEN"}});
socket.on("message", (data) => console.log(data));
socket.emit("send_message", {text: "Hello", roomId: "general"});
\`\`\`

**CRITICAL:**
- Test handlers with run_handler_test before saving
- path must be UNIQUE across gateways
- requireAuth: true requires valid JWT in auth.token
- Changes auto-reload gateways (no restart needed)`;

  const websocketOpsHint: HintContent = {
    category: 'websocket_operations',
    title: 'WebSocket Handler Operations',
    content: websocketOpsContent,
    tools: ['find_records', 'create_records', 'update_records', 'delete_records', 'run_handler_test', 'get_table_details'],
  };

  const handlerOpsHint: HintContent = {
    category: 'handler_operations',
    title: 'Route Handler Operations (Custom Logic)',
    content: handlerOpsContent,
    tools: ['find_records', 'create_records', 'update_records', 'delete_records', 'run_handler_test', 'get_table_details'],
  };

  const menuOpsContent = `**Menu Operations** – Create and manage navigation menus (sidebar) via menu_definition.

**CRITICAL - menu_definition has LABEL, NOT name:**
- menu_definition columns: id, type, **label**, icon, path, order, isEnabled, parent...
- There is NO "name" column. Display text is in **label**.
- To find menu by display text (Dashboard, Welcome, Settings): filter {"label":{"_eq":"Dashboard"}} - NEVER use "name"
- fields: use "id,label" or "id,label,path" - NOT "id,name"

**Menu Types:**
- **Menu** (leaf item): Clickable item that navigates to a page. Has path (e.g. /reports). Can also be a container for child items.
- **Dropdown Menu** (container): Collapsible section that groups child menus. Has label + icon. Path is optional (e.g. /settings for section URL). Children appear when expanded.

**Ordering (field: order):**
- order: integer. Lower number = appears HIGHER in list (0, 1, 2...).
- Top-level: order 1 = first, order 2 = second. Example: Dashboard=1, Data=2, Settings=4.
- Under same parent: order controls sibling order. Example: General=1, Menu=3, Extensions=4 under Settings.
- Check existing menus: find_records with filter parent.id._eq or no parent for top-level, then set order. To insert between items (e.g. after order 3): use order 4 and optionally update_records on the next item to shift it.

**Schema (get_table_details first):**
- type: "Menu" or "Dropdown Menu"
- label, icon (default "lucide:menu"), path (URL route, must start with /), order, isEnabled
- parent: many-to-one to menu_definition (optional, for nesting under Dropdown Menu)

**Workflow - Create top-level menu:**
1. get_table_details({"tableName":["menu_definition"]})
2. create_records({"table":"menu_definition","dataArray":[{"type":"Menu","label":"Reports","icon":"lucide:bar-chart","path":"/reports","order":5,"isEnabled":true}],"fields":"${idFieldName}"})

**Workflow - Create menu under parent (e.g. under Settings):**
1. find_records({"table":"menu_definition","filter":{"type":{"_eq":"Dropdown Menu"},"label":{"_eq":"Settings"}},"fields":"${idFieldName}","limit":1})
2. Extract parent ${idFieldName} from result
3. create_records with parent: {id: parentId} (SQL) or parent: {_id: parentId} (MongoDB). Example: {"table":"menu_definition","dataArray":[{"type":"Menu","label":"My Page","icon":"lucide:file","path":"/settings/my-page","order":10,"parent":{"${idFieldName}":parentId},"isEnabled":true}],"fields":"${idFieldName}"}

**Parent relation format:**
- SQL: parent: {id: number}
- MongoDB: parent: {_id: "..."} (ObjectId string)

**CRITICAL:**
- path must be UNIQUE across all menus
- icon: Lucide icon id, e.g. "lucide:bar-chart", "lucide:settings"
- order: lower number = higher in list

**After creating/updating menu:** Tell user to refresh the page (F5 or reload) to see the new menu in the sidebar. Menu changes may not appear until refresh.`;

  const menuOpsHint: HintContent = {
    category: 'menu_operations',
    title: 'Menu Operations (Navigation Sidebar)',
    content: menuOpsContent,
    tools: ['find_records', 'get_table_details', 'create_records', 'update_records'],
  };

  const extensionOpsContent = `**Extension Operations** – Create custom pages and widgets with Vue SFC. Extensions render when user navigates to menu path.

**CRITICAL - MUST call update_records to persist extension code:**
- Outputting Vue code in your text response does NOT update the extension. User will NOT see it.
- To update existing extension: find_records extension_definition → get id → update_records({"table":"extension_definition","updates":[{"id":X,"data":{"code":"<template>...</template>\\n<script setup>...</script>"}}]})
- NEVER say "I've updated" or "đã cập nhật" without having actually called update_records. If you didn't call the tool, the change was NOT saved.
- Same for create: MUST call create_records to create new extension – showing code is not enough.

**CRITICAL - Code is AUTO-COMPILED:** When create_records/update_records on extension_definition includes \`code\`, the server compiles Vue SFC to JS automatically. No separate tool needed. If compile fails, error message is returned - fix code and retry.

**Schema:**
- type: "page" (full page linked to menu) or "widget" (embed via <Widget :id="dbId" />)
- name, description, version (default "1.0.0"), isEnabled (default true)
- code: Vue SFC string (REQUIRED for page/widget with UI)
- menu: one-to-one relation to menu_definition (REQUIRED for type "page" - links extension to menu path)

**Workflow - Create menu + extension (full page):**
1. Create menu first: get_table_details + create_records menu_definition (e.g. path "/custom/analytics")
2. Find menu by path: find_records menu_definition filter path._eq "/custom/analytics" (or by label._eq "Dashboard")
3. create_records extension_definition with: name, type:"page", code (Vue SFC), menu:{id: menuId}
- CRITICAL: menu_definition has **label** not name. To find menu: filter by label or path, NEVER by name.

**Workflow - Create widget only (no menu):**
create_records extension_definition with: name, type:"widget", code (Vue SFC). No menu needed.

**Workflow - Update existing extension code (e.g. Welcome Page):**
1. find_records({"table":"extension_definition","filter":{"name":{"_eq":"Welcome Page"}},"fields":"id,name","limit":1}) – or filter by menu.path
2. update_records({"table":"extension_definition","updates":[{"id":<id from step 1>,"data":{"code":"<full Vue SFC string>"}}]})
3. User must refresh (F5) to see changes. Do NOT claim success without calling update_records.

**For consistent UI:** Call get_hint with category "ui_vibe" to get Enfyra design system (colors, layout, typography, spacing).

**Vue SFC - CRITICAL RULES (no imports, use globals):**
- NO import statements. All composables and Vue API are injected globally.
- Structure: <template>...</template> + <script setup>...</script>

**Available globally in extension code:**
- Vue: ref, reactive, computed, watch, onMounted, onUnmounted, etc.
- Composables: useToast, useApi, useEnfyraAuth, usePermissions, useHeaderActionRegistry, useRouter, useRoute, navigateTo
- Components: UButton, UCard, UInput, UTable, UBadge, FormEditor, DataTable, PermissionGate, Widget, etc.

**Header actions - CORRECT usage (pass actions directly, NOT .register()):**
\`\`\`javascript
useHeaderActionRegistry([
  { id: 'refresh', label: 'Refresh', onClick: refreshData, color: 'primary' }
]);
\`\`\`

**Minimal Vue SFC example:**
\`\`\`vue
<template>
  <div class="p-6">
    <h1 class="text-2xl font-bold">{{ title }}</h1>
    <UButton @click="handleClick">Click</UButton>
  </div>
</template>
<script setup>
const title = ref('My Extension');
const toast = useToast();
const handleClick = () => toast.add({ title: 'Clicked', color: 'green' });
</script>
\`\`\`

**API call – useApi requires execute():**
\`\`\`javascript
const { data, error, execute } = useApi('/user_definition', { query: { limit: 10 } });
onMounted(() => execute());  // Must call execute() - useApi does NOT auto-run
\`\`\`

**NPM packages (getPackages):** CRITICAL - Before writing extension code that uses a package:
1. Check if installed: find_records({"table":"package_definition","filter":{"type":{"_eq":"App"},"name":{"_eq":"dayjs"},"isEnabled":{"_eq":true}},"fields":"id,name","limit":1})
2. If NOT found: Tell user "Package [name] is required. Please install: Settings → Packages → Install Package → App Package → install [name]. Then retry." Agent CANNOT install. User MUST install manually.
3. Only use getPackages() in extension code AFTER confirming package exists in package_definition.
- Call inside onMounted or async handler (client-side only)
- Destructuring: const { dayjs, lodash } = await getPackages();
- With array (recommended): const packages = await getPackages(['dayjs', 'lodash']); then packages.dayjs, packages.lodash
- chart.js: const { Chart } = await getPackages(['chart.js']);
- Package names = npm names (dayjs, lodash, chart.js)

**If create_records fails with compile error:** Read error message (syntax, unknown component, etc.), fix the Vue SFC code, retry create_records or update_records.

**After creating/updating extension (or menu+extension):** Tell user to refresh the page (F5 or reload) to see the new menu and extension content. Changes may not appear until refresh.`;

  const extensionOpsHint: HintContent = {
    category: 'extension_operations',
    title: 'Extension Operations (Vue SFC Pages & Widgets)',
    content: extensionOpsContent,
    tools: ['find_records', 'get_table_details', 'create_records', 'update_records'],
  };

  const uiVibeContent = `**UI Vibe – Enfyra Design System** – Create extensions that match the app's visual style for consistency.

**Color palette (use via color= prop):**
- primary: violet/indigo gradient (main actions)
- success: emerald (positive, active)
- error: rose (danger, delete)
- warning: amber (caution)
- info: cyan (informational)
- neutral: slate (secondary, muted)

**Page layout – standard structure:**
\`\`\`vue
<div class="p-6 space-y-6">
  <!-- Header -->
  <div class="flex items-center justify-between">
    <div>
      <h1 class="text-3xl font-bold text-gray-800 dark:text-white/90">Page Title</h1>
      <p class="text-gray-500 dark:text-gray-400 mt-1">Optional description</p>
    </div>
    <UBadge variant="soft" color="primary">Status</UBadge>
  </div>

  <!-- Stats cards grid -->
  <div class="grid grid-cols-1 md:grid-cols-3 gap-6">
    <UCard>
      <div class="text-center p-4">
        <div class="text-2xl font-bold text-violet-600">{{ count }}</div>
        <div class="text-sm text-gray-500 dark:text-gray-400">Label</div>
      </div>
    </UCard>
  </div>

  <!-- Content sections -->
  <UCard>
    <template #header>
      <h3 class="text-lg font-semibold">Section Title</h3>
    </template>
    <div class="space-y-4">...</div>
  </UCard>
</div>
\`\`\`

**Components & styling:**
- UCard: use for sections; has #header slot. App uses rounded-2xl glass-card.
- UButton: color="primary" for main action, variant="soft" or "outline" for secondary, variant="ghost" for subtle.
- UBadge: variant="soft" for status labels; color matches context (success, error, info).
- UInput/UTextarea: use label prop; size="sm" often.
- EmptyState: use when no data – \`<EmptyState title="No items" description="Add first" :action="{ label: 'Add', onClick }" />\`

**Typography:**
- Page title: text-3xl font-bold text-gray-800 dark:text-white/90
- Section title: text-lg font-semibold
- Body: text-sm text-gray-800 dark:text-white/90
- Muted: text-gray-500 dark:text-gray-400

**Spacing:** p-6 for page padding, space-y-6 between sections, gap-6 in grids, space-y-4 in forms.

**Icons:** Lucide via UIcon – \`<UIcon name="lucide:bar-chart" />\`, lucide:settings, lucide:user, lucide:plus, etc.

**Responsive:** Use grid-cols-1 md:grid-cols-2 lg:grid-cols-3 for cards; flex flex-wrap gap-4 for button groups.`;

  const uiVibeHint: HintContent = {
    category: 'ui_vibe',
    title: 'UI Vibe (Enfyra Design System)',
    content: uiVibeContent,
    tools: [],
  };

  allHints.push(
    dbTypeHint,
    fieldOptHint,
    tableSchemaOpsHint,
    tableDeletionHint,
    handlerOpsHint,
    hookOpsHint,
    bootstrapOpsHint,
    websocketOpsHint,
    menuOpsHint,
    extensionOpsHint,
    uiVibeHint,
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
      'handler_operations',
      'hook_operations',
      'bootstrap_operations',
      'websocket_operations',
      'menu_operations',
      'extension_operations',
      'ui_vibe',
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

