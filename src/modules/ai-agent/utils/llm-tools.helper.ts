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

Use when:
- Handling read/create/update/delete on protected data
- User targets restricted tables or admin routes
- No cached check_permission result exists for the same table/route and operation in this response

Skip when:
- Only calling get_metadata, get_table_details, get_fields, get_hint
- Answering casual questions without touching data
- A matching check_permission result already exists in this response (reuse it instead of calling again)

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
    description: `Purpose → load the full schema (columns, relations, indexes, constraints).

Use when:
- Preparing create/update payloads that must match schema exactly
- Investigating relation structure or constraints

Skip when:
- You only need field names → prefer get_fields

Inputs:
- tableName (required) → exact table identifier as stored (case-sensitive)
- forceRefresh (optional) true to reload metadata

Response highlights:
- name, description, isSingleRecord, database type info
- columns[] → {name,type,isNullable,isPrimary,defaultValue,isUnique}
- relations[] → {propertyName,type,targetTable:{id:<REAL_ID_FROM_FIND>},inversePropertyName?,cascade?}
- CRITICAL: targetTable.id MUST be REAL ID from database. ALWAYS find table_definition by name first to get current ID. NEVER use IDs from history.
- indexes[] / uniques[] definitions

Example:
{"tableName":"user_definition"}`,
    parameters: {
      type: 'object',
      properties: {
        tableName: {
          type: 'string',
          description: 'REQUIRED. The exact name of the table (e.g., "user_definition", "post", "route_definition"). Extract this from the user message.',
        },
        forceRefresh: {
          type: 'boolean',
          description: 'Optional. Set to true to reload metadata from database. Default: false.',
          default: false,
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
    description: `Purpose → CRITICAL fallback tool for comprehensive guidance when uncertain or confused. This is your SAFETY NET and KNOWLEDGE BASE.

CRITICAL: Call get_hint IMMEDIATELY when:
- Confidence drops below 100% on ANY operation (syntax, permissions, relations, M2M, batch operations, etc.)
- You encounter an error you don't understand or cannot explain
- You're unsure about the correct workflow or sequence of steps
- You need detailed checklists before attempting complex operations (table creation, relation setup, batch operations)
- User asks about something you're not 100% certain about
- Tool returns unexpected results and you need guidance

STRATEGY: When in doubt, call get_hint FIRST before attempting operations. It's better to spend one extra tool call to get guidance than to make mistakes that waste tokens and cause errors.

Available categories (call with single category string, array of categories for multiple topics, or omit for all):
- table_operations → Table creation, relations, batch operations, workflows (MOST IMPORTANT for Enfyra)
- permission_check → Permission flows and route access
- field_optimization → Field selection, nested relations, query optimization
- database_type → Database-specific context (ID fields, types)
- error_handling → Error protocols and recovery
- table_discovery → Finding and identifying tables
- complex_workflows → Step-by-step workflows for complex tasks

TIP: When you need guidance on multiple related topics, use array format: {"category":["table_operations","permission_check"]} to get hints for multiple categories in one call.

Returns:
- dbType, idField (context-aware for current database)
- hints[] → Comprehensive guidance with examples, checklists, and workflows
- availableCategories (string[])

Example requests:
- {"category":"table_operations"} → Get guidance on table creation, relations, batch operations
- {"category":["table_operations","permission_check"]} → Get multiple categories at once
- {"category":"permission_check"} → Get guidance on permission flows
- {} → Get ALL hints (use when completely confused or need comprehensive overview)

REMEMBER: get_hint is your best friend. When confused, uncertain, or encountering errors → call get_hint immediately. Don't guess - get guidance.`,
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
    name: 'dynamic_repository',
    description: `Purpose → single gateway for CRUD and batch operations.

Planning checklist:
1. Run check_permission for any read/create/update/delete on business tables.
2. Use get_table_details / get_fields to understand columns or relations before writing.
3. Metadata requests (tables/columns/relations/routes) must operate on *_definition tables only; never scan the data tables.
4. CRITICAL: If ANY step is unclear, confusing, or you're uncertain → STOP and call get_hint(category="...") BEFORE using this tool. get_hint provides comprehensive guidance, examples, and checklists. Don't guess - get guidance first.
5. Reuse tool outputs from this turn; only repeat a call if the scope or filters change.
6. Table structure changes (columns/relations/indexes) must target table_definition / column_definition / relation_definition only; modifying actual data rows belongs to the non-definition tables.

Read (find):
- Query only the fields the user requested.
- For counts → limit=1 + meta="totalCount" (faster than limit=0).
- Use nested filters/fields (e.g., "roles.name") instead of multiple queries.

Write (create/update):
- Match schema returned by get_table_details.
- CRITICAL: After create/update, only fetch essential fields (e.g., "id,name" or just "id") using the fields parameter. Do NOT fetch all fields - this wastes tokens. Example: create({data: {...}, fields: "id,name"}) or update({id: X, data: {...}, fields: "id"}).
- Relations (one-to-one, one-to-many, many-to-many): CRITICAL WORKFLOW - 1) Find source table ID by name, 2) Find target table ID by name, 3) Verify both exist, 4) Fetch current relations from source table, 5) Merge new relation with existing, 6) Update source table with REAL IDs. NEVER use IDs from history. targetTable.id MUST be REAL ID from find result (e.g., {"propertyName":"orderItems","type":"one-to-many","targetTable":{"id":<REAL_ID_FROM_FIND>},"inversePropertyName":"order","cascade":true}). One-to-many MUST include inversePropertyName. Many-to-many also requires inversePropertyName. Never issue a mirrored update on the inverse table.
- Removing M2M: rewrite relations array on the surviving table.

Batch:
- Use batch_delete for 2+ delete operations (collect ALL IDs from find, then batch_delete with ids array).
- Use batch_create/batch_update for 5+ create/update operations.
- CRITICAL: When find returns multiple records, you MUST use batch operations with ALL collected IDs, not individual calls.
- EXCEPTION: For table_definition operations (creating/updating tables), do NOT use batch operations. Process each table sequentially (one create/update at a time). Batch operations are ONLY for data tables, NOT for metadata tables.

Metadata workflow examples:
- Create table: CRITICAL - First check if table exists by finding table_definition by name. If exists, skip creation. If not exists, create with {data: {name, description, columns: [...], relations: [...]}, fields: "id,name"}. CRITICAL: Do NOT include createdAt or updatedAt in columns array - system automatically adds them. Always include fields parameter (e.g., "id" or "id,name") to save tokens.
- Drop table: find table_definition by name, then delete via dynamic_repository.delete using {id: tableId} (include meta="human_confirmed" after user confirmation). Always remind the user to reload the admin UI.
- Add relation: CRITICAL WORKFLOW - 1) Find SOURCE table ID by name, 2) Find TARGET table ID by name, 3) Verify both IDs exist, 4) Fetch current columns.* and relations.* from source table to check for FK column conflicts, 5) Check FK column conflict: system generates FK column from propertyName using camelCase (e.g., "user" → "userId", "customer" → "customerId", "order" → "orderId"). CRITICAL: If table already has column "user_id"/"userId", "order_id"/"orderId", "customer_id"/"customerId", "product_id"/"productId" (check both snake_case and camelCase), you MUST use different propertyName (e.g., "buyer" instead of "customer"). If conflict exists, STOP and report error - do NOT proceed, 6) Merge new relation with ALL existing relations (preserve system relations), 7) Update ONLY the source table_definition with merged relations. CRITICAL: NEVER update both source and target tables - this causes duplicate FK column errors. System automatically handles inverse relation, FK column creation, and junction table. You only need to update ONE table. NEVER use IDs from history. NEVER use placeholder IDs. MUST use REAL IDs from find results. For system tables, preserve ALL existing system relations.

Safety notes:
- Do not mutate file_definition; it is read-only.
- System tables (user_definition, role_definition, route_definition, etc.) should be extended, not rewritten—only add new columns/relations.
    - Avoid redundant find calls (e.g., scanning \`post\`) when working on metadata; target *_definition tables directly.
- Surface permission errors clearly to the user.`,
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

