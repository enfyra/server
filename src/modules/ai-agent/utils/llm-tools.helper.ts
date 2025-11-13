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
- relations[] → {propertyName,type,targetTable:{id,name},inversePropertyName?,cascade}
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
    description: `Purpose → pull focused guidance when confidence drops.

Call when:
- Confidence <80% on syntax, permissions, relations, or M2M steps
- A tool returned an error you cannot explain
- You need a checklist before attempting an operation

Categories:
- permission_check, nested_relations, route_access, database_type, relations, metadata, field_optimization, table_operations, error_handling, table_discovery

Rule: if any checklist fails, stop and call get_hint before retrying.

Returns:
- dbType, idField (context-aware)
- hints[] → each {category,title,content,examples?}
- availableCategories (string[])

Example request:
{"category":"nested_relations"}`,
    parameters: {
      type: 'object',
      properties: {
        category: {
          type: 'string',
          description:
            'Hint category: permission_check, database_type, relations, metadata, field_optimization, table_operations, error_handling, table_discovery, nested_relations, route_access. Omit for all.',
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
4. If a step is unclear, call get_hint(category="...") before using this tool.
5. Reuse tool outputs from this turn; only repeat a call if the scope or filters change.
6. Table structure changes (columns/relations/indexes) must target table_definition / column_definition / relation_definition only; modifying actual data rows belongs to the non-definition tables.

Read (find):
- Query only the fields the user requested.
- For counts → limit=1 + meta="totalCount" (faster than limit=0).
- Use nested filters/fields (e.g., "roles.name") instead of multiple queries.

Write (create/update):
- Match schema returned by get_table_details.
- Relations (one-to-one, one-to-many, many-to-many): update exactly one table_definition with data.relations (merge existing entries and add {propertyName,type,targetTable:{id},inversePropertyName?}); never issue a mirrored update on the inverse table.
- Removing M2M: rewrite relations array on the surviving table.

Batch:
- Use batch_* for operations on ≥5 records to avoid multiple calls.

Metadata workflow examples:
- Create table: dynamic_repository.create on table_definition with {name, description, columns: [...], relations: [...]}.
- Drop table: find table_definition by name, then delete via dynamic_repository.delete using that id (include meta="human_confirmed" after user confirmation). Always remind the user to reload the admin UI.
- Add relation: find table_definition id, fetch current relations.*, merge new relation object, update that table_definition once.

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
            'Operation to perform: "find" (read records), "create" (insert one), "update" (modify one by id), "delete" (remove one by id), "batch_create" (insert many), "batch_update" (modify many), "batch_delete" (remove many). Use batch_* for 5+ records. NO "findOne" operation - use "find" with limit=1 instead.',
        },
        where: {
          type: 'object',
          description:
            'Filter conditions for find/update/delete operations. Supports operators such as _eq,_neq,_gt,_gte,_lt,_lte,_like,_ilike,_contains,_starts_with,_ends_with,_between,_in,_not_in,_is_null,_is_not_null as well as nested logical blocks (_and,_or,_not).',
        },
        fields: {
          type: 'string',
          description:
            'Fields to return. Use get_fields for available fields, then specify ONLY needed fields (e.g., "id" for count, "id,name" for list). Supports wildcards like "columns.*", "relations.*"',
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

