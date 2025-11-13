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

Skip when:
- Only calling get_metadata, get_table_details, get_fields, get_hint
- Answering casual questions without touching data

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

Before calling:
- check_permission is mandatory for every read/create/update/delete
- There is no findOne; use find + where + limit=1
- If unsure about syntax, grab a relevant get_hint first

Usage patterns:
- Read: request the minimal fields, use meta=totalCount for count queries
- Write: match schema from get_table_details before sending data
- Batch: more than four records → prefer batch_* once instead of looping

Relations & optimization:
- Use nested fields/filters (e.g., "roles.name") instead of multiple queries
- Many-to-many: update exactly one side, targetTable {"id": value} or {"_id": value}, inversePropertyName handles the reverse link
- Removing M2M: update the surviving table and rewrite its relations array

Additional notes:
- Do not create/update/delete on file_definition (read-only through this tool)
- Surface permission denials clearly to the user
- See examples library: categories nested_relations, batch_operations

Request parameters:
- table (required string) → exact table to target
- operation (required) → "find" | "create" | "update" | "delete" | "batch_create" | "batch_update" | "batch_delete"
- where (object) → filters support operators such as _eq,_neq,_gt,_gte,_lt,_lte,_like,_ilike,_contains,_starts_with,_ends_with,_between,_in,_not_in,_is_null,_is_not_null and nested logic (_and,_or,_not). Nest objects for relations: {"roles":{"name":{"_eq":"Admin"}}}
- fields (string) → comma-separated field list, supports dot paths (relation.field) and wildcards ("relations.*")
- limit (number) → 0 (all) or positive integer (default 10). For counts combine limit=1 + meta="totalCount".
- sort (string) → comma-separated terms, prefix "-" for desc (e.g., "-createdAt,name")
- meta (string) → "totalCount" | "filterCount" | "*" to include metadata blocks
- data (object) → payload for create/update (respect schema, include relations arrays when needed)
- id (string|number) → required for update/delete when not using batch
- dataArray (object[]) → records for batch_create
- updates (object[]) → [{id,data}] for batch_update
- ids (array) → list of ids for batch_delete

Examples:
Find:
{"table":"route_definition","operation":"find","fields":"id,path,roles.name","where":{"roles":{"name":{"_eq":"Admin"}}},"limit":5}
Create:
{"table":"post","operation":"create","data":{"title":"Hello","status":"draft"}}
Update:
{"table":"post","operation":"update","id":12,"data":{"status":"published"}}
Batch delete:
{"table":"product","operation":"batch_delete","ids":[1,2,3,4]}`,
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

