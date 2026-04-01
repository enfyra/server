/**
 * Compact tool definitions for Enfyra AI agent (token-efficient).
 * Canonical product rules: Enfyra MCP mcp-instructions + lazy get_enfyra_doc sections.
 */

export interface ToolDefinition {
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
    name: 'get_enfyra_doc',
    description:
      'Load MCP-aligned Enfyra rules on demand (REST shape, routes vs tables, GraphQL, extensions, etc.). Call with no args to list section ids; pass section or sections to fetch text. Prefer this over guessing when unsure.',
    parameters: {
      type: 'object',
      properties: {
        section: { type: 'string', description: 'Single section id from availableSections when listed with no args.' },
        sections: {
          type: 'array',
          items: { type: 'string' },
          description: 'Multiple section ids to fetch in one call.',
        },
      },
    },
  },
  {
    name: 'get_table_details',
    description:
      'Schema for one or more tables: columns, relations, table id. Pass tableName as array (e.g. ["user_definition"]). Max 5 tables; filter table_definition first if listing many. Reuse result in the same turn—do not repeat. Output uses compact columns.data / relations.data arrays—map with fields[].',
    parameters: {
      type: 'object',
      properties: {
        tableName: {
          type: 'array',
          items: { type: 'string' },
          description: 'Table names, e.g. ["post"]',
        },
        forceRefresh: { type: 'boolean', description: 'Reload metadata from DB' },
      },
      required: ['tableName'],
    },
  },
  {
    name: 'get_hint',
    description:
      'Workflow hints by category (handler_operations, routes_endpoints, extension_operations, crud_write_operations, …). Use when get_enfyra_doc is not enough for step-by-step Enfyra-specific procedures.',
    parameters: {
      type: 'object',
      properties: {
        category: {
          oneOf: [
            { type: 'string' },
            { type: 'array', items: { type: 'string' } },
          ],
          description: 'Hint category or array of categories; omit for overview.',
        },
      },
    },
  },
  {
    name: 'create_tables',
    description:
      'Create tables sequentially (one table per call recommended—schema lock). Include id column (int+generated for SQL, uuid for Mongo). Auto default route /{name} is created; for custom paths use route records + handlers, not extra empty tables. See get_enfyra_doc route_vs_table.',
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
    description:
      'PATCH table schema (columns/relations merge). One table per call when possible. Relation type cannot change—delete relation then add new.',
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
    description:
      'Destructive: drop table metadata and physical table. Requires numeric table ids from table_definition—never guess. Confirm with user first.',
    parameters: {
      type: 'object',
      properties: {
        ids: { type: 'array', items: { type: 'number' }, description: 'table_definition ids' },
      },
      required: ['ids'],
    },
  },
  {
    name: 'get_task',
    description: 'Read current conversation task (if any). Optional conversationId defaults from context.',
    parameters: {
      type: 'object',
      properties: {
        conversationId: { oneOf: [{ type: 'string' }, { type: 'number' }] },
      },
    },
  },
  {
    name: 'update_task',
    description: 'Create/update task state for long workflows (pending|in_progress|completed|failed|cancelled).',
    parameters: {
      type: 'object',
      properties: {
        conversationId: { oneOf: [{ type: 'string' }, { type: 'number' }] },
        type: { type: 'string', enum: ['create_tables', 'update_tables', 'delete_tables', 'custom'] },
        status: { type: 'string', enum: ['pending', 'in_progress', 'completed', 'cancelled', 'failed'] },
        data: { type: 'object' },
        result: { type: 'object' },
        error: { type: 'string' },
        priority: { type: 'number' },
      },
      required: ['conversationId', 'type', 'status'],
    },
  },
  {
    name: 'find_records',
    description:
      'Query any table with filter, fields (required, minimal), limit, sort, page. Count: fields=id, limit=1, meta=totalCount or filterCount. No column_definition table route—use get_enfyra_doc column_definition. Operators: _eq _neq _gt _gte _lt _lte _contains _in _and _or …',
    parameters: {
      type: 'object',
      properties: {
        table: { type: 'string' },
        filter: { type: 'object' },
        fields: { type: 'string' },
        limit: { type: 'number' },
        sort: { type: 'string' },
        page: { type: 'number' },
        meta: { type: 'string', enum: ['totalCount', 'filterCount'] },
      },
      required: ['table', 'fields'],
    },
  },
  {
    name: 'create_records',
    description:
      'Batch create. Omit id. Relations: use propertyName {id}, not FK column names. For route_definition rows follow Enfyra API: new routes need path + mainTable link + methods as your server expects (see get_enfyra_doc rest_routes).',
    parameters: {
      type: 'object',
      properties: {
        table: { type: 'string' },
        dataArray: { type: 'array', items: { type: 'object' } },
        fields: { type: 'string' },
      },
      required: ['table', 'dataArray'],
    },
  },
  {
    name: 'update_records',
    description: 'Batch update by id. Same relation rules as create. Minimal fields in response.',
    parameters: {
      type: 'object',
      properties: {
        table: { type: 'string' },
        updates: {
          type: 'array',
          items: {
            type: 'object',
            properties: {
              id: { oneOf: [{ type: 'string' }, { type: 'number' }] },
              data: { type: 'object' },
            },
            required: ['id', 'data'],
          },
        },
        fields: { type: 'string' },
      },
      required: ['table', 'updates'],
    },
  },
  {
    name: 'delete_records',
    description: 'Delete rows by id. For dropping whole tables use delete_tables.',
    parameters: {
      type: 'object',
      properties: {
        table: { type: 'string' },
        ids: { type: 'array', items: { oneOf: [{ type: 'string' }, { type: 'number' }] } },
      },
      required: ['table', 'ids'],
    },
  },
  {
    name: 'run_handler_test',
    description:
      'Sandbox-test handler logic with #table_name repos, @BODY @PARAMS @QUERY.filter. Use before saving route_handler_definition. Read fixGuidance on failure.',
    parameters: {
      type: 'object',
      properties: {
        table: { type: 'string' },
        handlerCode: { type: 'string' },
        body: { type: 'object' },
        params: { type: 'object' },
        query: { type: 'object' },
        timeoutMs: { type: 'number' },
      },
      required: ['table', 'handlerCode'],
    },
  },
];
