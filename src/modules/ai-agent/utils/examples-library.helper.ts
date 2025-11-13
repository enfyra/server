export interface ToolExample {
  id: string;
  scenario: string;
  userMessage: string;
  correctApproach: string;
  toolCalls: Array<{
    tool: string;
    args: any;
    reasoning?: string;
  }>;
}

export interface ExampleCategory {
  name: string;
  keywords: string[];
  examples: ToolExample[];
}

export const EXAMPLE_CATEGORIES: Record<string, ExampleCategory> = {
  find_queries: {
    name: 'Find/List Queries',
    keywords: ['show', 'list', 'get', 'find', 'display', 'view', 'see'],
    examples: [
      {
        id: 'FIND-01',
        scenario: 'Simple list with field selection',
        userMessage: 'Show me all posts',
        correctApproach: 'Call get_fields first, then find with minimal fields and limit=0 only when needed.',
        toolCalls: [
          {
            tool: 'get_fields',
            args: { tableName: 'post' },
            reasoning: 'Get available fields first (lightweight, fast)'
          },
          {
            tool: 'dynamic_repository',
            args: {
              table: 'post',
              operation: 'find',
              fields: 'id,title,createdAt',
              limit: 0
            },
            reasoning: 'Fetch only needed fields, limit=0 for "all"'
          }
        ]
      },
      {
        id: 'FIND-02',
        scenario: 'Filtered query with sort',
        userMessage: 'Show me the last 5 posts by createdAt',
        correctApproach: 'Fetch fields, then use sort=-createdAt and limit=5.',
        toolCalls: [
          {
            tool: 'get_fields',
            args: { tableName: 'post' }
          },
          {
            tool: 'dynamic_repository',
            args: {
              table: 'post',
              operation: 'find',
              fields: 'id,title,content,createdAt',
              sort: '-createdAt',
              limit: 5
            },
            reasoning: 'Use -createdAt for descending sort (newest first)'
          }
        ]
      },
      {
        id: 'FIND-03',
        scenario: 'Count query (optimized with meta)',
        userMessage: 'How many users do we have?',
        correctApproach: 'Use limit=1 with meta=totalCount instead of limit=0.',
        toolCalls: [
          {
            tool: 'dynamic_repository',
            args: {
              table: 'user_definition',
              operation: 'find',
              fields: 'id',
              limit: 1,
              meta: 'totalCount'
            },
            reasoning: '✅ FAST: limit=1 + meta=totalCount fetches 1 record + count. ❌ SLOW: limit=0 fetches ALL records'
          }
        ]
      }
    ]
  },

  batch_operations: {
    name: 'Batch Create/Update/Delete',
    keywords: ['create 10', 'add 20', 'delete multiple', 'update many', '5', '10', '20', '100'],
    examples: [
      {
        id: 'BATCH-01',
        scenario: 'Create multiple records',
        userMessage: 'Create 10 sample posts with titles',
        correctApproach: 'Confirm schema, then call batch_create with dataArray.',
        toolCalls: [
          {
            tool: 'get_table_details',
            args: { tableName: 'post' },
            reasoning: 'Need exact column names and types for create operation'
          },
          {
            tool: 'dynamic_repository',
            args: {
              table: 'post',
              operation: 'batch_create',
              dataArray: [
                { title: 'Post 1', content: 'Content 1' },
                { title: 'Post 2', content: 'Content 2' }
              ],
              fields: 'id'
            },
            reasoning: 'One batch_create call handles all records efficiently. Always specify fields parameter (e.g., "id") to save tokens'
          }
        ]
      },
      {
        id: 'BATCH-02',
        scenario: 'Update multiple records',
        userMessage: 'Update price for products 1, 2, and 3',
        correctApproach: 'Use batch_update with updates array, not multiple single updates.',
        toolCalls: [
          {
            tool: 'dynamic_repository',
            args: {
              table: 'product',
              operation: 'batch_update',
              updates: [
                { id: 1, data: { price: 100 } },
                { id: 2, data: { price: 200 } },
                { id: 3, data: { price: 300 } }
              ],
              fields: 'id'
            },
            reasoning: 'Use batch_update instead of looping single updates. Always specify fields parameter (e.g., "id") to save tokens'
          }
        ]
      }
    ]
  },

  nested_relations: {
    name: 'Nested Relations',
    keywords: ['with', 'and their', 'including', 'along with', 'related'],
    examples: [
      {
        id: 'REL-01',
        scenario: 'Get related data in one query',
        userMessage: 'Show me routes with their roles',
        correctApproach: 'Use relation.field fields and a single find call.',
        toolCalls: [
          {
            tool: 'get_fields',
            args: { tableName: 'route_definition' }
          },
          {
            tool: 'dynamic_repository',
            args: {
              table: 'route_definition',
              operation: 'find',
              fields: 'id,path,roles.id,roles.name',
              limit: 0
            },
            reasoning: 'Nested fields (roles.name) fetch related data in ONE query'
          }
        ]
      },
      {
        id: 'REL-02',
        scenario: 'Filter by related data',
        userMessage: 'Find routes that have Admin role',
        correctApproach: 'Nest the filter under the relation in the where clause.',
        toolCalls: [
          {
            tool: 'dynamic_repository',
            args: {
              table: 'route_definition',
              operation: 'find',
              where: { roles: { name: { _eq: 'Admin' } } },
              fields: 'id,path,roles.name',
              limit: 0
            },
            reasoning: 'Nested where filter by related data (roles.name)'
          }
        ]
      }
    ]
  },

  uncertainty_handling: {
    name: 'Uncertainty & Self-Awareness',
    keywords: ['not sure', 'unsure', 'confused', 'how to', 'what is'],
    examples: [
      {
        id: 'UNC-01',
        scenario: 'Uncertain about nested relations',
        userMessage: 'Show me routes with their roles',
        correctApproach: 'When confidence <80%, call get_hint before acting.',
        toolCalls: [
          {
            tool: 'get_hint',
            args: { category: 'nested_relations' },
            reasoning: 'Confidence <80% on nested fields syntax. Call get_hint BEFORE attempting query'
          },
          {
            tool: 'get_fields',
            args: { tableName: 'route_definition' },
            reasoning: 'After learning from hint, get available fields'
          },
          {
            tool: 'dynamic_repository',
            args: {
              table: 'route_definition',
              operation: 'find',
              fields: 'id,path,roles.id,roles.name',
              limit: 0
            },
            reasoning: 'Now confident with nested fields syntax from hint'
          }
        ]
      },
      {
        id: 'UNC-02',
        scenario: 'Error occurred - not sure why',
        userMessage: 'Get post with author name (returns error: unknown field)',
        correctApproach: 'Call get_hint to diagnose the error, then re-check fields.',
        toolCalls: [
          {
            tool: 'get_hint',
            args: { category: 'error_handling' },
            reasoning: 'Error about unknown field. Need guidance on field selection'
          },
          {
            tool: 'get_fields',
            args: { tableName: 'post' },
            reasoning: 'Check actual available fields after learning from hint'
          }
        ]
      }
    ]
  },

  permission_checks: {
    name: 'Permission Checks',
    keywords: ['create', 'add', 'insert', 'update', 'modify', 'delete', 'remove'],
    examples: [
      {
        id: 'PERM-01',
        scenario: 'Check permission before create',
        userMessage: 'Create a new post with title "Hello"',
        correctApproach: 'Always run check_permission first, then fetch schema, then perform the write.',
        toolCalls: [
          {
            tool: 'check_permission',
            args: {
              table: 'post',
              operation: 'create'
            },
            reasoning: 'ALWAYS check permission BEFORE any write operation'
          },
          {
            tool: 'get_table_details',
            args: { tableName: 'post' },
            reasoning: 'Get exact column names for create'
          },
          {
            tool: 'dynamic_repository',
            args: {
              table: 'post',
              operation: 'create',
              data: { title: 'Hello', content: 'World' },
              fields: 'id'
            },
            reasoning: 'Only proceed if permission check returned allowed=true. Always specify fields parameter to save tokens (e.g., "id" or "id,name")'
          }
        ]
      },
      {
        id: 'PERM-02',
        scenario: 'Check permission before delete',
        userMessage: 'Delete route with ID 5',
        correctApproach: 'Permission check precedes any delete call.',
        toolCalls: [
          {
            tool: 'check_permission',
            args: {
              table: 'route_definition',
              operation: 'delete'
            },
            reasoning: 'Permission check required before delete (critical operation)'
          },
          {
            tool: 'dynamic_repository',
            args: {
              table: 'route_definition',
              operation: 'delete',
              id: 5
            },
            reasoning: 'Delete only if allowed=true'
          }
        ]
      }
    ]
  },

  table_operations: {
    name: 'Create/Modify Tables',
    keywords: ['create table', 'add table', 'modify table', 'add column', 'new table', 'delete table', 'recreate', 'drop table'],
    examples: [
      {
        id: 'TABLE-01',
        scenario: 'Create new table',
        userMessage: 'Create a products table with name and price',
        correctApproach: 'Verify existing tables, inspect schema reference, then create with correct id column.',
        toolCalls: [
          {
            tool: 'dynamic_repository',
            args: {
              table: 'table_definition',
              operation: 'find',
              where: { name: { _eq: 'products' } },
              fields: 'id,name',
              limit: 1
            },
            reasoning: 'Check if table already exists'
          },
          {
            tool: 'get_table_details',
            args: { tableName: 'user_definition' },
            reasoning: 'Get reference schema to understand structure'
          },
          {
            tool: 'dynamic_repository',
            args: {
              table: 'table_definition',
              operation: 'create',
              data: {
                name: 'products',
                description: 'Products table',
                columns: [
                  { name: 'id', type: 'int', isPrimary: true, isGenerated: true },
                  { name: 'name', type: 'varchar', isNullable: false },
                  { name: 'price', type: 'decimal', isNullable: false }
                ]
              },
              fields: 'id,name'
            },
            reasoning: 'MUST include id column with isPrimary=true'
          }
        ]
      },
      {
        id: 'TABLE-02',
        scenario: 'Recreate tables with M2M relation',
        userMessage: 'Delete and recreate post and category tables with M2M relation',
        correctApproach: 'Find existing tables → Delete → Create new tables → Fetch IDs → Create M2M relation on ONE side',
        toolCalls: [
          {
            tool: 'dynamic_repository',
            args: {
              table: 'table_definition',
              operation: 'find',
              where: { name: { _in: ['post', 'category'] } },
              fields: 'id,name',
              limit: 0
            },
            reasoning: 'Find both tables in ONE query (efficient)'
          },
          {
            tool: 'check_permission',
            args: { table: 'post', operation: 'delete' },
            reasoning: 'Permission check before delete'
          },
          {
            tool: 'check_permission',
            args: { table: 'category', operation: 'delete' },
            reasoning: 'Permission check before delete'
          },
          {
            tool: 'dynamic_repository',
            args: {
              table: 'table_definition',
              operation: 'batch_delete',
              ids: ['post_table_id', 'category_table_id']
            },
            reasoning: 'Delete both tables in one batch_delete call (efficient)'
          },
          {
            tool: 'dynamic_repository',
            args: {
              table: 'table_definition',
              operation: 'create',
              data: {
                name: 'post',
                description: 'Blog posts',
                columns: [
                  { name: 'id', type: 'int', isPrimary: true, isGenerated: true },
                  { name: 'title', type: 'varchar', isNullable: false },
                  { name: 'content', type: 'text', isNullable: true }
                ]
              },
              fields: 'id,name'
            },
            reasoning: 'Create post table with all columns in ONE call'
          },
          {
            tool: 'dynamic_repository',
            args: {
              table: 'table_definition',
              operation: 'create',
              data: {
                name: 'category',
                description: 'Categories',
                columns: [
                  { name: 'id', type: 'int', isPrimary: true, isGenerated: true },
                  { name: 'name', type: 'varchar', isNullable: false },
                  { name: 'description', type: 'text', isNullable: true }
                ]
              },
              fields: 'id,name'
            },
            reasoning: 'Create category table. Always specify fields parameter to save tokens'
          },
          {
            tool: 'dynamic_repository',
            args: {
              table: 'table_definition',
              operation: 'find',
              where: { name: { _in: ['post', 'category'] } },
              fields: 'id,name',
              limit: 0
            },
            reasoning: 'Fetch newly created table IDs for relation'
          },
          {
            tool: 'dynamic_repository',
            args: {
              table: 'table_definition',
              operation: 'update',
              id: 'new_post_table_id',
              data: {
                relations: [
                  {
                    propertyName: 'categories',
                    type: 'many-to-many',
                    targetTable: { id: 'new_category_table_id' },
                    inversePropertyName: 'posts'
                  }
                ]
              },
              fields: 'id'
            },
            reasoning: 'Create M2M relation on POST side only (system handles inverse). Always specify fields parameter to save tokens'
          }
        ]
      }
    ]
  }
};

export function detectIntent(userMessage: string): string[] {
  const message = userMessage.toLowerCase();
  const detectedCategories: string[] = [];

  for (const [categoryKey, category] of Object.entries(EXAMPLE_CATEGORIES)) {
    const hasKeyword = category.keywords.some(keyword =>
      message.includes(keyword.toLowerCase())
    );

    if (hasKeyword) {
      detectedCategories.push(categoryKey);
    }
  }

  if (detectedCategories.length === 0) {
    detectedCategories.push('find_queries');
  }

  return detectedCategories;
}

export function getRelevantExamples(userMessage: string): ToolExample[] {
  const intents = detectIntent(userMessage);
  const examples: ToolExample[] = [];

  for (const intent of intents) {
    const category = EXAMPLE_CATEGORIES[intent];
    if (category) {
      examples.push(...category.examples.slice(0, 2));
    }

    if (examples.length >= 5) {
      break;
    }
  }

  return examples.slice(0, 5);
}

export function formatExamplesForPrompt(examples: ToolExample[]): string {
  if (examples.length === 0) {
    return '';
  }

  const formatted = examples.map((example, index) => {
    const toolCallsStr = example.toolCalls
      .map(tc => `  - ${tc.tool}(${JSON.stringify(tc.args)})${tc.reasoning ? ` — ${tc.reasoning}` : ''}`)
      .join('\n');

    return `**[${example.id}] ${example.scenario}**
• User: "${example.userMessage}"
• Approach: ${example.correctApproach}
• Tools:
${toolCallsStr}`;
  }).join('\n\n---\n\n');

  return `
## Few-Shot Examples (closest patterns)

${formatted}

Key reminders:
- Confidence <80% → call get_hint before acting
- get_fields precedes find queries to avoid invalid selections
- Count queries: limit=1 + meta="totalCount"
- Use batch_* for 5+ records; never loop single calls
- Always check_permission before create/update/delete
- Prefer nested fields for related data
- Match exact column names from get_table_details for writes

---
`;
}
