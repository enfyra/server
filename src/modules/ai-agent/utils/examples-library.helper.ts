/**
 * Few-Shot Examples Library
 *
 * Provides concrete examples for LLM to improve tool selection accuracy
 * and parameter formatting. Examples are injected into system prompt based
 * on detected user intent.
 *
 * Best Practice: Anthropic & OpenAI recommend 2-5 examples per pattern
 */

export interface ToolExample {
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

/**
 * Example Categories - Each contains 2-3 representative examples
 */
export const EXAMPLE_CATEGORIES: Record<string, ExampleCategory> = {

  // ============================================
  // 1. FIND QUERIES (Most Common)
  // ============================================
  find_queries: {
    name: 'Find/List Queries',
    keywords: ['show', 'list', 'get', 'find', 'display', 'view', 'see'],
    examples: [
      {
        scenario: 'Simple list with field selection',
        userMessage: 'Show me all posts',
        correctApproach: 'Step 1: Get field names. Step 2: Query with selected fields only',
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
        scenario: 'Filtered query with sort',
        userMessage: 'Show me the last 5 posts by createdAt',
        correctApproach: 'Get fields, then query with sort=-createdAt and limit=5',
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
        scenario: 'Count query (OPTIMIZED with meta)',
        userMessage: 'How many users do we have?',
        correctApproach: 'Use meta=totalCount with limit=1 (100x faster than limit=0)',
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

  // ============================================
  // 2. BATCH OPERATIONS (5+ records)
  // ============================================
  batch_operations: {
    name: 'Batch Create/Update/Delete',
    keywords: ['create 10', 'add 20', 'delete multiple', 'update many', '5', '10', '20', '100'],
    examples: [
      {
        scenario: 'Create multiple records',
        userMessage: 'Create 10 sample posts with titles',
        correctApproach: 'Step 1: Get exact column names. Step 2: Use batch_create with dataArray',
        toolCalls: [
          {
            tool: 'get_table_details',
            args: { tableName: 'post' },
            reasoning: 'Need exact column names and types for create operation'
          },
          {
            tool: 'batch_create',
            args: {
              table: 'post',
              operation: 'batch_create',
              dataArray: [
                { title: 'Post 1', content: 'Content 1' },
                { title: 'Post 2', content: 'Content 2' }
                // ... 10 items total
              ]
            },
            reasoning: 'ONE batch_create call handles all 10 records efficiently'
          }
        ]
      },
      {
        scenario: 'Update multiple records',
        userMessage: 'Update price for products 1, 2, and 3',
        correctApproach: 'Use batch_update with updates array',
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
              ]
            },
            reasoning: 'batch_update for multiple ID updates (not a loop!)'
          }
        ]
      }
    ]
  },

  // ============================================
  // 3. NESTED RELATIONS (Common mistake)
  // ============================================
  nested_relations: {
    name: 'Nested Relations',
    keywords: ['with', 'and their', 'including', 'along with', 'related'],
    examples: [
      {
        scenario: 'Get related data in one query',
        userMessage: 'Show me routes with their roles',
        correctApproach: 'Use nested fields: relation.field syntax',
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
        scenario: 'Filter by related data',
        userMessage: 'Find routes that have Admin role',
        correctApproach: 'Use nested filter: {relation: {field: {operator: value}}}',
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

  // ============================================
  // 4. SELF-AWARENESS & GET_HINT (New!)
  // ============================================
  uncertainty_handling: {
    name: 'Uncertainty & Self-Awareness',
    keywords: ['not sure', 'unsure', 'confused', 'how to', 'what is'],
    examples: [
      {
        scenario: 'Uncertain about nested relations',
        userMessage: 'Show me routes with their roles',
        correctApproach: 'NOT 100% sure about nested syntax → Call get_hint first',
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
        scenario: 'Error occurred - not sure why',
        userMessage: 'Get post with author name (returns error: unknown field)',
        correctApproach: 'Got error → Call get_hint to understand',
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

  // ============================================
  // 5. PERMISSION CHECKS (Critical!)
  // ============================================
  permission_checks: {
    name: 'Permission Checks',
    keywords: ['create', 'add', 'insert', 'update', 'modify', 'delete', 'remove'],
    examples: [
      {
        scenario: 'Check permission before create',
        userMessage: 'Create a new post with title "Hello"',
        correctApproach: 'Step 1: Check permission. Step 2: If allowed, get schema. Step 3: Create',
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
              data: { title: 'Hello', content: 'World' }
            },
            reasoning: 'Only proceed if permission check returned allowed=true'
          }
        ]
      },
      {
        scenario: 'Check permission before delete',
        userMessage: 'Delete route with ID 5',
        correctApproach: 'Check permission first, then delete if allowed',
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

  // ============================================
  // 6. TABLE OPERATIONS
  // ============================================
  table_operations: {
    name: 'Create/Modify Tables',
    keywords: ['create table', 'add table', 'modify table', 'add column', 'new table'],
    examples: [
      {
        scenario: 'Create new table',
        userMessage: 'Create a products table with name and price',
        correctApproach: 'Check existing tables, get reference schema, create with exact structure',
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
                  { name: 'id', type: 'int', isPrimary: true },
                  { name: 'name', type: 'varchar', isNullable: false },
                  { name: 'price', type: 'decimal', isNullable: false }
                ]
              }
            },
            reasoning: 'MUST include id column with isPrimary=true'
          }
        ]
      }
    ]
  }
};

/**
 * Detect user intent based on keywords in message
 */
export function detectIntent(userMessage: string): string[] {
  const message = userMessage.toLowerCase();
  const detectedCategories: string[] = [];

  // Check each category's keywords
  for (const [categoryKey, category] of Object.entries(EXAMPLE_CATEGORIES)) {
    const hasKeyword = category.keywords.some(keyword =>
      message.includes(keyword.toLowerCase())
    );

    if (hasKeyword) {
      detectedCategories.push(categoryKey);
    }
  }

  // Default to find_queries if no specific intent detected
  if (detectedCategories.length === 0) {
    detectedCategories.push('find_queries');
  }

  return detectedCategories;
}

/**
 * Get relevant examples for detected intent
 * Returns max 5 examples (2-3 from each category)
 */
export function getRelevantExamples(userMessage: string): ToolExample[] {
  const intents = detectIntent(userMessage);
  const examples: ToolExample[] = [];

  // Get 2 examples from each detected category (max 5 total)
  for (const intent of intents) {
    const category = EXAMPLE_CATEGORIES[intent];
    if (category) {
      // Take first 2 examples from category
      examples.push(...category.examples.slice(0, 2));
    }

    // Stop at 5 examples total
    if (examples.length >= 5) {
      break;
    }
  }

  return examples.slice(0, 5);
}

/**
 * Format examples as markdown for system prompt injection
 */
export function formatExamplesForPrompt(examples: ToolExample[]): string {
  if (examples.length === 0) {
    return '';
  }

  const formatted = examples.map((example, index) => {
    const toolCallsStr = example.toolCalls.map(tc =>
      `   ${tc.tool}(${JSON.stringify(tc.args, null, 6).replace(/\n/g, '\n   ')})`
    ).join('\n\n');

    return `**Example ${index + 1}: ${example.scenario}**
User: "${example.userMessage}"
Approach: ${example.correctApproach}
Tools:
${toolCallsStr}`;
  }).join('\n\n---\n\n');

  return `
## 📚 Few-Shot Examples (Similar to Your Task)

${formatted}

**Key Lessons from Examples:**
1. **If confidence <80%: Call get_hint FIRST!** (Better safe than wrong)
2. Always get_fields before find queries (fast, lightweight)
3. For count queries: Use meta='totalCount' with limit=1 (NOT limit=0!)
4. Use batch_* operations for 5+ records (never loop!)
5. Check permission before any create/update/delete
6. Use nested fields for related data (one query, not multiple)
7. Use exact column names from get_table_details for create/update

---
`;
}
