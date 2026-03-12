export const TOOL_SHORT_DESCRIPTIONS: Record<string, string> = {
  get_table_details: 'Load schema (columns, relations) for tables. Call before create/update/find.',
  create_tables: 'Create tables. Auto-creates route /{table_name}.',
  update_tables: 'Update table schema (columns, relations).',
  delete_tables: 'Delete tables. For data records use delete_records instead.',
  get_task: 'Get current multi-step task status.',
  update_task: 'Update task status (in_progress, completed, failed).',
  find_records: 'Find or count records in any table. Requires table name.',
  create_records: 'Create records in table.',
  update_records: 'Update records by id.',
  delete_records: 'Delete records by ids. Use for data (hooks, handlers, routes, etc).',
  run_handler_test: 'Test handler/hook code before saving. Required before create/update handler.',
};

export const EVALUATE_TOOL_SELECTION_BASE_PROMPT = `You select which tools the agent needs for the user's request. Output only tool names, no explanations.

**RULES:**
- Use conversation history for context. Short messages ("ok", "do it") → infer from previous message.
- Select MINIMUM tools needed. Include supporting tools (e.g. find_records if delete needs to find ids first).
- When user needs to delete/remove something (hook, handler, route, record) → include delete_records, find_records.
- When user creates/updates handler, hook, route → include run_handler_test, find_records, get_table_details, create_records or update_records.
- When user creates/updates tables → include create_tables or update_tables, find_records, get_table_details.
- When user finds/counts records, lists data → include find_records, get_table_details.
- When user asks about routes, API, endpoints → include find_records, get_table_details.
- When user creates/updates menu (navigation, sidebar) → include find_records, get_table_details, create_records or update_records.
- When user creates/updates extension (custom page, widget, Vue SFC) → include find_records, get_table_details, create_records or update_records.
- Return empty array for greetings, thanks, casual chat, or when no DB operations needed.

**Available tools (name → short description):**
${Object.entries(TOOL_SHORT_DESCRIPTIONS)
  .map(([name, desc]) => `- ${name}: ${desc}`)
  .join('\n')}

**Output:** {"tools": ["tool_name", ...]} - JSON only. Empty array if no tools needed.

Examples:
{"user": "Hello", "output": {"tools": []}}
{"user": "xóa cái hook đó đi", "output": {"tools": ["find_records", "delete_records", "get_table_details"]}}
{"user": "Create products table", "output": {"tools": ["create_tables", "find_records", "get_table_details"]}}
{"user": "Delete route /register", "output": {"tools": ["find_records", "delete_records", "get_table_details"]}}
{"user": "Add handler for POST /users", "output": {"tools": ["find_records", "get_table_details", "run_handler_test", "update_records"]}}
{"user": "Find all orders", "output": {"tools": ["find_records", "get_table_details"]}}
{"user": "List tables", "output": {"tools": ["find_records"]}}
{"user": "Create menu Reports", "output": {"tools": ["find_records", "get_table_details", "create_records"]}}
{"user": "Tạo extension dashboard", "output": {"tools": ["find_records", "get_table_details", "create_records"]}}
{"user": "Tạo menu và extension cho trang Analytics", "output": {"tools": ["find_records", "get_table_details", "create_records"]}}`;
