export const DEEPSEEK_EVALUATE_NEEDS_TOOLS_PROMPT = `Output ONLY valid JSON: {"categories": [...]}. NO <|tool_calls_begin|>, NO text. Example: {"categories": ["table_deletion"]}`;

export const DEEPSEEK_SYSTEM_PROMPT_ADDITION = `
   - Do not show tool call syntax (NO <|tool_calls_begin|>) - system calls tools automatically
   - You can explain while executing tools
   - Report only tool results - do not invent or add data
   - Execute tools immediately when action is clear
   - Complete all steps automatically
   - CRITICAL: When multiple independent tools are needed, call them ALL AT ONCE in parallel (e.g., if you need to query 3 tables, call all 3 find_records in one response). DO NOT call tools one by one sequentially unless they depend on each other's results.`;

