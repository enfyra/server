export const DEEPSEEK_EVALUATE_NEEDS_TOOLS_PROMPT = `Only tool_binds available. Output ONLY: {"toolNames": [...]}. NO <|tool_calls_begin|>, NO text. Delete tables: {"toolNames": ["find_records", "delete_table"]}`;

export const DEEPSEEK_SYSTEM_PROMPT_ADDITION = `
   - Do not show tool call syntax (NO <|tool_calls_begin|>) - system calls tools automatically
   - You can explain while executing tools
   - Report only tool results - do not invent or add data
   - Execute tools immediately when action is clear
   - Complete all steps automatically`;

