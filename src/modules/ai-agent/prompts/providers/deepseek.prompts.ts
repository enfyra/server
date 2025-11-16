export const DEEPSEEK_EVALUATE_NEEDS_TOOLS_PROMPT = `Output ONLY valid JSON: {"categories": [...]}. NO <|tool_calls_begin|>, NO text. Example: {"categories": ["table_deletion"]}`;

export const DEEPSEEK_SYSTEM_PROMPT_ADDITION = `
   - Do not show tool call syntax (NO <|tool_calls_begin|>) - system calls tools automatically
   - You can explain while executing tools
   - Report only tool results - do not invent or add data
   - Execute tools immediately when action is clear
   - Complete all steps automatically`;

