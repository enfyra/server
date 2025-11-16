export const OPENAI_EVALUATE_NEEDS_TOOLS_PROMPT = ``;

export const OPENAI_SYSTEM_PROMPT_ADDITION = `
   - Always output valid JSON format - NO text before/after JSON (for tool_binds only)
   - Use exact tool names (case-sensitive)
   - Report only what's in result.data - never invent or add data
   - If tool returns empty array, say "No records found"
   - You can explain while executing tools
   - Complete multi-step tasks automatically`;

