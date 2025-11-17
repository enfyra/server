export const OPENAI_EVALUATE_NEEDS_TOOLS_PROMPT = ``;

export const OPENAI_SYSTEM_PROMPT_ADDITION = `
   - Always output valid JSON format - NO text before/after JSON
   - Use exact tool names (case-sensitive)
   - If tool returns empty array, say "No records found"
   - Complete multi-step tasks automatically`;

