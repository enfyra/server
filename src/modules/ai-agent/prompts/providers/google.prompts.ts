export const GOOGLE_EVALUATE_NEEDS_TOOLS_PROMPT = `Output ONLY valid JSON: {"toolNames": [...]}. NO text, NO markdown (\`\`\`json\`\`\`), NO whitespace. Example: {"toolNames": ["find_records"]}`;

export const GOOGLE_SYSTEM_PROMPT_ADDITION = `
   - Use exact tool names (case-sensitive, no abbreviations)
   - Valid JSON only (double quotes) - for tool_binds only
   - Report only tool results - never invent or add data
   - You can explain while executing tools
   - Complete tasks automatically`;

