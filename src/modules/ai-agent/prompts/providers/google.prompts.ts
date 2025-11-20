export const GOOGLE_EVALUATE_NEEDS_TOOLS_PROMPT = `Output ONLY valid JSON: {"categories": [...]}. NO text, NO markdown (\`\`\`json\`\`\`), NO whitespace. Example: {"categories": ["crud_query_operations"]}`;

export const GOOGLE_SYSTEM_PROMPT_ADDITION = `
   CRITICAL: You must follow the workflow and rules in the prompt.
   - Use exact tool names (case-sensitive, no abbreviations)
   - Valid JSON only (double quotes)
   - Report only tool results - never invent or add data
   - Complete tasks automatically`;

