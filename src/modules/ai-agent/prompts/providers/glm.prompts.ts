export const GLM_EVALUATE_NEEDS_TOOLS_PROMPT = `Return ONLY valid JSON in the form {"categories": ["..."]}. No explanations, no markdown, no additional text.`;

export const GLM_SYSTEM_PROMPT_ADDITION = `
   - Tool calls must use the exact function names and JSON schemas provided
   - When a response is pure JSON, do not add text before or after it
   - Execute all required tools directly instead of describing what you will do
   - If multiple tools are required, batch them in a single response when possible
   - After receiving tool results, summarize them in natural language for the user`;

