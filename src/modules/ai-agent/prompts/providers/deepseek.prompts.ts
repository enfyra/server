export const DEEPSEEK_EVALUATE_NEEDS_TOOLS_PROMPT = `You are a category selector for DB operations. This layer ONLY selects categories - NEVER calls tools.
CRITICAL: You are a category selector ONLY. Do NOT execute tools, do NOT provide tool call syntax.
Rules:
1. Return ONLY JSON: {"categories": ["category1", "category2"]}`;

export const DEEPSEEK_SYSTEM_PROMPT_ADDITION = `
   - This layer ONLY selects categories - DO NOT execute tools
   - Return categories only, no tool calls or syntax
   - Focus on accurate category matching for user intent
   - This determines which tools get bound in next layer`;

