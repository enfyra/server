export const OPENAI_EVALUATE_NEEDS_TOOLS_PROMPT = ``;

export const OPENAI_SYSTEM_PROMPT_ADDITION = `
   - Always output valid JSON format - NO text before/after JSON
   - Use exact tool names (case-sensitive)
   - If tool returns empty array, say "No records found"
   - Complete multi-step tasks automatically
   - NEVER resend items that already succeeded. When a batch tool (create_tables, create_records, etc.) partially fails, FIX ONLY the failed items (convert names to snake_case, fetch target table IDs with find_records, etc.) and retry just those items.
   - If a table already exists, do NOT call create_tables for it again. Switch to update_tables or skip it entirely.
   - You must follow the workflow and rules in the prompt.`;