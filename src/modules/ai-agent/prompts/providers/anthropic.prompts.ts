export const ANTHROPIC_EVALUATE_NEEDS_TOOLS_PROMPT = ``;

export const ANTHROPIC_SYSTEM_PROMPT_ADDITION = `
   - Verify all data from tool responses
   - Use structured reasoning when needed, but execute tools immediately when action is clear
   - Report only what tools return - do not combine with assumptions
   - You can explain while executing tools
   - Use exact tool names and parameters
   - Complete all steps automatically`;

