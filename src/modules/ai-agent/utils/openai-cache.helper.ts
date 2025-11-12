/**
 * Helper functions for OpenAI Prompt Caching Optimization
 *
 * OpenAI Automatic Prompt Caching (GPT-4o, GPT-4o-mini, o1):
 * - Automatic caching for prompts ≥ 1024 tokens
 * - 50% discount on cached input tokens
 * - 80% latency reduction for cache hits
 * - Cache duration: 5-10 minutes (up to 1 hour)
 * - Caching increments: First 1024 tokens, then 128-token chunks
 * - NO cache write fees (unlike Anthropic)
 *
 * Best Practices for Cache Hits:
 * 1. Static content FIRST: System message → Tools → Early messages
 * 2. Dynamic content LAST: User messages, fresh context
 * 3. Tool ordering MUST be identical across requests
 * 4. First 1024 tokens MUST match exactly for cache hit
 *
 * Reference: https://platform.openai.com/docs/guides/prompt-caching
 */

export interface LLMMessage {
  role: 'system' | 'user' | 'assistant' | 'tool';
  content: string | null;
  tool_calls?: any[];
  tool_call_id?: string;
  name?: string;
}

/**
 * Optimize messages for OpenAI prompt caching
 * - System prompts are automatically cached by OpenAI when identical
 * - Ensure system prompt is at the beginning and stable
 * - Return messages in optimal order for caching
 */
export function optimizeOpenAIMessages(messages: LLMMessage[]): LLMMessage[] {
  if (!messages || messages.length === 0) {
    return messages;
  }

  // Find system message
  const systemMessages = messages.filter((m) => m.role === 'system');
  const nonSystemMessages = messages.filter((m) => m.role !== 'system');

  // If no system message, return as-is
  if (systemMessages.length === 0) {
    return messages;
  }

  // Combine system messages into one (OpenAI caches better with single system message)
  const combinedSystemContent = systemMessages
    .map((m) => m.content)
    .filter((c) => c !== null && c !== undefined)
    .join('\n\n');

  // Return optimized order: system first, then other messages
  // OpenAI will automatically cache the system prompt if it's identical across requests
  const optimized: LLMMessage[] = [];

  if (combinedSystemContent) {
    optimized.push({
      role: 'system',
      content: combinedSystemContent,
    });
  }

  optimized.push(...nonSystemMessages);

  return optimized;
}

/**
 * Extract system prompt from messages for caching optimization
 * Useful for logging and monitoring cache effectiveness
 */
export function extractSystemPrompt(messages: LLMMessage[]): string | null {
  const systemMessages = messages.filter((m) => m.role === 'system');
  if (systemMessages.length === 0) {
    return null;
  }

  return systemMessages
    .map((m) => m.content)
    .filter((c) => c !== null && c !== undefined)
    .join('\n\n');
}

