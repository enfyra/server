/**
 * Helper functions for OpenAI Prompt Caching Optimization
 *
 * OpenAI Prompt Caching benefits:
 * - System prompts: automatically cached when stable (50% cost reduction on cached tokens)
 * - OpenAI automatically caches identical prompt prefixes
 * - Cache is model-specific and persists for the request session
 *
 * Strategy:
 * - Ensure system prompt is stable and at the beginning of messages
 * - Keep system prompt consistent across requests for same config
 * - OpenAI will automatically apply caching for identical prefixes
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

