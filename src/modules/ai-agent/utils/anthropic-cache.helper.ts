/**
 * Helper functions for Anthropic Prompt Caching
 *
 * Prompt Caching benefits:
 * - System prompts: cached automatically (almost never change)
 * - Tools: cached when stable
 * - Conversation prefix: cache older messages
 *
 * Pricing:
 * - Cache write: $3.75/MTok (25% premium)
 * - Cache read: $0.30/MTok (90% cheaper than $3/MTok base)
 * - Cache TTL: 5 minutes
 */

export interface CacheableContent {
  type: string;
  text?: string;
  cache_control?: { type: 'ephemeral' };
  [key: string]: any;
}

/**
 * Add cache_control to system prompt
 * System prompts are almost always the same, so they benefit most from caching
 */
export function addSystemCache(systemPrompt: string | CacheableContent[]): string | CacheableContent[] {
  if (typeof systemPrompt === 'string') {
    // Return as array with cache_control
    return [
      {
        type: 'text',
        text: systemPrompt,
        cache_control: { type: 'ephemeral' },
      },
    ];
  }

  // If already array, add cache_control to last block
  if (Array.isArray(systemPrompt) && systemPrompt.length > 0) {
    const blocks = [...systemPrompt];
    blocks[blocks.length - 1] = {
      ...blocks[blocks.length - 1],
      cache_control: { type: 'ephemeral' },
    };
    return blocks;
  }

  return systemPrompt;
}

/**
 * Add cache_control to tools array
 * Tools usually don't change between requests
 */
export function addToolsCache(tools: any[]): any[] {
  if (!tools || tools.length === 0) {
    return tools;
  }

  const cachedTools = [...tools];
  // Add cache_control to last tool
  cachedTools[cachedTools.length - 1] = {
    ...cachedTools[cachedTools.length - 1],
    cache_control: { type: 'ephemeral' },
  };

  return cachedTools;
}

/**
 * Add cache_control to messages array
 * Cache older messages (first ~80% of conversation)
 * Leave recent messages uncached as they change frequently
 */
export function addMessagesCache(messages: any[]): any[] {
  if (!messages || messages.length <= 2) {
    // Don't cache very short conversations
    return messages;
  }

  // Cache breakpoint at ~80% of messages (older messages)
  const cacheIndex = Math.floor(messages.length * 0.8);

  // Ensure we have at least some messages before cache breakpoint
  if (cacheIndex < 1) {
    return messages;
  }

  const cachedMessages = [...messages];
  const targetMessage = cachedMessages[cacheIndex];

  // Add cache_control to content
  if (typeof targetMessage.content === 'string') {
    cachedMessages[cacheIndex] = {
      ...targetMessage,
      content: [
        {
          type: 'text',
          text: targetMessage.content,
          cache_control: { type: 'ephemeral' },
        },
      ],
    };
  } else if (Array.isArray(targetMessage.content)) {
    const contentBlocks = [...targetMessage.content];
    contentBlocks[contentBlocks.length - 1] = {
      ...contentBlocks[contentBlocks.length - 1],
      cache_control: { type: 'ephemeral' },
    };
    cachedMessages[cacheIndex] = {
      ...targetMessage,
      content: contentBlocks,
    };
  }

  return cachedMessages;
}

/**
 * Apply all caching optimizations
 * Returns modified system, tools, and messages with cache_control added
 */
export function applyPromptCaching(
  systemPrompt: string | CacheableContent[],
  tools: any[],
  messages: any[],
): {
  system: string | CacheableContent[];
  tools: any[];
  messages: any[];
} {
  return {
    system: addSystemCache(systemPrompt),
    tools: addToolsCache(tools),
    messages: addMessagesCache(messages),
  };
}
