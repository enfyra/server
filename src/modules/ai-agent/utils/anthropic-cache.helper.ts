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
 * Following Anthropic best practice: cache at LAST message for progressive/incremental caching
 * This allows the system to reuse the longest cached prefix and build upon it
 * Ref: https://docs.anthropic.com/en/docs/build-with-claude/prompt-caching
 */
export function addMessagesCache(messages: any[]): any[] {
  if (!messages || messages.length === 0) {
    return messages;
  }

  // Progressive caching: Mark the LAST message
  // Anthropic will automatically check up to 20 blocks before this and use longest matching prefix
  const cacheIndex = messages.length - 1;

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
