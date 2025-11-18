import { Logger } from '@nestjs/common';
import { getToolCallsFromResponse } from './llm-response.helper';

const logger = new Logger('StreamToolAggregatorHelper');

export interface AggregateToolCallChunk {
  id?: string;
  index?: number;
  function?: {
    name?: string;
    arguments?: string | any;
  };
  name?: string;
  args?: string | any;
}

export function aggregateToolCallsFromChunks(chunks: any[]): Map<number, any> {
  const aggregatedToolCalls: Map<number, any> = new Map();

  for (const chunk of chunks) {
    const chunkToolCalls = getToolCallsFromResponse(chunk);
    if (chunkToolCalls.length > 0) {
      for (const tc of chunkToolCalls) {
        const chunkToolId = tc.id;
        const chunkToolName = tc.function?.name || tc.name;

        let index: number;
        if (!chunkToolId && !chunkToolName && aggregatedToolCalls.size > 0) {
          index = aggregatedToolCalls.size - 1;
        } else {
          index = tc.index !== undefined ? tc.index : aggregatedToolCalls.size;
        }

        const existing = aggregatedToolCalls.get(index) || {};

        let chunkArgs = tc.args || tc.function?.arguments || '';
        if (typeof chunkArgs !== 'string') {
          chunkArgs = typeof chunkArgs === 'object' ? JSON.stringify(chunkArgs) : String(chunkArgs);
        }

        let existingArgs = existing.args || existing.function?.arguments || '';
        if (typeof existingArgs !== 'string') {
          existingArgs = typeof existingArgs === 'object' ? JSON.stringify(existingArgs) : String(existingArgs);
        }

        let mergedArgs: string;
        if (!existingArgs || existingArgs === '{}' || existingArgs.trim() === '') {
          mergedArgs = chunkArgs && chunkArgs.trim() && chunkArgs !== '{}' ? chunkArgs : '{}';
        } else if (!chunkArgs || chunkArgs === '{}' || chunkArgs.trim() === '') {
          mergedArgs = existingArgs;
        } else {
          try {
            const existingParsed = existingArgs !== '{}' ? JSON.parse(existingArgs) : {};
            const chunkParsed = chunkArgs !== '{}' ? JSON.parse(chunkArgs) : {};
            mergedArgs = JSON.stringify({ ...existingParsed, ...chunkParsed });
          } catch {
            mergedArgs = existingArgs + chunkArgs;
          }
        }

        let toolId = tc.id || existing.id;
        const toolName = tc.function?.name || tc.name || existing.function?.name;

        if (!toolId && toolName) {
          toolId = `call_${Date.now()}_${index}_${Math.random().toString(36).substr(2, 9)}`;
        }

        aggregatedToolCalls.set(index, {
          ...existing,
          ...tc,
          id: toolId,
          args: mergedArgs,
          function: {
            ...(existing.function || {}),
            ...(tc.function || {}),
            name: toolName,
            arguments: mergedArgs,
          },
        });
      }
    }
  }

  return aggregatedToolCalls;
}

export function deduplicateToolCalls(aggregatedToolCalls: Map<number, any>): any[] {
  const uniqueToolCalls = new Map<string, any>();
  
  for (const tc of aggregatedToolCalls.values()) {
    const toolId = tc.id;
    if (!toolId) {
      continue;
    }

    const existing = uniqueToolCalls.get(toolId);
    if (existing) {
      const existingArgs = existing.args || existing.function?.arguments || '';
      const newArgs = tc.args || tc.function?.arguments || '';

      if (newArgs && newArgs !== '{}' && newArgs.trim() && (!existingArgs || existingArgs === '{}')) {
        uniqueToolCalls.set(toolId, tc);
      }
    } else {
      uniqueToolCalls.set(toolId, tc);
    }
  }

  return Array.from(uniqueToolCalls.values()).map((tc) => {
    const argsString = tc.args || tc.function?.arguments || '';
    const toolName = tc.function?.name || tc.name || 'unknown';
    const toolId = tc.id;

    if (argsString && typeof argsString === 'string' && argsString.trim() && argsString !== '{}') {
      try {
        const parsed = JSON.parse(argsString);
        if (Object.keys(parsed).length > 0) {
          return {
            ...tc,
            id: toolId,
            args: parsed,
            function: {
              ...tc.function,
              name: toolName,
              arguments: parsed,
            },
          };
        }
      } catch (e) {
      }
    }

    return {
      ...tc,
      id: toolId,
      function: {
        ...tc.function,
        name: toolName,
        arguments: argsString && argsString !== '{}' ? argsString : undefined,
      },
    };
  });
}

export function parseRedactedToolCalls(fullContent: string): any[] {
  try {
    const toolCallRegex = /<\|redacted_tool_call_begin\|>([^<]+)<\|redacted_tool_sep\|>([^<]+)<\|redacted_tool_call_end\|>/g;
    const matches = [...fullContent.matchAll(toolCallRegex)];

    if (matches.length === 0) {
      return [];
    }

    return matches.map((match, index) => {
      const toolName = match[1].trim();
      let toolArgs = {};

      try {
        const argsString = match[2].trim();
        toolArgs = JSON.parse(argsString);
      } catch (parseError: any) {
        logger.error(`[LLM Stream] ❌ Failed to parse tool args for ${toolName}: ${parseError.message}`);
        logger.error(`[LLM Stream] ❌ argsString.length=${match[2]?.trim().length}, first 500 chars: ${match[2]?.substring(0, 500)}`);
        logger.error(`[LLM Stream] ❌ last 100 chars: ${match[2]?.substring(Math.max(0, (match[2]?.length || 0) - 100))}`);
      }

      return {
        id: `call_${Date.now()}_${index}`,
        name: toolName,
        args: toolArgs,
        function: {
          name: toolName,
          arguments: JSON.stringify(toolArgs),
        },
        type: 'tool_call' as const,
      };
    });
  } catch (e: any) {
    logger.error(`[LLM Stream] Failed to parse tool calls from fullContent: ${e.message}`);
    logger.error(`[LLM Stream] Error stack: ${e.stack}`);
    return [];
  }
}

