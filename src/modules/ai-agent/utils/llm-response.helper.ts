import { Logger } from '@nestjs/common';

const logger = new Logger('LLMResponseHelper');

export function reduceContentToString(content: any): string {
  if (!content) {
    return '';
  }
  if (typeof content === 'string') {
    return content;
  }
  if (Array.isArray(content)) {
    if (content.length === 0) {
      return '';
    }
    return content
      .map((item) => {
        if (!item) {
          return '';
        }
        if (typeof item === 'string') {
          return item;
        }
        if (typeof item === 'object') {
          if (item.text) {
            return item.text;
          }
          if (item.value) {
            return item.value;
          }
          if (item.content) {
            return reduceContentToString(item.content);
          }
          if (item.type === 'text' && item.text) {
            return item.text;
          }
          if (item.type === 'text' && item.content) {
            return reduceContentToString(item.content);
          }
          if (item.message && typeof item.message === 'string') {
            return item.message;
          }
          if (item.parts && Array.isArray(item.parts)) {
            return item.parts.map((part: any) => {
              if (typeof part === 'string') return part;
              if (part?.text) return part.text;
              return '';
            }).join('');
          }
        }
        return '';
      })
      .join('');
  }
  if (typeof content === 'object') {
    if (content.text) {
      return content.text;
    }
    if (content.value) {
      return content.value;
    }
    if (content.content) {
      return reduceContentToString(content.content);
    }
    if (content.type === 'text' && content.text) {
      return content.text;
    }
    if (content.parts && Array.isArray(content.parts)) {
      return content.parts.map((part: any) => {
        if (typeof part === 'string') return part;
        if (part?.text) return part.text;
        return '';
      }).join('');
    }
  }
  return '';
}

export async function streamChunkedContent(
  content: string,
  abortSignal?: AbortSignal,
  onChunk?: (chunk: string) => void,
): Promise<void> {
  if (!content || !onChunk) {
    return;
  }

  const CHUNK_SIZE = 10;
  const DELAY_MS = 20;

  for (let i = 0; i < content.length; i += CHUNK_SIZE) {
    if (abortSignal?.aborted) {
      throw new Error('Request aborted by client');
    }

    const chunk = content.slice(i, i + CHUNK_SIZE);
    onChunk(chunk);
    if (i + CHUNK_SIZE < content.length) {
      await new Promise((resolve) => setTimeout(resolve, DELAY_MS));
    }
  }
}

export function getToolCallsFromResponse(response: any): any[] {
  if (!response) {
    return [];
  }

  if (response.tool_calls && Array.isArray(response.tool_calls) && response.tool_calls.length > 0) {
    return response.tool_calls;
  }

  if (response.additional_kwargs?.tool_calls && Array.isArray(response.additional_kwargs.tool_calls) && response.additional_kwargs.tool_calls.length > 0) {
    return response.additional_kwargs.tool_calls;
  }

  if (response.response_metadata?.tool_calls && Array.isArray(response.response_metadata.tool_calls) && response.response_metadata.tool_calls.length > 0) {
    return response.response_metadata.tool_calls;
  }

  if (response.lc_kwargs?.tool_calls && Array.isArray(response.lc_kwargs.tool_calls) && response.lc_kwargs.tool_calls.length > 0) {
    return response.lc_kwargs.tool_calls;
  }

  if (response.kwargs?.tool_calls && Array.isArray(response.kwargs.tool_calls) && response.kwargs.tool_calls.length > 0) {
    return response.kwargs.tool_calls;
  }

  if (response.content && typeof response.content === 'string') {
    if (response.content.includes('redacted_tool_calls_begin') || response.content.includes('<|redacted_tool_call')) {
      try {
        const toolCallRegex = /<\|redacted_tool_call_begin\|>([^<]+)<\|redacted_tool_sep\|>([^<]+)<\|redacted_tool_call_end\|>/g;
        const matches = [...response.content.matchAll(toolCallRegex)];
        if (matches.length > 0) {
          const parsedToolCalls = matches.map((match, index) => {
            const toolName = match[1].trim();
            let toolArgs = {};
            try {
              toolArgs = JSON.parse(match[2].trim());
            } catch {
            }
            return {
              id: `call_${Date.now()}_${index}`,
              function: {
                name: toolName,
                arguments: JSON.stringify(toolArgs),
              },
            };
          });
          return parsedToolCalls;
        }
      } catch (e) {
        logger.error(`[LLM Stream] Failed to parse tool calls from text: ${e}`);
      }
      return [];
    }
    try {
      const parsed = JSON.parse(response.content);
      if (parsed.tool_calls && Array.isArray(parsed.tool_calls) && parsed.tool_calls.length > 0) {
        return parsed.tool_calls;
      }
    } catch {
    }
  }

  if (response.tool_call_chunks && Array.isArray(response.tool_call_chunks) && response.tool_call_chunks.length > 0) {
    return response.tool_call_chunks;
  }

  return [];
}

