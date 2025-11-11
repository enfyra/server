import { Logger } from '@nestjs/common';
import OpenAI from 'openai';

export interface StreamData {
  stop_reason: string;
  content: any[];
  toolCalls: any[];
  textContent: string;
  inputTokens: number;
  outputTokens: number;
}

const logger = new Logger('OpenAIStream');

export async function handleOpenAIStream(
  stream: AsyncIterable<OpenAI.Chat.Completions.ChatCompletionChunk>,
  timeout: number,
): Promise<StreamData> {
  return new Promise<StreamData>((resolve, reject) => {
    const timeoutId = setTimeout(() => {
      reject(new Error(`LLM request timeout after ${timeout}ms`));
    }, timeout);

    const collectedToolCalls: Map<number, any> = new Map();
    let collectedText = '';
    let finalStopReason = 'stop';
    let inputTokens = 0;
    let outputTokens = 0;

    (async () => {
      try {
        for await (const chunk of stream) {
          const delta = chunk.choices[0]?.delta;

          if (!delta) continue;

          // Collect text content
          if (delta.content) {
            collectedText += delta.content;
          }

          // Collect tool calls
          if (delta.tool_calls) {
            for (const toolCallDelta of delta.tool_calls) {
              const index = toolCallDelta.index;

              if (!collectedToolCalls.has(index)) {
                collectedToolCalls.set(index, {
                  id: toolCallDelta.id || '',
                  type: 'function',
                  function: {
                    name: toolCallDelta.function?.name || '',
                    arguments: toolCallDelta.function?.arguments || '',
                  },
                });
              } else {
                const existing = collectedToolCalls.get(index);
                if (toolCallDelta.id) {
                  existing.id = toolCallDelta.id;
                }
                if (toolCallDelta.function?.name) {
                  existing.function.name = toolCallDelta.function.name;
                }
                if (toolCallDelta.function?.arguments) {
                  existing.function.arguments += toolCallDelta.function.arguments;
                }
              }
            }
          }

          // Collect finish reason
          if (chunk.choices[0]?.finish_reason) {
            finalStopReason = chunk.choices[0].finish_reason;
          }

          // Collect usage (usually in last chunk)
          if (chunk.usage) {
            inputTokens = chunk.usage.prompt_tokens || 0;
            outputTokens = chunk.usage.completion_tokens || 0;
          }
        }

        clearTimeout(timeoutId);

        const content: any[] = [];
        if (collectedText) {
          content.push({ type: 'text', text: collectedText });
        }

        const toolCallsArray: any[] = Array.from(collectedToolCalls.values());

        resolve({
          stop_reason: finalStopReason,
          content,
          toolCalls: toolCallsArray,
          textContent: collectedText,
          inputTokens,
          outputTokens,
        });
      } catch (error: any) {
        clearTimeout(timeoutId);
        logger.error('Error processing OpenAI stream:', error);
        reject(error);
      }
    })();
  });
}
