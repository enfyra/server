import { Logger } from '@nestjs/common';
import OpenAI from 'openai';
import { StreamEvent } from '../interfaces/stream-event.interface';

const logger = new Logger('OpenAIStreamClient');

export async function streamOpenAIToClient(
  stream: AsyncIterable<OpenAI.Chat.Completions.ChatCompletionChunk>,
  timeout: number,
  onEvent: (event: StreamEvent) => void,
): Promise<{
  stop_reason: string;
  toolCalls: any[];
  textContent: string;
  inputTokens: number;
  outputTokens: number;
}> {
  return new Promise((resolve, reject) => {
    const timeoutId = setTimeout(() => {
      reject(new Error(`LLM request timeout after ${timeout}ms`));
    }, timeout);

    const collectedToolCalls: Map<number, any> = new Map();
    let collectedText = '';
    let finalStopReason = 'stop';
    let inputTokens = 0;
    let outputTokens = 0;

    const streamStartTime = Date.now();
    let firstChunkTime: number | null = null;
    let lastChunkTime = streamStartTime;
    let chunkCount = 0;
    let totalChunkInterval = 0;

    (async () => {
      try {
        for await (const chunk of stream) {
          chunkCount++;
          const currentTime = Date.now();

          if (firstChunkTime === null) {
            firstChunkTime = currentTime;
            logger.log(`[OpenAI Stream] First chunk received after ${currentTime - streamStartTime}ms`);
          }

          const intervalSinceLastChunk = currentTime - lastChunkTime;
          totalChunkInterval += intervalSinceLastChunk;

          if (intervalSinceLastChunk > 1000) {
            logger.warn(`[OpenAI Stream] Long interval between chunks: ${intervalSinceLastChunk}ms (chunk #${chunkCount})`);
          }

          lastChunkTime = currentTime;

          const delta = chunk.choices[0]?.delta;

          if (!delta) continue;

          // Stream text content
          if (delta.content) {
            collectedText += delta.content;
            onEvent({
              type: 'text',
              data: { delta: delta.content, text: collectedText },
            });
          }

          // Stream tool calls
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
            const cachedTokens = chunk.usage.prompt_tokens_details?.cached_tokens || 0;

            logger.log(`[OpenAI Stream] Usage - Input: ${inputTokens}, Output: ${outputTokens}`);
            if (cachedTokens > 0) {
              const cacheHitRate = ((cachedTokens / inputTokens) * 100).toFixed(1);
              logger.log(`[OpenAI Stream] Cache - ${cachedTokens} tokens cached (${cacheHitRate}% hit rate, 50% cost saving)`);
            } else {
              logger.warn(`[OpenAI Stream] ⚠️ NO CACHE HIT - cached_tokens: 0 (prompt may be < 1024 tokens or changed)`);
            }

            onEvent({
              type: 'tokens',
              data: {
                inputTokens,
                outputTokens,
                cachedTokens,
              },
            });
          }
        }

        clearTimeout(timeoutId);

        const totalStreamTime = Date.now() - streamStartTime;
        const avgChunkInterval = chunkCount > 0 ? totalChunkInterval / chunkCount : 0;

        logger.log(`[OpenAI Stream] Stream completed in ${totalStreamTime}ms`);
        logger.log(`[OpenAI Stream] Total chunks: ${chunkCount}, Avg interval: ${avgChunkInterval.toFixed(2)}ms`);
        logger.log(`[OpenAI Stream] Time to first chunk: ${firstChunkTime ? (firstChunkTime - streamStartTime) : 0}ms`);
        logger.log(`[OpenAI Stream] Tool calls: ${collectedToolCalls.size}, Text length: ${collectedText.length} chars`);

        // Warn if no usage data received
        if (inputTokens === 0 && outputTokens === 0) {
          logger.warn(`[OpenAI Stream] No usage data received from OpenAI API.`);
        }

        const toolCallsArray: any[] = Array.from(collectedToolCalls.values());

        onEvent({
          type: 'done',
          data: {
            stop_reason: finalStopReason,
            toolCalls: toolCallsArray,
          },
        });

        resolve({
          stop_reason: finalStopReason,
          toolCalls: toolCallsArray,
          textContent: collectedText,
          inputTokens,
          outputTokens,
        });
      } catch (error: any) {
        clearTimeout(timeoutId);
        logger.error('Error processing OpenAI stream:', error);
        onEvent({
          type: 'error',
          data: { error: error.message || String(error) },
        });
        reject(error);
      }
    })();
  });
}
