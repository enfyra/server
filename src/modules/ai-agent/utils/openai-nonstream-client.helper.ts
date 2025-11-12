import { Logger } from '@nestjs/common';
import OpenAI from 'openai';
import { StreamEvent } from '../interfaces/stream-event.interface';

const logger = new Logger('OpenAINonStreamClient');

/**
 * Call OpenAI with non-streaming API (faster connection) but simulate streaming to client
 *
 * Why: Stream connection setup takes 7-8 seconds, but non-streaming is much faster.
 * This approach gets the full response quickly, then simulates streaming to client for UX.
 *
 * @param client OpenAI client
 * @param params Chat completion params
 * @param timeout Timeout in ms
 * @param onEvent Event handler for streaming events
 */
export async function chatOpenAINonStreamButStreamToClient(
  client: OpenAI,
  params: {
    model: string;
    messages: any[];
    tools?: any[];
    tool_choice?: any;
    max_tokens?: number;
    temperature?: number;
    parallel_tool_calls?: boolean;
  },
  timeout: number,
  onEvent: (event: StreamEvent) => void,
): Promise<{
  stop_reason: string;
  toolCalls: any[];
  textContent: string;
  inputTokens: number;
  outputTokens: number;
}> {
  const startTime = Date.now();

  try {
    logger.log(`[OpenAI Non-Stream] Calling API (no streaming)...`);

    // Call OpenAI REST API (non-streaming) - should be much faster
    const response = await Promise.race([
      client.chat.completions.create({
        model: params.model,
        messages: params.messages,
        tools: params.tools,
        tool_choice: params.tool_choice,
        max_completion_tokens: params.max_tokens,
        temperature: params.temperature,
        parallel_tool_calls: params.parallel_tool_calls,
        stream: false, // Non-streaming
      }),
      new Promise<never>((_, reject) =>
        setTimeout(() => reject(new Error(`Request timeout after ${timeout}ms`)), timeout)
      ),
    ]);

    const apiCallDuration = Date.now() - startTime;
    logger.log(`[OpenAI Non-Stream] ✓ Got response in ${apiCallDuration}ms`);

    // Extract response data
    const choice = response.choices[0];
    const message = choice.message;
    const textContent = message.content || '';
    const toolCalls = message.tool_calls || [];
    const stopReason = choice.finish_reason || 'stop';

    // Extract token usage
    const inputTokens = response.usage?.prompt_tokens || 0;
    const outputTokens = response.usage?.completion_tokens || 0;
    const cachedTokens = (response.usage as any)?.prompt_tokens_details?.cached_tokens || 0;

    logger.log(`[OpenAI Non-Stream] Usage - Input: ${inputTokens}, Output: ${outputTokens}`);
    if (cachedTokens > 0) {
      const cacheHitRate = ((cachedTokens / inputTokens) * 100).toFixed(1);
      logger.log(`[OpenAI Non-Stream] Cache - ${cachedTokens} tokens cached (${cacheHitRate}% hit rate, 50% cost saving)`);
    } else {
      logger.warn(`[OpenAI Non-Stream] ⚠️ NO CACHE HIT - cached_tokens: 0`);
    }

    // Now simulate streaming to client for UX
    const streamSimulationStart = Date.now();
    logger.log(`[OpenAI Non-Stream] Simulating stream to client...`);

    // Stream text content in chunks
    if (textContent) {
      const chunkSize = 10; // Characters per chunk
      let streamedText = '';

      for (let i = 0; i < textContent.length; i += chunkSize) {
        const chunk = textContent.slice(i, i + chunkSize);
        streamedText += chunk;

        onEvent({
          type: 'text',
          data: { delta: chunk, text: streamedText },
        });

        // Small delay to simulate streaming (1ms per chunk)
        await new Promise(resolve => setTimeout(resolve, 1));
      }
    }

    // Send tool calls (already complete)
    if (toolCalls.length > 0) {
      logger.log(`[OpenAI Non-Stream] ${toolCalls.length} tool calls received`);
      // Tool calls are sent as part of the response, client will handle them
    }

    // Send token usage
    onEvent({
      type: 'tokens',
      data: {
        inputTokens,
        outputTokens,
        cachedTokens,
      },
    });

    // Send done event
    const toolCallsArray = toolCalls.map((tc: any) => ({
      id: tc.id,
      type: 'function',
      function: {
        name: tc.function.name,
        arguments: tc.function.arguments,
      },
    }));

    onEvent({
      type: 'done',
      data: {
        stop_reason: stopReason,
        toolCalls: toolCallsArray,
      },
    });

    const totalDuration = Date.now() - startTime;
    const streamSimulationDuration = Date.now() - streamSimulationStart;
    logger.log(`[OpenAI Non-Stream] Total time: ${totalDuration}ms (API: ${apiCallDuration}ms, Stream simulation: ${streamSimulationDuration}ms)`);
    logger.log(`[OpenAI Non-Stream] Text length: ${textContent.length} chars, Tool calls: ${toolCallsArray.length}`);

    return {
      stop_reason: stopReason,
      toolCalls: toolCallsArray,
      textContent,
      inputTokens,
      outputTokens,
    };
  } catch (error: any) {
    logger.error('[OpenAI Non-Stream] Error:', error);
    onEvent({
      type: 'error',
      data: { error: error.message || String(error) },
    });
    throw error;
  }
}
