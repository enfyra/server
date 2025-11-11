import { Logger } from '@nestjs/common';
import Anthropic from '@anthropic-ai/sdk';

export interface StreamEvent {
  type: 'text' | 'tool_call' | 'tool_result' | 'done' | 'error' | 'tokens';
  data?: any;
}

const logger = new Logger('AnthropicStreamClient');

export async function streamAnthropicToClient(
  stream: any,
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
      stream.abort();
      reject(new Error(`LLM request timeout after ${timeout}ms`));
    }, timeout);

    const collectedToolCalls: Map<string, any> = new Map();
    const indexToToolId: Map<number, string> = new Map();
    const toolInputJsonStrings: Map<string, string> = new Map();
    let collectedText = '';
    let finalStopReason = 'end_turn';
    let finalMessage: any = null;
    let inputTokens = 0;
    let outputTokens = 0;
    let lastTextSnapshot = '';

    // Removed stream.on('text') to avoid duplicate events
    // Text events are handled in the async for loop below

    stream.on('contentBlock', (block: any) => {
      if (block.type === 'tool_use') {
        if (!collectedToolCalls.has(block.id)) {
          collectedToolCalls.set(block.id, {
            id: block.id,
            name: block.name,
            input: block.input || {},
          });
          onEvent({
            type: 'tool_call',
            data: {
              id: block.id,
              name: block.name,
              input: block.input || {},
            },
          });
        }
      }
    });

    stream.on('finalMessage', (message: any) => {
      finalMessage = message;
      
      if (message.usage) {
        inputTokens = message.usage.input_tokens || 0;
        outputTokens = message.usage.output_tokens || 0;
        onEvent({
          type: 'tokens',
          data: { inputTokens, outputTokens },
        });
      }
      
      if (message.content) {
        for (const block of message.content) {
          if (block.type === 'text' && block.text) {
            const newText = block.text;
            if (newText !== collectedText) {
              const delta = newText.slice(collectedText.length);
              if (delta) {
                onEvent({
                  type: 'text',
                  data: { delta, text: newText },
                });
              }
              collectedText = newText;
              lastTextSnapshot = newText;
            }
          } else if (block.type === 'tool_use') {
            if (!collectedToolCalls.has(block.id)) {
              collectedToolCalls.set(block.id, {
                id: block.id,
                name: block.name,
                input: block.input || {},
              });
              onEvent({
                type: 'tool_call',
                data: {
                  id: block.id,
                  name: block.name,
                  input: block.input || {},
                },
              });
            }
          }
        }
      }
      if (message.stop_reason) {
        finalStopReason = message.stop_reason;
      }
    });

    stream.on('error', (error: any) => {
      clearTimeout(timeoutId);
      onEvent({
        type: 'error',
        data: { error: error.message || String(error) },
      });
      logger.error('Stream error:', error);
      reject(error);
    });

    stream.on('end', () => {
      clearTimeout(timeoutId);
      
      if (finalMessage && !collectedText && finalMessage.content) {
        for (const block of finalMessage.content) {
          if (block.type === 'text' && block.text) {
            collectedText = block.text;
            onEvent({
              type: 'text',
              data: { delta: block.text, text: block.text },
            });
          }
        }
      }

      const toolCallsArray: any[] = [];
      for (const toolCall of collectedToolCalls.values()) {
        toolCallsArray.push({
          id: toolCall.id,
          type: 'function',
          function: {
            name: toolCall.name,
            arguments: JSON.stringify(toolCall.input),
          },
        });
      }

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
    });

    (async () => {
      try {
        for await (const event of stream) {
          if (event.type === 'message_start') {
            if (event.message?.content && Array.isArray(event.message.content)) {
              for (const block of event.message.content) {
                if (block.type === 'text' && block.text) {
                  collectedText = block.text;
                  onEvent({
                    type: 'text',
                    data: { delta: block.text, text: block.text },
                  });
                } else if (block.type === 'tool_use') {
                  if (!collectedToolCalls.has(block.id)) {
                    collectedToolCalls.set(block.id, {
                      id: block.id,
                      name: block.name,
                      input: block.input || {},
                    });
                    onEvent({
                      type: 'tool_call',
                      data: {
                        id: block.id,
                        name: block.name,
                        input: block.input || {},
                      },
                    });
                  }
                }
              }
            }
          } else if (event.type === 'content_block_start') {
            const block = event.content_block;
            if (block.type === 'text' && block.text) {
              collectedText = block.text;
              onEvent({
                type: 'text',
                data: { delta: block.text, text: block.text },
              });
            } else if (block.type === 'tool_use') {
              if (!collectedToolCalls.has(block.id)) {
                collectedToolCalls.set(block.id, {
                  id: block.id,
                  name: block.name,
                  input: block.input || {},
                });
                indexToToolId.set(event.index, block.id);
                toolInputJsonStrings.set(block.id, '');
                onEvent({
                  type: 'tool_call',
                  data: {
                    id: block.id,
                    name: block.name,
                    input: block.input || {},
                  },
                });
              }
            }
          } else if (event.type === 'content_block_delta') {
            const delta = event.delta;
            if (delta.type === 'text_delta' && delta.text) {
              collectedText += delta.text;
              onEvent({
                type: 'text',
                data: { delta: delta.text, text: collectedText },
              });
            } else if (delta.type === 'input_json_delta' && delta.partial_json) {
              const toolUseId = indexToToolId.get(event.index);
              if (toolUseId) {
                const currentJson = toolInputJsonStrings.get(toolUseId) || '';
                toolInputJsonStrings.set(toolUseId, currentJson + delta.partial_json);
              }
            }
          } else if (event.type === 'content_block_stop') {
            const toolUseId = indexToToolId.get(event.index);
            if (toolUseId) {
              const toolUse = collectedToolCalls.get(toolUseId);
              const jsonString = toolInputJsonStrings.get(toolUseId);
              if (toolUse && jsonString) {
                try {
                  const parsed = JSON.parse(jsonString);
                  toolUse.input = parsed;
                } catch (e) {
                  logger.error(`Failed to parse tool input JSON for ${toolUseId}:`, e);
                }
              }
            }
          } else if (event.type === 'message_delta') {
            if (event.delta?.stop_reason) {
              finalStopReason = event.delta.stop_reason;
            }
          }
        }
      } catch (error: any) {
        clearTimeout(timeoutId);
        try {
          stream.abort();
        } catch (abortError) {
        }
        logger.error(`Error processing Anthropic stream:`, error);
        onEvent({
          type: 'error',
          data: { error: error.message || String(error) },
        });
        reject(error);
      }
    })();
  });
}

