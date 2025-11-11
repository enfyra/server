import { Injectable, Logger, BadRequestException, HttpException, HttpStatus } from '@nestjs/common';
import OpenAI from 'openai';
import Anthropic from '@anthropic-ai/sdk';
import { MetadataCacheService } from '../../../infrastructure/cache/services/metadata-cache.service';
import { AiConfigCacheService } from '../../../infrastructure/cache/services/ai-config-cache.service';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { TableHandlerService } from '../../table-management/services/table-handler.service';
import { QueryEngine } from '../../../infrastructure/query-engine/services/query-engine.service';
import { RouteCacheService } from '../../../infrastructure/cache/services/route-cache.service';
import { StorageConfigCacheService } from '../../../infrastructure/cache/services/storage-config-cache.service';
import { SystemProtectionService } from '../../dynamic-api/services/system-protection.service';
import { TableValidationService } from '../../dynamic-api/services/table-validation.service';
import { SwaggerService } from '../../../infrastructure/swagger/services/swagger.service';
import { GraphqlService } from '../../graphql/services/graphql.service';
import { TDynamicContext } from '../../../shared/interfaces/dynamic-context.interface';
import { IToolCall, IToolResult } from '../interfaces/message.interface';
import { createLLMClient } from '../utils/llm-client.helper';
import { getTools } from '../utils/llm-tools.helper';
import { convertMessagesToAnthropic } from '../utils/message-converter.helper';
import { mapStatusCodeToHttpStatus, getErrorCodeFromStatus } from '../utils/error-handler.helper';
import { handleAnthropicStream } from '../utils/anthropic-stream.helper';
import { streamAnthropicToClient, StreamEvent } from '../utils/anthropic-stream-client.helper';
import { createLLMContext } from '../utils/context.helper';
import { ToolExecutor } from '../utils/tool-executor.helper';

export interface LLMMessage {
  role: 'system' | 'user' | 'assistant' | 'tool';
  content: string | null;
  tool_calls?: Array<{
    id: string;
    type: 'function';
    function: {
      name: string;
      arguments: string;
    };
  }>;
  tool_call_id?: string;
}

export interface LLMResponse {
  content: string | null;
  toolCalls: IToolCall[];
  toolResults: IToolResult[];
}

@Injectable()
export class LLMService {
  private readonly logger = new Logger(LLMService.name);
  private readonly toolExecutor: ToolExecutor;

  constructor(
    private readonly aiConfigCacheService: AiConfigCacheService,
    private readonly metadataCacheService: MetadataCacheService,
    private readonly queryBuilder: QueryBuilderService,
    private readonly tableHandlerService: TableHandlerService,
    private readonly queryEngine: QueryEngine,
    private readonly routeCacheService: RouteCacheService,
    private readonly storageConfigCacheService: StorageConfigCacheService,
    private readonly systemProtectionService: SystemProtectionService,
    private readonly tableValidationService: TableValidationService,
    private readonly swaggerService: SwaggerService,
    private readonly graphqlService: GraphqlService,
  ) {
    this.toolExecutor = new ToolExecutor(
      this.metadataCacheService,
      this.queryBuilder,
      this.tableHandlerService,
      this.queryEngine,
      this.routeCacheService,
      this.storageConfigCacheService,
      this.aiConfigCacheService,
      this.systemProtectionService,
      this.tableValidationService,
      this.swaggerService,
      this.graphqlService,
    );
  }

  async chat(
    messages: LLMMessage[],
    configId: number,
  ): Promise<LLMResponse> {
    const config = await this.aiConfigCacheService.getConfigById(configId);
    if (!config) {
      throw new BadRequestException(`AI config with ID ${configId} not found`);
    }

    if (!config.isEnabled) {
      throw new BadRequestException(`AI config with ID ${configId} is disabled`);
    }

    const client = await createLLMClient(config);
    const provider = config.provider;
    const tools = getTools(provider);
    const model = config.model;
    const timeout = config.llmTimeout;
    const allToolCalls: IToolCall[] = [];
    const allToolResults: IToolResult[] = [];
    const context = createLLMContext();

    const conversationMessages: LLMMessage[] = [...messages];
    let maxIterations = 10;
    let iteration = 0;
    const baseTimeout = timeout || 30000;
    const startTime = Date.now();
    const totalTimeout = baseTimeout * 3;

    while (iteration < maxIterations) {
      iteration++;

      const elapsed = Date.now() - startTime;
      if (elapsed > totalTimeout) {
        throw new Error(`LLM request timeout after ${totalTimeout}ms (${iteration} iterations)`);
      }

      const remainingTimeout = totalTimeout - elapsed;
      const currentRequestTimeout = Math.max(remainingTimeout, baseTimeout);

      try {
        if (provider === 'OpenAI') {
        const response = await Promise.race([
            (client as OpenAI).chat.completions.create({
            model,
            messages: conversationMessages as any,
            tools: tools as any,
            tool_choice: 'auto',
          }),
          new Promise((_, reject) =>
            setTimeout(() => reject(new Error(`LLM request timeout after ${currentRequestTimeout}ms`)), currentRequestTimeout),
          ),
        ]) as any;

        const choice = response.choices[0];
        if (!choice) {
          throw new Error('No response from LLM');
        }

        const message = choice.message;

        if (message.tool_calls && message.tool_calls.length > 0) {
          conversationMessages.push({
            role: 'assistant',
            content: message.content,
            tool_calls: message.tool_calls,
          });

          const toolResults: LLMMessage[] = [];

          for (const toolCall of message.tool_calls) {
            allToolCalls.push({
              id: toolCall.id,
              type: 'function',
              function: {
                name: toolCall.function.name,
                arguments: toolCall.function.arguments,
              },
            });

            try {
              this.logger.debug(`Executing tool: ${toolCall.function.name} with args:`, JSON.parse(toolCall.function.arguments));
              const result = await this.toolExecutor.executeTool(toolCall, context);
              const resultStr = JSON.stringify(result);

              allToolResults.push({
                toolCallId: toolCall.id,
                result,
              });

              toolResults.push({
                role: 'tool',
                content: resultStr,
                tool_call_id: toolCall.id,
              });
            } catch (error: any) {
              const errorStr = JSON.stringify({ error: error.message || String(error) });

              allToolResults.push({
                toolCallId: toolCall.id,
                result: { error: error.message || String(error) },
              });

              toolResults.push({
                role: 'tool',
                content: errorStr,
                tool_call_id: toolCall.id,
              });
            }
          }

          conversationMessages.push(...toolResults);
        } else {
          return {
            content: message.content,
            toolCalls: allToolCalls,
            toolResults: allToolResults,
          };
          }
        } else if (provider === 'Anthropic') {
          const anthropicMessages = convertMessagesToAnthropic(conversationMessages);
          const systemMessage = anthropicMessages.find(m => m.role === 'system');
          const nonSystemMessages = anthropicMessages.filter(m => m.role !== 'system');
          
          const systemPromptLength = (systemMessage?.content as string)?.length || 0;
          const messagesLength = nonSystemMessages.reduce((sum, msg) => {
            if (typeof msg.content === 'string') {
              return sum + msg.content.length;
            } else if (Array.isArray(msg.content)) {
              return sum + JSON.stringify(msg.content).length;
            }
            return sum;
          }, 0);
          const toolsLength = JSON.stringify(tools).length;
          
          this.logger.log(`Request size - System: ${systemPromptLength} chars, Messages: ${messagesLength} chars, Tools: ${toolsLength} chars, Total: ${systemPromptLength + messagesLength + toolsLength} chars`);
          
          const elapsed = Date.now() - startTime;
          if (elapsed > totalTimeout) {
            throw new Error(`LLM request timeout after ${totalTimeout}ms (${iteration} iterations)`);
          }

          const remainingTimeout = totalTimeout - elapsed;
          const currentRequestTimeout = Math.max(remainingTimeout, baseTimeout);

          const stream = (client as Anthropic).messages.stream({
            model: model,
            max_tokens: 4096,
            system: systemMessage?.content as string,
            messages: nonSystemMessages as any,
            tools: tools as any,
          });

          const streamData = await handleAnthropicStream(stream, currentRequestTimeout);

          this.logger.log(`Input tokens: ${streamData.inputTokens}, Output tokens: ${streamData.outputTokens}`);

          const textContent = streamData.textContent;

          if (streamData.stop_reason === 'tool_use' && streamData.toolCalls.length > 0) {
            const toolCalls = streamData.toolCalls;

            conversationMessages.push({
              role: 'assistant',
              content: textContent || null,
              tool_calls: toolCalls,
            });

            const toolResults: LLMMessage[] = [];

            for (const toolCall of toolCalls) {
              allToolCalls.push({
                id: toolCall.id,
                type: 'function',
                function: {
                  name: toolCall.function.name,
                  arguments: toolCall.function.arguments,
                },
              });

              try {
                const toolCallObj = {
                  id: toolCall.id,
                  function: {
                    name: toolCall.function.name,
                    arguments: toolCall.function.arguments,
                  },
                };
                const result = await this.toolExecutor.executeTool(toolCallObj, context);
                const resultStr = JSON.stringify(result);

                allToolResults.push({
                  toolCallId: toolCall.id,
                  result,
                });

                toolResults.push({
                  role: 'tool',
                  content: resultStr,
                  tool_call_id: toolCall.id,
                });
              } catch (error: any) {
                const errorStr = JSON.stringify({ error: error.message || String(error) });

                allToolResults.push({
                  toolCallId: toolCall.id,
                  result: { error: error.message || String(error) },
                });

                toolResults.push({
                  role: 'tool',
                  content: errorStr,
                  tool_call_id: toolCall.id,
                });
              }
            }

            conversationMessages.push(...toolResults);
          } else {
            return {
              content: textContent || null,
              toolCalls: allToolCalls,
              toolResults: allToolResults,
            };
          }
        }
      } catch (error: any) {
        this.logger.error('LLM chat error:', error);
        const errorMessage = error?.message || String(error);
        
        const statusCode = error?.status || error?.statusCode || error?.response?.status;
        
        if (statusCode) {
          const httpStatus = mapStatusCodeToHttpStatus(statusCode);
          throw new HttpException(
            {
              message: errorMessage,
              code: getErrorCodeFromStatus(httpStatus),
            },
            httpStatus,
          );
        }
        
        throw new HttpException(
          {
            message: errorMessage,
            code: 'LLM_ERROR',
          },
          HttpStatus.INTERNAL_SERVER_ERROR,
        );
      }
    }

    throw new Error('Max iterations reached in tool calling loop');
  }

  async chatStream(
    messages: LLMMessage[],
    configId: number,
    onEvent: (event: StreamEvent) => void,
  ): Promise<LLMResponse> {
    const config = await this.aiConfigCacheService.getConfigById(configId);
    if (!config) {
      throw new BadRequestException(`AI config with ID ${configId} not found`);
    }

    if (!config.isEnabled) {
      throw new BadRequestException(`AI config with ID ${configId} is disabled`);
    }

    const client = await createLLMClient(config);
    const provider = config.provider;
    const tools = getTools(provider);
    const model = config.model;
    const timeout = config.llmTimeout;
    const allToolCalls: IToolCall[] = [];
    const allToolResults: IToolResult[] = [];
    const context = createLLMContext();

    const conversationMessages: LLMMessage[] = [...messages];
    let maxIterations = 10;
    let iteration = 0;
    const baseTimeout = timeout || 30000;
    const startTime = Date.now();
    const totalTimeout = baseTimeout * 3;

    while (iteration < maxIterations) {
      iteration++;

      const elapsed = Date.now() - startTime;
      if (elapsed > totalTimeout) {
        throw new Error(`LLM request timeout after ${totalTimeout}ms (${iteration} iterations)`);
      }

      const remainingTimeout = totalTimeout - elapsed;
      const currentRequestTimeout = Math.max(remainingTimeout, baseTimeout);

      try {
        if (provider === 'OpenAI') {
          throw new BadRequestException('OpenAI streaming is not yet implemented');
        } else if (provider === 'Anthropic') {
          const anthropicMessages = convertMessagesToAnthropic(conversationMessages);
          const systemMessage = anthropicMessages.find(m => m.role === 'system');
          const nonSystemMessages = anthropicMessages.filter(m => m.role !== 'system');

          const stream = (client as Anthropic).messages.stream({
            model: model,
            max_tokens: 4096,
            system: systemMessage?.content as string,
            messages: nonSystemMessages as any,
            tools: tools as any,
          });

          const streamData = await streamAnthropicToClient(stream, currentRequestTimeout, onEvent);

          this.logger.log(`Input tokens: ${streamData.inputTokens}, Output tokens: ${streamData.outputTokens}`);

          const textContent = streamData.textContent;

          if (streamData.stop_reason === 'tool_use' && streamData.toolCalls.length > 0) {
            const toolCalls = streamData.toolCalls;

            conversationMessages.push({
              role: 'assistant',
              content: textContent || null,
              tool_calls: toolCalls,
            });

            const toolResults: LLMMessage[] = [];

            for (const toolCall of toolCalls) {
              allToolCalls.push({
                id: toolCall.id,
                type: 'function',
                function: {
                  name: toolCall.function.name,
                  arguments: toolCall.function.arguments,
                },
              });

              try {
                const toolCallObj = {
                  id: toolCall.id,
                  function: {
                    name: toolCall.function.name,
                    arguments: toolCall.function.arguments,
                  },
                };
                const result = await this.toolExecutor.executeTool(toolCallObj, context);
                const resultStr = JSON.stringify(result);

                allToolResults.push({
                  toolCallId: toolCall.id,
                  result,
                });

                onEvent({
                  type: 'tool_result',
                  data: {
                    toolCallId: toolCall.id,
                    name: toolCall.function.name,
                    result,
                  },
                });

                toolResults.push({
                  role: 'tool',
                  content: resultStr,
                  tool_call_id: toolCall.id,
                });
              } catch (error: any) {
                const errorStr = JSON.stringify({ error: error.message || String(error) });

                allToolResults.push({
                  toolCallId: toolCall.id,
                  result: { error: error.message || String(error) },
                });

                onEvent({
                  type: 'tool_result',
                  data: {
                    toolCallId: toolCall.id,
                    name: toolCall.function.name,
                    result: { error: error.message || String(error) },
                  },
                });

                toolResults.push({
                  role: 'tool',
                  content: errorStr,
                  tool_call_id: toolCall.id,
                });
              }
            }

            // Add newline after tool calls to prevent text concatenation
            onEvent({
              type: 'text',
              data: { delta: '\n', text: '\n' },
            });

            conversationMessages.push(...toolResults);
          } else {
            return {
              content: textContent || null,
              toolCalls: allToolCalls,
              toolResults: allToolResults,
            };
          }
        }
      } catch (error: any) {
        this.logger.error('LLM chat stream error:', error);
        const errorMessage = error?.message || String(error);
        
        onEvent({
          type: 'error',
          data: { error: errorMessage },
        });
        
        const statusCode = error?.status || error?.statusCode || error?.response?.status;
        
        if (statusCode) {
          const httpStatus = mapStatusCodeToHttpStatus(statusCode);
          throw new HttpException(
            {
              message: errorMessage,
              code: getErrorCodeFromStatus(httpStatus),
            },
            httpStatus,
          );
        }
        
        throw new HttpException(
          {
            message: errorMessage,
            code: 'LLM_ERROR',
          },
          HttpStatus.INTERNAL_SERVER_ERROR,
        );
      }
    }

    throw new Error('Max iterations reached in tool calling loop');
  }
}

