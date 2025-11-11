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
import { streamOpenAIToClient } from '../utils/openai-stream-client.helper';
import { handleAnthropicStream } from '../utils/anthropic-stream.helper';
import { streamAnthropicToClient, StreamEvent } from '../utils/anthropic-stream-client.helper';
import { createLLMContext } from '../utils/context.helper';
import { ToolExecutor } from '../utils/tool-executor.helper';
import { applyPromptCaching } from '../utils/anthropic-cache.helper';
import { optimizeOpenAIMessages } from '../utils/openai-cache.helper';

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
    configId: string | number,
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
    this.logger.log(`[LLM][Stream] Resolved config ${configId} provider=${provider} model=${model}`);
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
          const optimizedMessages = optimizeOpenAIMessages(conversationMessages);
          const response = await Promise.race([
            (client as OpenAI).chat.completions.create({
            model,
            messages: optimizedMessages as any,
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

          // Apply prompt caching for cost optimization
          const cached = applyPromptCaching(
            systemMessage?.content as string,
            tools,
            nonSystemMessages,
          );

          const systemPromptLength = typeof cached.system === 'string' ? cached.system.length : JSON.stringify(cached.system).length;
          const messagesLength = cached.messages.reduce((sum, msg) => {
            if (typeof msg.content === 'string') {
              return sum + msg.content.length;
            } else if (Array.isArray(msg.content)) {
              return sum + JSON.stringify(msg.content).length;
            }
            return sum;
          }, 0);
          const toolsLength = JSON.stringify(cached.tools).length;

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
            system: cached.system as any,
            messages: cached.messages as any,
            tools: cached.tools as any,
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

  // Lightweight chat without tools (for summaries and low-token calls)
  async chatSimple(
    messages: LLMMessage[],
    configId: string | number,
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
    const model = config.model;
    const timeout = Math.min(config.llmTimeout || 30000, 30000);
    const conversationMessages: LLMMessage[] = [...messages];

    if (provider === 'OpenAI') {
      const response: any = await Promise.race([
        (client as OpenAI).chat.completions.create({
          model,
          messages: conversationMessages as any,
          // No tools, no function calling
        }),
        new Promise((_, reject) => setTimeout(() => reject(new Error(`LLM request timeout after ${timeout}ms`)), timeout)),
      ]);
      const choice = response.choices?.[0];
      const content = choice?.message?.content || '';
      return { content, toolCalls: [], toolResults: [] };
    } else if (provider === 'Anthropic') {
      this.logger.debug(`[chatSimple - Anthropic] Input messages count: ${conversationMessages.length}`);
      conversationMessages.forEach((msg, idx) => {
        this.logger.debug(`[chatSimple - Anthropic] Message ${idx}: role=${msg.role}, content=${typeof msg.content === 'string' ? msg.content.slice(0, 100) : JSON.stringify(msg.content).slice(0, 100)}`);
      });

      const anthropicMessages = convertMessagesToAnthropic(conversationMessages);
      this.logger.debug(`[chatSimple - Anthropic] Converted messages count: ${anthropicMessages.length}`);

      const systemMessage = anthropicMessages.find(m => m.role === 'system');
      const nonSystemMessages = anthropicMessages.filter(m => m.role !== 'system');

      this.logger.debug(`[chatSimple - Anthropic] System messages: ${systemMessage ? 1 : 0}, Non-system messages: ${nonSystemMessages.length}`);

      // Ensure we have at least one message for Anthropic API
      if (nonSystemMessages.length === 0) {
        this.logger.error(`[chatSimple - Anthropic] No non-system messages after conversion. Original messages: ${JSON.stringify(conversationMessages.map(m => ({role: m.role, hasContent: !!m.content})))}`);
        throw new BadRequestException('At least one user or assistant message is required for Anthropic API');
      }

      const msg = await (client as Anthropic).messages.create({
        model,
        max_tokens: 1024,
        system: systemMessage?.content as any,
        messages: nonSystemMessages as any,
      });
      const content = (msg.content || [])
        .filter((c: any) => c.type === 'text')
        .map((c: any) => c.text)
        .join('');
      return { content, toolCalls: [], toolResults: [] };
    }

    throw new BadRequestException(`Unsupported provider: ${provider}`);
  }

  async chatStream(
    messages: LLMMessage[],
    configId: string | number,
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
      const iterationStartTime = Date.now();
      this.logger.log(`[Tool Loop - Stream] Starting iteration ${iteration}`);

      const elapsed = Date.now() - startTime;
      if (elapsed > totalTimeout) {
        throw new Error(`LLM request timeout after ${totalTimeout}ms (${iteration} iterations)`);
      }

      const remainingTimeout = totalTimeout - elapsed;
      const currentRequestTimeout = Math.max(remainingTimeout, baseTimeout);

      try {
        if (provider === 'OpenAI') {
          const optimizedMessages = optimizeOpenAIMessages(conversationMessages);
          const stream = await (client as OpenAI).chat.completions.create({
            model: model,
            messages: optimizedMessages as any,
            tools: tools as any,
            tool_choice: 'auto',
            stream: true,
          });

          const streamData = await streamOpenAIToClient(stream, currentRequestTimeout, onEvent);

          this.logger.log(`Input tokens: ${streamData.inputTokens}, Output tokens: ${streamData.outputTokens}`);

          const textContent = streamData.textContent;

          if (streamData.stop_reason === 'tool_calls' && streamData.toolCalls.length > 0) {
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
                const toolStartTime = Date.now();
                this.logger.log(`[Tool Execution - OpenAI Stream] Starting tool: ${toolCall.function.name}`);

                const toolCallObj = {
                  id: toolCall.id,
                  function: {
                    name: toolCall.function.name,
                    arguments: toolCall.function.arguments,
                  },
                };
                const result = await this.toolExecutor.executeTool(toolCallObj, context);
                const resultStr = JSON.stringify(result);

                const toolDuration = Date.now() - toolStartTime;
                this.logger.log(`[Tool Execution - OpenAI Stream] Completed tool: ${toolCall.function.name} in ${toolDuration}ms`);

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
                this.logger.error(`[Tool Execution - OpenAI Stream] Failed tool: ${toolCall.function.name}`, error);

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

            const iterationDuration = Date.now() - iterationStartTime;
            this.logger.log(`[Tool Loop - Stream] Iteration ${iteration} completed in ${iterationDuration}ms, continuing to next iteration`);
            continue;
          } else {
            const iterationDuration = Date.now() - iterationStartTime;
            const totalDuration = Date.now() - startTime;
            this.logger.log(`[Tool Loop - Stream] Iteration ${iteration} completed in ${iterationDuration}ms. Final response generated. Total time: ${totalDuration}ms across ${iteration} iterations`);

            return {
              content: textContent,
              toolCalls: allToolCalls,
              toolResults: allToolResults,
            };
          }
        } else if (provider === 'Anthropic') {
          const anthropicMessages = convertMessagesToAnthropic(conversationMessages);
          const systemMessage = anthropicMessages.find(m => m.role === 'system');
          const nonSystemMessages = anthropicMessages.filter(m => m.role !== 'system');

          // Apply prompt caching for cost optimization
          const cached = applyPromptCaching(
            systemMessage?.content as string,
            tools,
            nonSystemMessages,
          );

          const stream = (client as Anthropic).messages.stream({
            model: model,
            max_tokens: 4096,
            system: cached.system as any,
            messages: cached.messages as any,
            tools: cached.tools as any,
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

