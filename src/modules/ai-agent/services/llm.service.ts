import { Injectable, Logger, BadRequestException, HttpException, HttpStatus } from '@nestjs/common';
import { ChatOpenAI } from '@langchain/openai';
import { ChatAnthropic } from '@langchain/anthropic';
const { HumanMessage, AIMessage, SystemMessage } = require('@langchain/core/messages');
import { z } from 'zod';

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
import { IToolCall, IToolResult } from '../interfaces/message.interface';
import { createLLMContext } from '../utils/context.helper';
import { ToolExecutor } from '../utils/tool-executor.helper';
import { StreamEvent } from '../interfaces/stream-event.interface';

export interface LLMMessage {
  role: 'system' | 'user' | 'assistant' | 'tool';
  content: string | null;
  tool_calls?: IToolCall[];
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

  private async createLLM(config: any): Promise<any> {
    if (config.provider === 'OpenAI') {
      return new ChatOpenAI({
        apiKey: config.apiKey,
        model: config.model?.trim(),
        timeout: config.llmTimeout || 30000,
      });
    }

    if (config.provider === 'Anthropic') {
      return new ChatAnthropic({
        apiKey: config.apiKey,
        model: config.model,
        temperature: 0.7,
        maxTokens: 4096,
      });
    }

    if (config.provider === 'Google') {
      const { ChatGoogleGenerativeAI } = require('@langchain/google-genai');
      return new ChatGoogleGenerativeAI({
        apiKey: config.apiKey,
        model: config.model?.trim() || 'gemini-2.0-flash-exp',
        temperature: 0.7,
        maxOutputTokens: 8192,
      });
    }

    throw new BadRequestException(`Unsupported LLM provider: ${config.provider}`);
  }

  private createTools(context: any): any[] {
    const toolDefFile = require('../utils/llm-tools.helper');
    const COMMON_TOOLS = toolDefFile.COMMON_TOOLS || [];

    return COMMON_TOOLS.map((toolDef: any) => {
      const zodSchema = this.convertParametersToZod(toolDef.parameters);

      return {
        name: toolDef.name,
        description: toolDef.description,
        schema: zodSchema,
        func: async (input: any) => {
          const toolCall = {
            id: `tool_${Date.now()}_${Math.random()}`,
            function: {
              name: toolDef.name,
              arguments: JSON.stringify(input),
            },
          };

          const result = await this.toolExecutor.executeTool(toolCall, context);
          return JSON.stringify(result);
        },
      };
    });
  }

  private convertParametersToZod(parameters: any): any {
    const props = parameters.properties || {};
    const required = parameters.required || [];

    const zodObj: any = {};

    for (const [key, value] of Object.entries(props)) {
      const propDef: any = value;
      let zodField: any;

      if (propDef.type === 'string') {
        zodField = z.string();
      } else if (propDef.type === 'number') {
        zodField = z.number();
      } else if (propDef.type === 'boolean') {
        zodField = z.boolean();
      } else if (propDef.type === 'array') {
        zodField = z.array(z.any());
      } else if (propDef.type === 'object' || propDef.type === undefined) {
        zodField = z.any();
      } else if (propDef.oneOf) {
        zodField = z.union([z.string(), z.number()]);
      } else {
        zodField = z.any();
      }

      if (propDef.enum) {
        zodField = z.enum(propDef.enum as [string, ...string[]]);
      }

      if (!required.includes(key)) {
        zodField = zodField.optional();
      }

      if (propDef.description) {
        zodField = zodField.describe(propDef.description);
      }

      zodObj[key] = zodField;
    }

    return z.object(zodObj);
  }

  private convertToLangChainMessages(messages: LLMMessage[]): any[] {
    const result: any[] = [];

    for (const msg of messages) {
      if (msg.role === 'system') {
        result.push(new SystemMessage(msg.content || ''));
      } else if (msg.role === 'user') {
        result.push(new HumanMessage(msg.content || ''));
      } else if (msg.role === 'assistant') {
        let toolCallsFormatted = undefined;

        if (msg.tool_calls && msg.tool_calls.length > 0) {
          toolCallsFormatted = msg.tool_calls.map((tc: any) => {
            const toolName = tc.function?.name || tc.name;
            let toolArgs = tc.function?.arguments || tc.arguments || tc.input || tc.args;
            if (typeof toolArgs === 'string') {
              try {
                toolArgs = JSON.parse(toolArgs);
              } catch (e) {
                this.logger.warn(`[convertToLangChainMessages] Failed to parse tool arguments: ${toolArgs}`);
                toolArgs = {};
              }
            }

            return {
              name: toolName,
              args: toolArgs || {},
              id: tc.id,
              type: 'tool_call' as const,
            };
          });
        }

        const aiMsg = new AIMessage({
          content: msg.content || '',
          tool_calls: toolCallsFormatted || [],
        });
        result.push(aiMsg);
      } else if (msg.role === 'tool') {
        const ToolMessage = require('@langchain/core/messages').ToolMessage;
        result.push(
          new ToolMessage({
            content: msg.content || '',
            tool_call_id: msg.tool_call_id,
          }),
        );
      }
    }

    return result;
  }

  async chat(params: {
    messages: LLMMessage[];
    configId: string | number;
    user?: any;
  }): Promise<LLMResponse> {
    const { messages, configId, user } = params;

    const config = await this.aiConfigCacheService.getConfigById(configId);
    if (!config || !config.isEnabled) {
      throw new BadRequestException(`AI config ${configId} not found or disabled`);
    }

    try {
      const llm = await this.createLLM(config);
      const context = createLLMContext(user);
      const tools = this.createTools(context);

      const llmWithTools = (llm as any).bindTools(tools);

      const conversationMessages = this.convertToLangChainMessages(messages);

      const allToolCalls: IToolCall[] = [];
      const allToolResults: IToolResult[] = [];

      const maxIterations = 10;

      for (let iterations = 0; iterations < maxIterations; iterations++) {
        const result: any = await llmWithTools.invoke(conversationMessages);

        const toolCalls = result.tool_calls || result.additional_kwargs?.tool_calls || [];

        if (toolCalls.length === 0) {
          this.reportTokenUsage('chat', result);
          this.logger.log(`[LLM] Tool calls executed: ${allToolCalls.length}`);
          return {
            content: result.content || '',
            toolCalls: allToolCalls,
            toolResults: allToolResults,
          };
        }

        conversationMessages.push(result);

        for (const tc of toolCalls) {
          const toolName = tc.function?.name || tc.name;
          const toolArgs = tc.function?.arguments || tc.arguments;
          const toolId = tc.id;

          if (!toolName) {
            this.logger.error(`[LLM Chat] Tool name is undefined. Full tool call: ${JSON.stringify(tc)}`);
            continue;
          }

          if (!toolId) {
            this.logger.error(`[LLM Chat] Tool ID is missing for ${toolName}. Full tool call: ${JSON.stringify(tc)}`);
            continue;
          }

          allToolCalls.push({
            id: toolId,
            type: 'function',
            function: {
              name: toolName,
              arguments: typeof toolArgs === 'string' ? toolArgs : JSON.stringify(toolArgs || {}),
            },
          });

          try {
            const tool = tools.find((t) => t.name === toolName);
            if (!tool) {
              throw new Error(`Tool ${toolName} not found`);
            }

            let parsedArgs: any;
            if (typeof toolArgs === 'string') {
              try {
                parsedArgs = JSON.parse(toolArgs);
              } catch (parseError: any) {
                this.logger.error(`[LLM Chat] Failed to parse tool args string: ${toolArgs}`);
                throw new Error(`Invalid JSON in tool arguments: ${parseError.message}`);
              }
            } else if (typeof toolArgs === 'object' && toolArgs !== null) {
              parsedArgs = toolArgs;
            } else {
              this.logger.warn(`[LLM Chat] Tool args is ${typeof toolArgs}, using empty object`);
              parsedArgs = {};
            }

            const toolResult = await tool.func(parsedArgs);

            allToolResults.push({
              toolCallId: toolId,
              result: typeof toolResult === 'string' ? JSON.parse(toolResult) : toolResult,
            });

            const ToolMessage = require('@langchain/core/messages').ToolMessage;
            conversationMessages.push(
              new ToolMessage({
                content: toolResult,
                tool_call_id: toolId,
              }),
            );
          } catch (error: any) {
            this.logger.error(`Tool execution failed: ${toolName}`, error);

            const errorResult = { error: error.message || String(error) };
            allToolResults.push({
              toolCallId: toolId,
              result: errorResult,
            });

            const ToolMessage = require('@langchain/core/messages').ToolMessage;
            conversationMessages.push(
              new ToolMessage({
                content: JSON.stringify(errorResult),
                tool_call_id: toolId,
              }),
            );
          }
        }
      }

      throw new Error('Max iterations reached');
    } catch (error: any) {
      this.logger.error('[LLM] Error:', error);
      throw new HttpException(
        {
          message: error?.message || String(error),
          code: 'LLM_ERROR',
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  async chatStream(params: {
    messages: LLMMessage[];
    configId: string | number;
    abortSignal?: AbortSignal;
    onEvent: (event: StreamEvent) => void;
    user?: any;
  }): Promise<LLMResponse> {
    const { messages, configId, abortSignal, onEvent, user } = params;

    const config = await this.aiConfigCacheService.getConfigById(configId);
    if (!config || !config.isEnabled) {
      throw new BadRequestException(`AI config ${configId} not found or disabled`);
    }

    try {
      const llm = await this.createLLM(config);
      const context = createLLMContext(user);
      const tools = this.createTools(context);

      const llmWithTools = (llm as any).bindTools(tools);
      const provider = config.provider;
      const canStream = provider !== 'Google' && typeof llmWithTools.stream === 'function';

      let conversationMessages = this.convertToLangChainMessages(messages);

      let fullContent = '';
      const allToolCalls: IToolCall[] = [];
      const allToolResults: IToolResult[] = [];
      let iterations = 0;
      const maxIterations = config.maxToolIterations || 15;

      while (iterations < maxIterations) {
        iterations++;

        if (abortSignal?.aborted) {
          throw new Error('Request aborted by client');
        }

        let currentContent = '';
        let currentToolCalls: any[] = [];
        let streamError: Error | null = null;
        let aggregateResponse: any = null;

        try {
          if (canStream) {
            const stream = await llmWithTools.stream(conversationMessages);

            for await (const chunk of stream) {
              if (abortSignal?.aborted) {
                throw new Error('Request aborted by client');
              }

              if (aggregateResponse === null) {
                aggregateResponse = chunk;
              } else {
                aggregateResponse = aggregateResponse.concat(chunk);
              }

              if (chunk.content) {
                let delta = chunk.content;
                if (typeof delta !== 'string') {
                  if (Array.isArray(delta)) {
                    delta = delta
                      .filter((block) => block.type === 'text' && block.text)
                      .map((block) => block.text)
                      .join('');
                  } else if (typeof delta === 'object' && delta.text) {
                    delta = delta.text;
                  } else {
                    this.logger.warn(`[LLM Stream] chunk.content is unexpected type ${typeof delta}, stringifying`);
                    delta = JSON.stringify(delta);
                  }
                }

                currentContent += delta;
                fullContent += delta;

                onEvent({
                  type: 'text',
                  data: { delta, text: fullContent },
                });
              }
            }
          } else {
            aggregateResponse = await llmWithTools.invoke(conversationMessages);
            const delta = this.reduceContentToString(aggregateResponse?.content);
            if (delta) {
              currentContent += delta;
              fullContent += delta;
              onEvent({
                type: 'text',
                data: { delta, text: fullContent },
              });
            }
          }
        } catch (streamErr: any) {
          streamError = streamErr;
          this.logger.error(`[LLM Stream] Stream interrupted: ${streamErr.message}`);

          if (canStream && currentToolCalls.length > 0) {
            this.logger.warn(`[LLM Stream] Stream interrupted but recovered ${currentToolCalls.length} tool calls`);
            onEvent({
              type: 'text',
              data: {
                delta: '\n\n⚠️ Connection interrupted, attempting to continue with partial data...\n',
                text: fullContent + '\n\n⚠️ Connection interrupted, attempting to continue with partial data...\n',
              },
            });
          } else {
            throw streamErr;
          }
        }

        if (aggregateResponse) {
          const toolCalls = this.getToolCallsFromResponse(aggregateResponse);
          if (toolCalls.length > 0) {
            currentToolCalls = toolCalls;
          }
        }

        if (currentToolCalls.length === 0) {
          this.reportTokenUsage('stream', aggregateResponse, onEvent);
          this.logger.log(`[LLM] Tool calls executed: ${allToolCalls.length}`);
          return {
            content: fullContent,
            toolCalls: allToolCalls,
            toolResults: allToolResults,
          };
        }

        const langchainToolCalls = currentToolCalls.map((tc) => {
          const toolName = tc.function?.name || tc.name;
          let toolArgs = tc.args || tc.function?.arguments || tc.arguments;
          if (typeof toolArgs === 'string') {
            try {
              toolArgs = JSON.parse(toolArgs);
            } catch (e) {
              this.logger.warn(`[LLM Stream] Failed to parse tool arguments: ${toolArgs}`);
              toolArgs = {};
            }
          }

          return {
            name: toolName,
            args: toolArgs || {},
            id: tc.id,
            type: 'tool_call' as const,
          };
        });

        const aiMessageWithTools = new AIMessage({
          content: currentContent,
          tool_calls: langchainToolCalls,
        });
        conversationMessages.push(aiMessageWithTools);

        for (const tc of currentToolCalls) {
          const toolName = tc.function?.name || tc.name;
          const toolArgs = tc.args || tc.function?.arguments || tc.arguments;
          const toolId = tc.id;

          if (!toolName) {
            this.logger.error(`[LLM Stream] Tool name is undefined. Full tool call: ${JSON.stringify(tc)}`);
            continue;
          }

          if (!toolId) {
            this.logger.error(`[LLM Stream] Tool ID is missing for ${toolName}. Full tool call: ${JSON.stringify(tc)}`);
            continue;
          }

          onEvent({
            type: 'text',
            data: {
              delta: `\n\n🔧 ${toolName}...\n`,
              text: fullContent + `\n\n🔧 ${toolName}...\n`,
            },
          });

          allToolCalls.push({
            id: toolId,
            type: 'function',
            function: {
              name: toolName,
              arguments: typeof toolArgs === 'string' ? toolArgs : JSON.stringify(toolArgs || {}),
            },
          });

          try {
            const tool = tools.find((t) => t.name === toolName);
            if (!tool) {
              throw new Error(`Tool ${toolName} not found`);
            }

            let parsedArgs;
            if (typeof toolArgs === 'string') {
              try {
                parsedArgs = JSON.parse(toolArgs);
              } catch (parseError) {
                this.logger.error(`[LLM Stream] Failed to parse tool args string: ${toolArgs}`);
                throw new Error(`Invalid JSON in tool arguments: ${parseError.message}`);
              }
            } else if (typeof toolArgs === 'object' && toolArgs !== null) {
              parsedArgs = toolArgs;
            } else {
              this.logger.warn(`[LLM Stream] Tool args is ${typeof toolArgs}, using empty object`);
              parsedArgs = {};
            }

            const toolResult = await tool.func(parsedArgs);

            const resultObj = typeof toolResult === 'string' ? JSON.parse(toolResult) : toolResult;

            allToolResults.push({
              toolCallId: toolId,
              result: resultObj,
            });

            const serializableResult = JSON.parse(JSON.stringify(resultObj));

            onEvent({
              type: 'tool_result',
              data: {
                toolCallId: toolId,
                name: toolName,
                result: serializableResult,
              },
            });

            const ToolMessage = require('@langchain/core/messages').ToolMessage;
            conversationMessages.push(
              new ToolMessage({
                content: toolResult,
                tool_call_id: toolId,
              }),
            );
          } catch (error: any) {
            this.logger.error(`Tool failed: ${toolName}`, error);

            const errorResult = { error: error.message || String(error) };
            allToolResults.push({
              toolCallId: toolId,
              result: errorResult,
            });

            const serializableError = JSON.parse(JSON.stringify(errorResult));

            onEvent({
              type: 'tool_result',
              data: {
                toolCallId: toolId,
                name: toolName,
                result: serializableError,
              },
            });

            const ToolMessage = require('@langchain/core/messages').ToolMessage;
            conversationMessages.push(
              new ToolMessage({
                content: JSON.stringify(errorResult),
                tool_call_id: toolId,
              }),
            );
          }
        }
      }

      const lastToolError = allToolResults.reverse().find((item) => item?.result?.error);
      const fallbackMessage = lastToolError?.result?.message || 'Operation stopped because the model issued too many tool calls without completing.';
      return {
        content: fallbackMessage,
        toolCalls: allToolCalls,
        toolResults: allToolResults,
      };
    } catch (error: any) {
      this.logger.error('[LLM Stream] Error:', error);

      if (error.message === 'Request aborted by client') {
        throw error;
      }

      onEvent({
        type: 'error',
        data: { error: error.message || String(error) },
      });

      throw new HttpException(
        {
          message: error?.message || String(error),
          code: 'LLM_STREAM_ERROR',
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  async chatSimple(params: {
    messages: LLMMessage[];
    configId: string | number;
  }): Promise<LLMResponse> {
    const { messages, configId } = params;

    const config = await this.aiConfigCacheService.getConfigById(configId);
    if (!config || !config.isEnabled) {
      throw new BadRequestException(`AI config ${configId} not found or disabled`);
    }

    try {
      const llm = await this.createLLM(config);
      const lcMessages = this.convertToLangChainMessages(messages);

      const result: any = await (llm as any).invoke(lcMessages);

      this.reportTokenUsage('chatSimple', result);
      this.logger.log('[LLM] Tool calls executed: 0');

      return {
        content: result.content as string,
        toolCalls: [],
        toolResults: [],
      };
    } catch (error: any) {
      this.logger.error('[LLM Simple] Error:', error);
      throw new HttpException(
        {
          message: error?.message || String(error),
          code: 'LLM_SIMPLE_ERROR',
        },
        HttpStatus.INTERNAL_SERVER_ERROR,
      );
    }
  }

  private reduceContentToString(content: any): string {
    if (!content) {
      return '';
    }
    if (typeof content === 'string') {
      return content;
    }
    if (Array.isArray(content)) {
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
              return this.reduceContentToString(item.content);
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
        return this.reduceContentToString(content.content);
      }
    }
    return '';
  }

  private getToolCallsFromResponse(response: any): any[] {
    if (!response) {
      return [];
    }
    const direct = Array.isArray(response.tool_calls) ? response.tool_calls : null;
    if (direct && direct.length > 0) {
      return direct;
    }
    const additional = Array.isArray(response.additional_kwargs?.tool_calls)
      ? response.additional_kwargs.tool_calls
      : null;
    if (additional && additional.length > 0) {
      return additional;
    }
    const responseMeta = Array.isArray(response.response_metadata?.tool_calls)
      ? response.response_metadata.tool_calls
      : null;
    if (responseMeta && responseMeta.length > 0) {
      return responseMeta;
    }
    return [];
  }

  private reportTokenUsage(context: string, source: any, onEvent?: (event: StreamEvent) => void) {
    const usage = this.extractTokenUsage(source);
    if (!usage) {
      return;
    }

    const inputTokens = usage.inputTokens ?? 0;
    const outputTokens = usage.outputTokens ?? 0;

    if (onEvent) {
      onEvent({
        type: 'tokens',
        data: {
          inputTokens,
          outputTokens,
        },
      });
    }

    this.logger.log(`[LLM] Tokens (${context}) → input=${inputTokens}, output=${outputTokens}`);
  }

  private extractTokenUsage(source: any): { inputTokens?: number; outputTokens?: number } | null {
    if (!source) {
      return null;
    }

    const candidates = [
      source.usage_metadata,
      source.usage,
      source.response_metadata?.tokenUsage,
      source.response_metadata?.usage,
      source.response_metadata?.metadata?.tokenUsage,
      source.metadata?.tokenUsage,
    ];

    for (const usage of candidates) {
      if (!usage) {
        continue;
      }

      const input =
        usage.input_tokens ??
        usage.prompt_tokens ??
        usage.promptTokens ??
        usage.inputTokens ??
        usage.total_input_tokens ??
        usage.total_prompt_tokens;

      const output =
        usage.output_tokens ??
        usage.completion_tokens ??
        usage.completionTokens ??
        usage.outputTokens ??
        usage.total_output_tokens ??
        usage.total_completion_tokens;

      if (input !== undefined || output !== undefined) {
        return {
          inputTokens: input ?? 0,
          outputTokens: output ?? 0,
        };
      }
    }

    return null;
  }
}
