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
      const baseOptions: Record<string, any> = {
        apiKey: config.apiKey,
        model: config.model?.trim(),
        timeout: config.llmTimeout || 30000,
      };

      return new ChatOpenAI(baseOptions);
    }

    if (config.provider === 'Anthropic') {
      return new ChatAnthropic({
        apiKey: config.apiKey,
        model: config.model,
        temperature: config.temperature || 0.7,
        maxTokens: config.maxTokens || 4096,
      });
    }

    throw new BadRequestException(`Unsupported LLM provider: ${config.provider}`);
  }

  private createTools(context: any): any[] {
    const toolDefFile = require('../utils/llm-tools.helper');
    const COMMON_TOOLS = toolDefFile.COMMON_TOOLS || [];

    return COMMON_TOOLS.map((toolDef: any) => {
      const zodSchema = this.convertParametersToZod(toolDef.parameters);

      if (toolDef.name === 'get_table_details') {
        this.logger.debug(`[createTools] ${toolDef.name} schema: ${JSON.stringify(toolDef.parameters)}`);
        this.logger.debug(`[createTools] ${toolDef.name} required: ${JSON.stringify(toolDef.parameters.required)}`);
      }

      return {
        name: toolDef.name,
        description: toolDef.description,
        schema: zodSchema,
        func: async (input: any) => {
          this.logger.debug(`[Tool Execution] ${toolDef.name} called with input: ${JSON.stringify(input)}`);

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

      this.logger.debug(`[LLM Stream] Created ${tools.length} tools: ${tools.map(t => t.name).join(', ')}`);

      const llmWithTools = (llm as any).bindTools(tools);

      const lcMessages = this.convertToLangChainMessages(messages);

      if (lcMessages.length > 0) {
        const firstMsg = lcMessages[0];
        const content = typeof firstMsg.content === 'string' ? firstMsg.content : JSON.stringify(firstMsg.content);
        this.logger.debug(`[LLM Stream] First message role: ${firstMsg.constructor.name}, length: ${content.length} chars`);
      }

      let conversationMessages = [...lcMessages];
      const allToolCalls: IToolCall[] = [];
      const allToolResults: IToolResult[] = [];
      let iterations = 0;
      const maxIterations = 10;

      while (iterations < maxIterations) {
        iterations++;

        const result: any = await llmWithTools.invoke(conversationMessages);

        const toolCalls = result.tool_calls || result.additional_kwargs?.tool_calls || [];

        if (toolCalls.length === 0) {
          return {
            content: result.content || '',
            toolCalls: allToolCalls,
            toolResults: allToolResults,
          };
        }

        conversationMessages.push(result);

        for (const tc of toolCalls) {
          this.logger.debug(`[LLM Chat] Processing tool call: ${JSON.stringify(tc)}`);

          const toolName = tc.function?.name || tc.name;
          const toolArgs = tc.function?.arguments || tc.arguments;
          const toolId = tc.id;

          this.logger.debug(`[LLM Chat] Extracted - Name: ${toolName}, Args: ${typeof toolArgs}, ID: ${toolId}`);

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

            this.logger.debug(`[LLM Chat] Calling tool ${toolName} with args: ${JSON.stringify(parsedArgs)}`);
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

      this.logger.debug(`[LLM Stream] Created ${tools.length} tools: ${tools.map(t => t.name).join(', ')}`);

      const llmWithTools = (llm as any).bindTools(tools);

      let conversationMessages = this.convertToLangChainMessages(messages);

      if (conversationMessages.length > 0) {
        const firstMsg = conversationMessages[0];
        const content = typeof firstMsg.content === 'string' ? firstMsg.content : JSON.stringify(firstMsg.content);
        this.logger.debug(`[LLM Stream] First message role: ${firstMsg.constructor.name}, length: ${content.length} chars`);
      }

      let fullContent = '';
      const allToolCalls: IToolCall[] = [];
      const allToolResults: IToolResult[] = [];
      let iterations = 0;
      const maxIterations = 10;

      while (iterations < maxIterations) {
        iterations++;

        if (abortSignal?.aborted) {
          throw new Error('Request aborted by client');
        }

        this.logger.debug(`[LLM Stream] Iteration ${iterations}: conversationMessages.length = ${conversationMessages.length}`);
        conversationMessages.forEach((msg, idx) => {
          const roleInfo = msg.constructor.name;
          const hasToolCalls = msg.tool_calls?.length || msg.additional_kwargs?.tool_calls?.length || 0;
          const toolCallId = msg.tool_call_id || 'N/A';
          this.logger.debug(`  [${idx}] ${roleInfo}, tool_calls: ${hasToolCalls}, tool_call_id: ${toolCallId}`);
        });

        let currentContent = '';
        let currentToolCalls: any[] = [];
        let streamError: Error | null = null;
        let aggregateResponse: any = null;

        try {
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
                    .filter(block => block.type === 'text' && block.text)
                    .map(block => block.text)
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
        } catch (streamErr: any) {
          streamError = streamErr;
          this.logger.error(`[LLM Stream] Stream interrupted: ${streamErr.message}`);

          if (currentToolCalls.length > 0) {
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

        if (aggregateResponse && aggregateResponse.tool_calls) {
          currentToolCalls = aggregateResponse.tool_calls;
          this.logger.debug(`[LLM Stream] Extracted ${currentToolCalls.length} tool calls from aggregated response`);
        }

        this.logger.debug(`[LLM Stream] Stream finished. Tool calls: ${currentToolCalls.length}, Content length: ${currentContent.length}`);

        if (currentToolCalls.length > 0) {
          this.logger.debug(`[LLM Stream] Final tool calls: ${JSON.stringify(currentToolCalls, null, 2)}`);
        }

        if (currentToolCalls.length === 0) {
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

        this.logger.debug(`[LLM Stream] Added AI message to conversation. tool_calls count: ${langchainToolCalls.length}`);
        this.logger.debug(`[LLM Stream] AI message tool_calls: ${JSON.stringify(aiMessageWithTools.tool_calls, null, 2)}`);

        for (const tc of currentToolCalls) {
          this.logger.debug(`[LLM Stream] Processing tool call: ${JSON.stringify(tc)}`);

          const toolName = tc.function?.name || tc.name;
          const toolArgs = tc.args || tc.function?.arguments || tc.arguments;
          const toolId = tc.id;

          this.logger.debug(`[LLM Stream] Extracted - Name: ${toolName}, Args: ${typeof toolArgs}, ID: ${toolId}`);

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

            this.logger.debug(`[LLM Stream] Calling tool ${toolName} with args: ${JSON.stringify(parsedArgs)}`);
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

      throw new Error('Max iterations reached');
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
}
