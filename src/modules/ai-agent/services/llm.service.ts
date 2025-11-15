
import { Injectable, Logger, BadRequestException, HttpException, HttpStatus } from '@nestjs/common';
import { ChatOpenAI } from '@langchain/openai';
import { ChatAnthropic } from '@langchain/anthropic';
import { ChatDeepSeek } from '@langchain/deepseek';
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
import { ConversationService } from './conversation.service';

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
  toolLoops?: number;
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
    private readonly conversationService: ConversationService,
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
      this.conversationService,
    );
  }

  private async createLLM(config: any): Promise<any> {
    if (config.provider === 'OpenAI') {
      return new ChatOpenAI({
        apiKey: config.apiKey,
        model: config.model?.trim(),
        timeout: config.llmTimeout || 30000,
        streaming: true,
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

    if (config.provider === 'DeepSeek') {
      return new ChatDeepSeek({
        apiKey: config.apiKey,
        model: config.model?.trim() || 'deepseek-chat',
        timeout: config.llmTimeout || 30000,
        streaming: true,
      });
    }

    throw new BadRequestException(`Unsupported LLM provider: ${config.provider}`);
  }

  private createTools(context: any, abortSignal?: AbortSignal, selectedToolNames?: string[]): any[] {
    const toolDefFile = require('../utils/llm-tools.helper');
    const COMMON_TOOLS = toolDefFile.COMMON_TOOLS || [];

    const toolsToCreate = selectedToolNames && selectedToolNames.length > 0
      ? COMMON_TOOLS.filter((tool: any) => selectedToolNames.includes(tool.name))
      : COMMON_TOOLS;

    return toolsToCreate.map((toolDef: any) => {
      const zodSchema = this.convertParametersToZod(toolDef.parameters);

      return {
        name: toolDef.name,
        description: toolDef.description,
        schema: zodSchema,
        func: async (input: any) => {
          if (abortSignal?.aborted) {
            throw new Error('Request aborted by client');
          }

          const toolCall = {
            id: `tool_${Date.now()}_${Math.random()}`,
            function: {
              name: toolDef.name,
              arguments: JSON.stringify(input),
            },
          };

          const result = await this.toolExecutor.executeTool(toolCall, context, abortSignal);
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

  async evaluateNeedsTools(params: {
    userMessage: string;
    configId: string | number;
    conversationHistory?: any[];
  }): Promise<string[]> {
    const { userMessage, configId, conversationHistory = [] } = params;

    const config = await this.aiConfigCacheService.getConfigById(configId);
    if (!config || !config.isEnabled) {
      return [];
    }

    try {
      const toolDefFile = require('../utils/llm-tools.helper');
      const TOOL_BINDS_TOOL = toolDefFile.TOOL_BINDS_TOOL;
      const COMMON_TOOLS = toolDefFile.COMMON_TOOLS || [];
      
      const hasToolCallsInHistory = conversationHistory.some((m: any) => 
        m.role === 'assistant' && m.toolCalls && m.toolCalls.length > 0
      );

      const systemPrompt = `Analyze the request and call tool_binds with tool names needed.

Request: "${userMessage}"
${hasToolCallsInHistory ? 'Previous messages used tools.' : ''}

Rules:
- Greetings (hello, thanks) → []
- Data ops (create/read/update/delete) → ["check_permission", "dynamic_repository"]
- Schema/metadata → ["get_table_details"] or ["get_metadata"]
- Need guidance → ["get_hint"]

Call tool_binds now.`;

      const llm = await this.createLLM(config);
      const toolBindsTool = this.createToolFromDefinition(TOOL_BINDS_TOOL);
      const llmWithToolBinds = (llm as any).bindTools([toolBindsTool]);

      const messages = [
        new SystemMessage(systemPrompt),
        new HumanMessage(userMessage),
      ];

      const response = await llmWithToolBinds.invoke(messages);
      const toolCalls = response.tool_calls || response.additional_kwargs?.tool_calls || [];
      
      if (toolCalls.length > 0 && toolCalls[0].name === 'tool_binds') {
        const toolArgs = typeof toolCalls[0].args === 'string' 
          ? JSON.parse(toolCalls[0].args) 
          : toolCalls[0].args || {};
        
        const selectedToolNames = toolArgs.toolNames || [];
        if (Array.isArray(selectedToolNames)) {
          return selectedToolNames.filter((tool: any) => 
            typeof tool === 'string' && COMMON_TOOLS.some((t: any) => t.name === tool)
          );
        }
      }
      
      return [];
    } catch (error) {
      this.logger.warn(`Failed to select tools, defaulting to empty: ${error}`);
      return [];
    }
  }

  private createToolFromDefinition(toolDef: any): any {
    const zodSchema = this.convertParametersToZod(toolDef.parameters);
    return {
      name: toolDef.name,
      description: toolDef.description,
      schema: zodSchema,
      func: async (input: any) => {
        return JSON.stringify(input);
      },
    };
  }

  async chat(params: {
    messages: LLMMessage[];
    configId: string | number;
    user?: any;
    conversationId?: string | number;
    selectedToolNames?: string[];
  }): Promise<LLMResponse> {
    const { messages, configId, user, conversationId, selectedToolNames } = params;

    const config = await this.aiConfigCacheService.getConfigById(configId);
    if (!config || !config.isEnabled) {
      throw new BadRequestException(`AI config ${configId} not found or disabled`);
    }

    try {
      const llm = await this.createLLM(config);
      
      let llmWithTools = llm;
      let tools: any[] = [];
      if (selectedToolNames && selectedToolNames.length > 0) {
        const context = createLLMContext(user);
        tools = this.createTools(context, undefined, selectedToolNames);
        llmWithTools = (llm as any).bindTools(tools);
      }

      const conversationMessages = this.convertToLangChainMessages(messages);

      const allToolCalls: IToolCall[] = [];
      const allToolResults: IToolResult[] = [];

      const maxIterations = 10;
      const cacheKey = conversationId ? `conv_${conversationId}` : undefined;

      for (let iterations = 0; iterations < maxIterations; iterations++) {
        const invokeOptions: any = {};
        if (config.provider === 'OpenAI' && cacheKey) {
          invokeOptions.promptCacheKey = cacheKey;
        }
        if (config.provider === 'Anthropic' && cacheKey) {
          invokeOptions.cache_control = { type: 'ephemeral' };
        }
        const result: any = await llmWithTools.invoke(conversationMessages, invokeOptions);

        const toolCalls = result.tool_calls || result.additional_kwargs?.tool_calls || [];

        if (toolCalls.length === 0) {
          this.reportTokenUsage('chat', result);
          return {
            content: result.content || '',
            toolCalls: allToolCalls,
            toolResults: allToolResults,
            toolLoops: iterations + 1,
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

          let parsedArgs: any = {};
          try {
            const tool = tools.find((t) => t.name === toolName);
            if (!tool) {
              throw new Error(`Tool ${toolName} not found`);
            }

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

            const resultObj = typeof toolResult === 'string' ? JSON.parse(toolResult) : toolResult;
            allToolResults.push({
              toolCallId: toolId,
              result: resultObj,
            });

            const ToolMessage = require('@langchain/core/messages').ToolMessage;
            const summarizedResult = this.summarizeToolResult(toolName, parsedArgs, resultObj);
            conversationMessages.push(
              new ToolMessage({
                content: summarizedResult,
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
            const summarizedError = this.summarizeToolResult(toolName, parsedArgs, errorResult);
            conversationMessages.push(
              new ToolMessage({
                content: summarizedError,
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
    conversationId?: string | number;
    selectedToolNames?: string[];
  }): Promise<LLMResponse> {
    const { messages, configId, abortSignal, onEvent, user, conversationId, selectedToolNames } = params;

    const config = await this.aiConfigCacheService.getConfigById(configId);
    if (!config || !config.isEnabled) {
      throw new BadRequestException(`AI config ${configId} not found or disabled`);
    }

    try {
      const llm = await this.createLLM(config);
      
      let llmWithTools = llm;
      let tools: any[] = [];
      if (selectedToolNames && selectedToolNames.length > 0) {
        const context = createLLMContext(user);
        tools = this.createTools(context, abortSignal, selectedToolNames);
        llmWithTools = (llm as any).bindTools(tools);
      }
      const provider = config.provider;
      const canStream = provider !== 'Google' && typeof llmWithTools.stream === 'function';

      let conversationMessages = this.convertToLangChainMessages(messages);

      let fullContent = '';
      const allToolCalls: IToolCall[] = [];
      const allToolResults: IToolResult[] = [];
      let iterations = 0;
      const maxIterations = config.maxToolIterations || 15;
      let accumulatedTokenUsage: { inputTokens: number; outputTokens: number } = { inputTokens: 0, outputTokens: 0 };
      const cacheKey = conversationId ? `conv_${conversationId}` : undefined;

      while (iterations < maxIterations) {
        iterations++;

        if (abortSignal?.aborted) {
          throw new Error('Request aborted by client');
        }

        let currentContent = '';
        let currentToolCalls: any[] = [];
        let streamError: Error | null = null;
        let aggregateResponse: any = null;
        const streamedToolCallIds = new Set<string>();

        try {
          if (canStream) {
            const streamOptions: any = {};
            if (config.provider === 'OpenAI' && cacheKey) {
              streamOptions.promptCacheKey = cacheKey;
            }
            if (config.provider === 'Anthropic' && cacheKey) {
              streamOptions.cache_control = { type: 'ephemeral' };
            }
            const stream = await llmWithTools.stream(conversationMessages, streamOptions);
            const allChunks: any[] = [];
            const aggregatedToolCalls: Map<number, any> = new Map();

            for await (const chunk of stream) {
              if (abortSignal?.aborted) {
                throw new Error('Request aborted by client');
              }

              allChunks.push(chunk);
              aggregateResponse = chunk;

              const chunkUsage = this.extractTokenUsage(chunk);
              if (chunkUsage && (chunkUsage.inputTokens || chunkUsage.outputTokens)) {
                const prevInput = accumulatedTokenUsage.inputTokens;
                const prevOutput = accumulatedTokenUsage.outputTokens;
                accumulatedTokenUsage.inputTokens = Math.max(prevInput, chunkUsage.inputTokens ?? 0);
                accumulatedTokenUsage.outputTokens += chunkUsage.outputTokens ?? 0;
                
                if (accumulatedTokenUsage.inputTokens > prevInput || accumulatedTokenUsage.outputTokens > prevOutput) {
                  onEvent({
                    type: 'tokens',
                    data: {
                      inputTokens: accumulatedTokenUsage.inputTokens,
                      outputTokens: accumulatedTokenUsage.outputTokens,
                    },
                  });
                }
              }

              const chunkToolCalls = this.getToolCallsFromResponse(chunk);
              if (chunkToolCalls.length > 0) {
                for (const tc of chunkToolCalls) {
                  const index = tc.index !== undefined ? tc.index : (aggregatedToolCalls.size);
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

                  const streamKey = toolId || `index_${index}`;
                  if (toolName && toolId) {
                    let toolCallArgs = {};
                    if (mergedArgs && typeof mergedArgs === 'string' && mergedArgs.trim() && mergedArgs !== '{}') {
                      try {
                        toolCallArgs = JSON.parse(mergedArgs);
                      } catch (e) {
                        toolCallArgs = {};
                      }
                    } else if (mergedArgs && typeof mergedArgs === 'object' && Object.keys(mergedArgs).length > 0) {
                      toolCallArgs = mergedArgs;
                    }
                    
                    const hasValidArgs = Object.keys(toolCallArgs).length > 0;
                    const toolsWithoutArgs = ['list_tables'];
                    const canHaveEmptyArgs = toolsWithoutArgs.includes(toolName);
                    const shouldEmit = hasValidArgs || canHaveEmptyArgs;
                    
                    if (!streamedToolCallIds.has(streamKey)) {
                      if (shouldEmit) {
                        streamedToolCallIds.add(streamKey);
                        onEvent({
                          type: 'tool_call',
                          data: {
                            id: toolId,
                            name: toolName,
                            arguments: toolCallArgs,
                          },
                        });
                      }
                    } else if (shouldEmit) {
                      onEvent({
                        type: 'tool_call',
                        data: {
                          id: toolId,
                          name: toolName,
                          arguments: toolCallArgs,
                        },
                      });
                    }
                  }
                }
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
                    delta = JSON.stringify(delta);
                  }
                }

                currentContent += delta;
                fullContent += delta;

                onEvent({
                  type: 'text',
                  data: { delta },
                });
              }
            }

            if (aggregatedToolCalls.size > 0) {
              currentToolCalls = Array.from(aggregatedToolCalls.values()).map((tc) => {
                const argsString = tc.args || tc.function?.arguments || '';
                const toolName = tc.function?.name || tc.name || 'unknown';
                const toolId = tc.id;
                
                if (argsString && typeof argsString === 'string' && argsString.trim()) {
                  try {
                    const parsed = JSON.parse(argsString);
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
                  } catch (e) {
                    return {
                      ...tc,
                      id: toolId,
                      function: {
                        ...tc.function,
                        name: toolName,
                      },
                    };
                  }
                }
                return {
                  ...tc,
                  id: toolId,
                  function: {
                    ...tc.function,
                    name: toolName,
                  },
                };
              });
            } else if (allChunks.length > 0) {
              for (let i = allChunks.length - 1; i >= 0; i--) {
                const chunk = allChunks[i];
                const toolCalls = this.getToolCallsFromResponse(chunk);
                if (toolCalls.length > 0) {
                  currentToolCalls = toolCalls;
                  break;
                }
              }
              
            }
          } else {
            aggregateResponse = await llmWithTools.invoke(conversationMessages);
            const usage = this.extractTokenUsage(aggregateResponse);
            if (usage) {
              accumulatedTokenUsage.inputTokens += usage.inputTokens ?? 0;
              accumulatedTokenUsage.outputTokens += usage.outputTokens ?? 0;
              
              onEvent({
                type: 'tokens',
                data: {
                  inputTokens: accumulatedTokenUsage.inputTokens,
                  outputTokens: accumulatedTokenUsage.outputTokens,
                },
              });
            }
            
            const fullDelta = this.reduceContentToString(aggregateResponse?.content);
            if (fullDelta) {
              await this.streamChunkedContent(fullDelta, abortSignal, (chunk) => {
                currentContent += chunk;
                fullContent += chunk;
                onEvent({
                  type: 'text',
                  data: { delta: chunk },
                });
              });
            }
          }
        } catch (streamErr: any) {
          streamError = streamErr;
          const errorMessage = streamErr?.message || streamErr?.cause?.message || String(streamErr);
          const isSocketError = errorMessage.includes('UND_ERR_SOCKET') || 
                                errorMessage.includes('other side closed') ||
                                errorMessage === 'terminated' ||
                                streamErr?.code === 'UND_ERR_SOCKET';
          
          this.logger.error(`[LLM Stream] Stream interrupted: ${errorMessage}`, streamErr);

          if (abortSignal?.aborted) {
            throw new Error('Request aborted by client');
          }

          if (isSocketError && (canStream || currentToolCalls.length > 0 || allToolCalls.length > 0)) {
            onEvent({
              type: 'text',
              data: {
                delta: '\n\n⚠️ Connection interrupted by provider, continuing with available data...\n',
              },
            });
            
            if (currentToolCalls.length === 0 && allToolCalls.length > 0) {
              const finalUsage = this.extractTokenUsage(aggregateResponse);
              if (finalUsage) {
                accumulatedTokenUsage.inputTokens = Math.max(accumulatedTokenUsage.inputTokens, finalUsage.inputTokens ?? 0);
                accumulatedTokenUsage.outputTokens += finalUsage.outputTokens ?? 0;
              }
              
              if (accumulatedTokenUsage.inputTokens > 0 || accumulatedTokenUsage.outputTokens > 0) {
                onEvent({
                  type: 'tokens',
                  data: {
                    inputTokens: accumulatedTokenUsage.inputTokens,
                    outputTokens: accumulatedTokenUsage.outputTokens,
                  },
                });
              }
              
              this.reportTokenUsage('stream', aggregateResponse);
              return {
                content: fullContent,
                toolCalls: allToolCalls,
                toolResults: allToolResults,
                toolLoops: iterations,
              };
            }
          } else if (canStream && currentToolCalls.length > 0) {
            onEvent({
              type: 'text',
              data: {
                delta: '\n\n⚠️ Connection interrupted, attempting to continue with partial data...\n',
              },
            });
          } else {
            onEvent({
              type: 'error',
              data: { error: isSocketError ? 'Connection closed by provider. Please try again.' : (errorMessage || 'Stream error') },
            });
            throw new HttpException(
              {
                message: isSocketError ? 'Connection closed by LLM provider. Please try again.' : errorMessage,
                code: 'LLM_STREAM_ERROR',
              },
              HttpStatus.INTERNAL_SERVER_ERROR,
            );
          }
        }

        if (aggregateResponse && currentToolCalls.length === 0) {
          const toolCalls = this.getToolCallsFromResponse(aggregateResponse);
          if (toolCalls.length > 0) {
            currentToolCalls = toolCalls;
          }
        }

        if (currentToolCalls.length === 0) {
          const finalUsage = this.extractTokenUsage(aggregateResponse);
          if (finalUsage) {
            accumulatedTokenUsage.inputTokens = Math.max(accumulatedTokenUsage.inputTokens, finalUsage.inputTokens ?? 0);
            accumulatedTokenUsage.outputTokens += finalUsage.outputTokens ?? 0;
          }
          
          if (accumulatedTokenUsage.inputTokens > 0 || accumulatedTokenUsage.outputTokens > 0) {
            onEvent({
              type: 'tokens',
              data: {
                inputTokens: accumulatedTokenUsage.inputTokens,
                outputTokens: accumulatedTokenUsage.outputTokens,
              },
            });
          }
          
          const finalContent = this.reduceContentToString(aggregateResponse?.content) || '';
          if (finalContent) {
            const previousFullContentLength = fullContent.length - currentContent.length;
            const expectedFullContent = fullContent.substring(0, previousFullContentLength) + finalContent;
            if (expectedFullContent.length > fullContent.length) {
              const newContent = expectedFullContent.substring(fullContent.length);
              if (newContent) {
                fullContent = expectedFullContent;
                await this.streamChunkedContent(newContent, abortSignal, (chunk) => {
                  onEvent({
                    type: 'text',
                    data: {
                      delta: chunk,
                    },
                  });
                });
              }
            }
          }
          
          this.reportTokenUsage('stream', aggregateResponse);
          return {
            content: fullContent,
            toolCalls: allToolCalls,
            toolResults: allToolResults,
            toolLoops: iterations,
          };
        }

        const iterationUsage = this.extractTokenUsage(aggregateResponse);
        if (iterationUsage) {
          accumulatedTokenUsage.inputTokens = Math.max(accumulatedTokenUsage.inputTokens, iterationUsage.inputTokens ?? 0);
          accumulatedTokenUsage.outputTokens += iterationUsage.outputTokens ?? 0;
          
          onEvent({
            type: 'tokens',
            data: {
              inputTokens: accumulatedTokenUsage.inputTokens,
              outputTokens: accumulatedTokenUsage.outputTokens,
            },
          });
        }

        const validToolCalls: any[] = [];
        const toolCallIdMap = new Map<string, any>();

        for (const tc of currentToolCalls) {
          const toolName = tc.function?.name || tc.name;
          if (!toolName) {
            this.logger.error(`[LLM Stream] Tool name is undefined. Full tool call: ${JSON.stringify(tc)}`);
            continue;
          }

          let toolArgs = tc.function?.arguments || tc.args || tc.arguments;
          let toolCallId = tc.id || tc.tool_call_id;
          
          if (!toolCallId) {
            toolCallId = `call_${Date.now()}_${validToolCalls.length}_${Math.random().toString(36).substr(2, 9)}`;
            tc.id = toolCallId;
          }

          let toolCallArgs = {};
          if (typeof toolArgs === 'string' && toolArgs.trim()) {
            try {
              toolCallArgs = JSON.parse(toolArgs);
            } catch (e) {
              toolCallArgs = {};
            }
          } else if (toolArgs && typeof toolArgs === 'object') {
            toolCallArgs = toolArgs;
          }

          const hasValidArgs = Object.keys(toolCallArgs).length > 0;
          const toolsWithoutArgs = ['list_tables'];
          const canHaveEmptyArgs = toolsWithoutArgs.includes(toolName);
          
          if (!hasValidArgs && !canHaveEmptyArgs) {
            this.logger.warn(`[LLM Stream] Skipping tool call ${toolCallId} (${toolName}) - no valid arguments`);
            continue;
          }

          let parsedToolArgs = toolCallArgs;
          if (typeof toolArgs === 'string') {
            try {
              parsedToolArgs = JSON.parse(toolArgs);
            } catch (e) {
              parsedToolArgs = toolCallArgs;
            }
          }

          if (!streamedToolCallIds.has(toolCallId)) {
            streamedToolCallIds.add(toolCallId);
            onEvent({
              type: 'tool_call',
              data: {
                id: toolCallId,
                name: toolName,
                arguments: toolCallArgs,
              },
            });
          }

          validToolCalls.push({
            name: toolName,
            args: parsedToolArgs,
            id: toolCallId,
            type: 'tool_call' as const,
          });

          toolCallIdMap.set(toolCallId, {
            tc,
            toolName,
            toolArgs,
            parsedArgs: parsedToolArgs,
          });
        }

        if (validToolCalls.length === 0) {
          this.reportTokenUsage('stream', aggregateResponse, onEvent);
          return {
            content: fullContent,
            toolCalls: allToolCalls,
            toolResults: allToolResults,
            toolLoops: iterations,
          };
        }

        const aiMessageWithTools = new AIMessage({
          content: currentContent,
          tool_calls: validToolCalls,
        });
        conversationMessages.push(aiMessageWithTools);

        for (const [toolCallId, { tc, toolName, toolArgs, parsedArgs }] of toolCallIdMap) {
          const toolId = toolCallId;

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

            const toolResult = await tool.func(parsedArgs);

            const resultObj = typeof toolResult === 'string' ? JSON.parse(toolResult) : toolResult;

            allToolResults.push({
              toolCallId: toolId,
              result: resultObj,
            });

            const ToolMessage = require('@langchain/core/messages').ToolMessage;
            const summarizedResult = this.summarizeToolResult(toolName, parsedArgs, resultObj);
            conversationMessages.push(
              new ToolMessage({
                content: summarizedResult,
                tool_call_id: toolId,
              }),
            );

            const successIcon = resultObj?.error ? '❌' : '✅';
            const successText = `\n\n${successIcon} ${toolName}\n`;
            fullContent += successText;
            onEvent({
              type: 'text',
              data: {
                delta: successText,
              },
            });
          } catch (error: any) {
            this.logger.error(`Tool failed: ${toolName}`, error);

            const errorResult = { error: error.message || String(error) };
            allToolResults.push({
              toolCallId: toolId,
              result: errorResult,
            });

            const errorIconText = `\n\n❌ ${toolName}\n`;
            fullContent += errorIconText;
            onEvent({
              type: 'text',
              data: {
                delta: errorIconText,
              },
            });

            const ToolMessage = require('@langchain/core/messages').ToolMessage;
            const summarizedError = this.summarizeToolResult(toolName, parsedArgs, errorResult);
            conversationMessages.push(
              new ToolMessage({
                content: summarizedError,
                tool_call_id: toolId,
              }),
            );
          }
        }

        continue;
      }

      const lastToolError = allToolResults.reverse().find((item) => item?.result?.error);
      const fallbackMessage = lastToolError?.result?.message || 'Operation stopped because the model issued too many tool calls without completing.';
      return {
        content: fallbackMessage,
        toolCalls: allToolCalls,
        toolResults: allToolResults,
        toolLoops: iterations,
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

  private async streamChunkedContent(
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

  private getToolCallsFromResponse(response: any): any[] {
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

    this.logger.debug({
      context,
      inputTokens,
      outputTokens,
      totalTokens: inputTokens + outputTokens,
    });
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

  private summarizeToolResult(toolName: string, toolArgs: any, result: any): string {
    const name = toolName || 'unknown_tool';
    const resultStr = JSON.stringify(result || {});
    const resultSize = resultStr.length;

    if (resultSize < 100 && !result?.error) {
      return resultStr;
    }

    if (name === 'get_metadata' || name === 'get_table_details') {
      if (name === 'get_table_details') {
        const tableName = toolArgs?.tableName;
        if (result?.error) {
          const message = typeof result.error === 'string' ? result.error : JSON.stringify(result.error);
          return `[get_table_details] ERROR: ${message.substring(0, 300)}`;
        }
        
        if (Array.isArray(tableName)) {
          const tableCount = tableName.length;
          const tableNames = tableName.slice(0, 3).join(', ');
          const moreInfo = tableCount > 3 ? ` (+${tableCount - 3} more)` : '';
          
          if (result?.tables && Array.isArray(result.tables)) {
            const tablesSummary = result.tables.slice(0, 3).map((t: any) => {
              const relCount = t.relations?.length || 0;
              const colCount = t.columnCount || t.columns?.length || 0;
              return `${t.name}(${colCount} cols, ${relCount} rels)`;
            }).join(', ');
            return `[get_table_details] ${tableCount} table(s): ${tablesSummary}${moreInfo}. Full schema in result.`;
          }
          return `[get_table_details] ${tableCount} table(s): ${tableNames}${moreInfo}. Full schema in result.`;
        }
        
        if (result?.table) {
          const table = result.table;
          const relCount = table.relations?.length || 0;
          const colCount = table.columnCount || table.columns?.length || 0;
          const relations = table.relations?.map((r: any) => `${r.type}:${r.targetTableName || r.targetTable}`).slice(0, 3).join(', ') || 'none';
          const moreRels = relCount > 3 ? ` (+${relCount - 3} more)` : '';
          const requiredCols = table.columns?.filter((c: any) => !c.isNullable && !c.isGenerated && !c.isPrimary).map((c: any) => c.name).slice(0, 5).join(', ') || 'none';
          const moreCols = colCount > 5 ? ` (+${colCount - 5} more columns)` : '';
          return `[get_table_details] ${tableName || 'unknown'}: ${colCount} column(s), ${relCount} relation(s) [${relations}${moreRels}]. Required fields: ${requiredCols}${moreCols}. Full schema in result.`;
        }
        
        return `[get_table_details] ${tableName || 'unknown'}: Schema retrieved. Full details in result.`;
      }
      return `[${name}] Executed. Metadata retrieved. Full details available in result.`;
    }

    if (name === 'update_table') {
      if (result?.error) {
        const message = typeof result.error === 'string' ? result.error : JSON.stringify(result.error);
        return `[update_table] ${toolArgs?.tableName || 'unknown'} -> ERROR: ${message.substring(0, 220)}`;
      }
      const tableName = result?.tableName || toolArgs?.tableName || 'unknown';
      const updated = result?.updated || 'table metadata';
      return `[update_table] ${tableName} -> SUCCESS: Updated ${updated}`;
    }

    if (name === 'check_permission') {
      const table = toolArgs?.table || 'n/a';
      const routePath = toolArgs?.routePath || 'n/a';
      const operation = toolArgs?.operation || 'n/a';
      const allowed = result?.allowed === true ? 'ALLOWED' : 'DENIED';
      const reason = result?.reason || 'unknown_reason';
      const cacheKey = result?.cacheKey ? ` cacheKey=${result.cacheKey}` : '';
      return `[check_permission] table=${table} route=${routePath} operation=${operation} -> ${allowed} (${reason})${cacheKey}`;
    }

    if (name === 'dynamic_repository') {
      const table = toolArgs?.table || 'unknown';
      const operation = toolArgs?.operation || 'unknown';

      if (result?.error) {
        const message = typeof result.error === 'string' ? result.error : JSON.stringify(result.error);
        const userMessage = result?.userMessage || '';
        const suggestion = result?.suggestion || '';
        const fullError = userMessage || message;
        return `[dynamic_repository] ${operation} ${table} -> ERROR: ${fullError.substring(0, 500)}${suggestion ? ` Suggestion: ${suggestion.substring(0, 200)}` : ''}`;
      }

      if (operation === 'batch_delete') {
        const ids = Array.isArray(toolArgs?.ids) ? toolArgs.ids : [];
        const deletedCount = result?.count ?? ids.length;
        return `[dynamic_repository] ${operation} ${table} -> DELETED ${deletedCount} record(s) (ids: ${ids.length})`;
      }

      if (operation === 'find' && Array.isArray(result?.data)) {
        const length = result.data.length;
        if (table === 'table_definition' && length > 0) {
          const allIds = result.data.map((r: any) => r.id).filter((id: any) => id !== undefined);
          const tableNames = result.data.map((r: any) => r.name).filter(Boolean).slice(0, 5);
          const tableIds = allIds.slice(0, 5);
          const namesStr = tableNames.length > 0 ? ` names=[${tableNames.join(', ')}]` : '';
          const idsStr = tableIds.length > 0 ? ` ids=[${tableIds.join(', ')}]` : '';
          const moreInfo = length > 5 ? ` (+${length - 5} more)` : '';
          if (length > 1) {
            return `[dynamic_repository] ${operation} ${table} -> Found ${length} table(s)${namesStr}${idsStr}${moreInfo}. ALL IDs: [${allIds.join(', ')}]. CRITICAL: For table deletion, you MUST delete ONE BY ONE sequentially (not batch_delete) to avoid deadlocks. Delete each table separately: delete id1, then delete id2, etc.`;
          }
          return `[dynamic_repository] ${operation} ${table} -> Found ${length} table(s)${namesStr}${idsStr}${moreInfo}.`;
        }
        if (length > 1) {
          const allIds = result.data.map((r: any) => r.id).filter((id: any) => id !== undefined);
          const ids = allIds.slice(0, 5);
          const idsStr = ids.length > 0 ? ` ids=[${ids.join(', ')}]` : '';
          const moreInfo = length > 5 ? ` (+${length - 5} more)` : '';
          const allIdsStr = allIds.length > 0 ? ` ALL IDs: [${allIds.join(', ')}]` : '';
          return `[dynamic_repository] ${operation} ${table} -> Found ${length} record(s)${idsStr}${moreInfo}.${allIdsStr} CRITICAL: For delete operations on 2+ records, use batch_delete with ALL ${allIds.length} IDs. For create/update on 5+ records, use batch_create/batch_update. Process ALL ${length} records, not just one.`;
        }
      }

      const metaParts: string[] = [];
      if (result?.success !== undefined) {
        metaParts.push(`success=${result.success}`);
      }
      if (result?.count !== undefined) {
        metaParts.push(`count=${result.count}`);
      }
      if (result?.total !== undefined) {
        metaParts.push(`total=${result.total}`);
      }

      let dataInfo = '';
      if (operation === 'create' || operation === 'update') {
        if (Array.isArray(result?.data)) {
          const length = result.data.length;
          if (length > 0) {
            const essentialFields = result.data.map((r: any) => {
              const essential: any = {};
              if (r.id !== undefined) essential.id = r.id;
              if (r.name !== undefined) essential.name = r.name;
              if (r.email !== undefined) essential.email = r.email;
              if (r.title !== undefined) essential.title = r.title;
              return essential;
            }).slice(0, 2);
            dataInfo = ` dataCount=${length} essentialFields=${JSON.stringify(essentialFields).substring(0, 120)}`;
          } else {
            dataInfo = ' dataCount=0';
          }
        } else if (result?.data) {
          const essential: any = {};
          if (result.data.id !== undefined) essential.id = result.data.id;
          if (result.data.name !== undefined) essential.name = result.data.name;
          if (result.data.email !== undefined) essential.email = result.data.email;
          if (result.data.title !== undefined) essential.title = result.data.title;
          dataInfo = ` essentialFields=${JSON.stringify(essential).substring(0, 120)}`;
        }
      } else {
        if (Array.isArray(result?.data)) {
          const length = result.data.length;
          if (length > 0) {
            const sample = result.data.slice(0, 2);
            dataInfo = ` dataCount=${length} sample=${JSON.stringify(sample).substring(0, 160)}`;
          } else {
            dataInfo = ' dataCount=0';
          }
        } else if (result?.data) {
          dataInfo = ` data=${JSON.stringify(result.data).substring(0, 160)}`;
        }
      }

      const metaInfo = metaParts.length > 0 ? ` ${metaParts.join(' ')}` : '';
      return `[dynamic_repository] ${operation} ${table}${metaInfo}${dataInfo}`;
    }

    if (name === 'get_hint') {
      const category = toolArgs?.category || 'all';
      const hintsCount = Array.isArray(result?.hints) ? result.hints.length : 0;
      const titles =
        Array.isArray(result?.hints) && result.hints.length > 0
          ? result.hints
              .slice(0, 2)
              .map((h: any) => h?.title)
              .filter(Boolean)
              .join(', ')
          : '';
      const titleInfo = titles ? ` sampleTitles=[${titles.substring(0, 120)}]` : '';
      return `[get_hint] category=${category} -> ${hintsCount} hint(s)${titleInfo}`;
    }

    if (name === 'get_fields') {
      const table = toolArgs?.tableName || 'unknown';
      const fields = Array.isArray(result?.fields) ? result.fields : [];
      const sample = fields.slice(0, 5).join(', ');
      return `[get_fields] table=${table} -> ${fields.length} field(s) sample=[${sample}]`;
    }

    if (name === 'list_tables') {
      const tables = Array.isArray(result?.tables) ? result.tables : [];
      const sample = tables.slice(0, 5).map((t: any) => t?.name || t).join(', ');
      return `[list_tables] -> ${tables.length} table(s) sample=[${sample}]`;
    }

    const summary = JSON.stringify(result).substring(0, 200);
    return `[${name}] ${summary}${resultStr.length > 200 ? '...' : ''}`;
  }
}
