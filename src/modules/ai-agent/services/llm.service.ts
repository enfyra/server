
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
import { buildEvaluateNeedsToolsPrompt } from '../prompts/prompt-builder';

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
        streaming: true,
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

    if (!selectedToolNames || selectedToolNames.length === 0) {
      return [];
    }

    const toolsToCreate = COMMON_TOOLS.filter((tool: any) => selectedToolNames.includes(tool.name));

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
    const seenToolCallIds = new Set<string>();

    for (let i = 0; i < messages.length; i++) {
      const msg = messages[i];
      
      if (msg.role === 'system') {
        result.push(new SystemMessage(msg.content || ''));
      } else if (msg.role === 'user') {
        result.push(new HumanMessage(msg.content || ''));
      } else if (msg.role === 'assistant') {
        let toolCallsFormatted = undefined;

        if (msg.tool_calls && msg.tool_calls.length > 0) {
          
          toolCallsFormatted = msg.tool_calls.map((tc: any, tcIndex: number) => {
            const toolName = tc.function?.name || tc.name;
            let toolArgs = tc.function?.arguments || tc.arguments || tc.input || tc.args;
            
            
            if (typeof toolArgs === 'string') {
              try {
                if (toolArgs.length > 0 && !toolArgs.trim().endsWith('}') && !toolArgs.trim().endsWith(']')) {
                  this.logger.error(`[convertToLangChainMessages] Tool args string appears truncated: length=${toolArgs.length}, last 100 chars: ${toolArgs.substring(Math.max(0, toolArgs.length - 100))}`);
                }
                toolArgs = JSON.parse(toolArgs);
              } catch (e) {
                this.logger.error(`[convertToLangChainMessages] Failed to parse tool args for ${toolName}: ${e}, argsLength=${toolArgs?.length || 0}, first 500 chars: ${toolArgs?.substring(0, 500)}, last 100 chars: ${toolArgs?.substring(Math.max(0, (toolArgs?.length || 0) - 100))}`);
                toolArgs = {};
              }
            }

            const formatted = {
              name: toolName,
              args: toolArgs || {},
              id: tc.id,
              type: 'tool_call' as const,
            };
            
            
            return formatted;
          });
        }

        const aiMsg = new AIMessage({
          content: msg.content || '',
          tool_calls: toolCallsFormatted || [],
        });
        
        
        result.push(aiMsg);
      } else if (msg.role === 'tool') {
        const toolCallId = msg.tool_call_id;
        if (!toolCallId) {
          continue;
        }
        
        if (seenToolCallIds.has(toolCallId)) {
          continue;
        }
        
        let hasMatchingAIMessage = false;
        for (let j = result.length - 1; j >= 0; j--) {
          const prevMsg = result[j];
          if (prevMsg && prevMsg.constructor.name === 'AIMessage' && prevMsg.tool_calls) {
            const hasMatchingToolCall = prevMsg.tool_calls.some((tc: any) => tc.id === toolCallId);
            if (hasMatchingToolCall) {
              hasMatchingAIMessage = true;
              break;
            }
          }
          if (prevMsg && prevMsg.constructor.name === 'HumanMessage') {
            break;
          }
        }
        
        if (!hasMatchingAIMessage) {
          continue;
        }
        
        seenToolCallIds.add(toolCallId);
        const ToolMessage = require('@langchain/core/messages').ToolMessage;
        
        result.push(
          new ToolMessage({
            content: msg.content || '',
            tool_call_id: toolCallId,
          }),
        );
      } else {
      }
    }

    return result;
  }

  async evaluateNeedsTools(params: {
    userMessage: string;
    configId: string | number;
    conversationHistory?: any[];
    conversationSummary?: string;
  }): Promise<{ toolNames: string[]; categories?: string[] }> {
    const { userMessage, configId, conversationHistory = [], conversationSummary } = params;

    const debugInfo: any = {
      userMessage: userMessage.substring(0, 200),
      provider: 'Unknown',
      dbType: 'Unknown',
      finalResult: null,
      errors: [],
    };

    const config = await this.aiConfigCacheService.getConfigById(configId);
    if (!config || !config.isEnabled) {
      debugInfo.errors.push('Config not found or disabled');
      return { toolNames: [] };
    }
    
    const provider = config.provider || 'Unknown';
    debugInfo.provider = provider;

    const userMessageLower = userMessage.toLowerCase().trim();
    const isGreeting = /^(xin chào|hello|hi|hey|chào|greetings|good (morning|afternoon|evening)|how are you|how do you do|what's up|sup)$/i.test(userMessageLower);
    const isCapabilityQuestion = /^(bạn làm|what can|can you|what do you|what are you|capabilities|abilities|help|giúp gì|bạn giúp)/i.test(userMessageLower);
    const isCasual = userMessageLower.length < 20 && !/[a-z]{3,}/i.test(userMessageLower.replace(/[^a-z]/gi, ''));
    
    if (isGreeting || isCapabilityQuestion || isCasual) {
      debugInfo.earlyExit = true;
      debugInfo.reason = isGreeting ? 'greeting' : (isCapabilityQuestion ? 'capability_question' : 'casual_message');
      debugInfo.finalResult = { toolNames: [], categories: [] };
      debugInfo.tokenUsage = { inputTokens: 0, outputTokens: 0 };
      return { toolNames: [], categories: [] };
    }

    try {
      const queryBuilder = this.queryBuilder;
      const dbType = queryBuilder.getDbType();
      debugInfo.dbType = dbType;

      const systemPrompt = buildEvaluateNeedsToolsPrompt(provider);

      const llm = await this.createLLM(config);

      const messages: any[] = [
        new SystemMessage(systemPrompt),
      ];

      if (conversationSummary) {
        messages.push(new AIMessage(`[Previous conversation summary]: ${conversationSummary}`));
      }

      if (conversationHistory && conversationHistory.length > 0) {
        for (const msg of conversationHistory) {
          if (msg.role === 'user') {
            messages.push(new HumanMessage(msg.content || ''));
          } else if (msg.role === 'assistant') {
            const content = msg.content || '';
            if (content) {
              messages.push(new AIMessage(content));
            }
          }
        }
      }

      messages.push(new HumanMessage(userMessage));

      const response = await llm.invoke(messages);
      const responseContent = typeof response?.content === 'string' ? response.content : JSON.stringify(response?.content || '');
      
      const tokenUsage = this.extractTokenUsage(response);
      if (tokenUsage) {
        debugInfo.tokenUsage = tokenUsage;
      }

      let selectedCategories: string[] = [];
      try {
        const parsed = JSON.parse(responseContent);
        if (parsed.categories !== undefined) {
          selectedCategories = Array.isArray(parsed.categories) ? parsed.categories : [];
        } else {
          debugInfo.errors.push('Response does not contain categories field');
        }
      } catch (e) {
        debugInfo.errors.push(`Failed to parse JSON: ${responseContent.substring(0, 200)}`);
        debugInfo.responseContent = responseContent.substring(0, 500);
        return { toolNames: [], categories: [] };
      }

      if (selectedCategories.length === 0) {
        debugInfo.finalResult = { toolNames: [], categories: [] };
        return { toolNames: [], categories: [] };
      }

      debugInfo.finalResult = {
        categories: selectedCategories,
        tokenUsage: tokenUsage || undefined,
      };

      return { toolNames: [], categories: selectedCategories };
    } catch (error) {
      debugInfo.errors.push(`ERROR: ${error instanceof Error ? error.message : String(error)}`);
      debugInfo.stackTrace = error instanceof Error ? error.stack : 'N/A';
      return { toolNames: [] };
    }
  }

  async chat(params: {
    messages: LLMMessage[];
    configId: string | number;
    user?: any;
    conversationId?: string | number;
    selectedToolNames?: string[];
  }): Promise<LLMResponse> {
    const { messages, configId, user, conversationId, selectedToolNames = [] } = params;

    const config = await this.aiConfigCacheService.getConfigById(configId);
    if (!config || !config.isEnabled) {
      throw new BadRequestException(`AI config ${configId} not found or disabled`);
    }

    try {
      const llm = await this.createLLM(config);
      
      const context = createLLMContext(user, conversationId);
      const tools = this.createTools(context, undefined, selectedToolNames);
      const llmWithTools = (llm as any).bindTools(tools);

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

        const validToolNames = new Set(selectedToolNames);
        
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

          if (selectedToolNames.length > 0 && !validToolNames.has(toolName)) {
            const ToolMessage = require('@langchain/core/messages').ToolMessage;
            const errorMsg = `Tool "${toolName}" is not available. Available tools: ${selectedToolNames.join(', ')}. You can ONLY call tools that are provided in your system prompt.`;
            this.logger.error(`[LLM Chat] ${errorMsg}`);
            conversationMessages.push(
              new ToolMessage({
                content: JSON.stringify({
                  error: true,
                  errorCode: 'TOOL_NOT_AVAILABLE',
                  message: errorMsg,
                  availableTools: selectedToolNames,
                }),
                tool_call_id: toolId,
              })
            );
            allToolResults.push({
              toolCallId: toolId,
              result: {
                error: true,
                errorCode: 'TOOL_NOT_AVAILABLE',
                message: errorMsg,
                availableTools: selectedToolNames,
              },
            });
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
              parsedArgs = {};
            }

            const toolResult = await tool.func(parsedArgs);

            const resultObj = typeof toolResult === 'string' ? JSON.parse(toolResult) : toolResult;
            allToolResults.push({
              toolCallId: toolId,
              result: resultObj,
            });

            const hasExistingToolMessage = conversationMessages.some(
              (m: any) => m.constructor.name === 'ToolMessage' && m.tool_call_id === toolId
            );
            
            if (!hasExistingToolMessage) {
              const ToolMessage = require('@langchain/core/messages').ToolMessage;
              conversationMessages.push(
                new ToolMessage({
                  content: JSON.stringify(resultObj),
                  tool_call_id: toolId,
                }),
              );
            }
          } catch (error: any) {
            this.logger.error(`Tool execution failed: ${toolName}`, error);

            const errorResult = { error: error.message || String(error) };
            allToolResults.push({
              toolCallId: toolId,
              result: errorResult,
            });

            const hasExistingToolMessage = conversationMessages.some(
              (m: any) => m.constructor.name === 'ToolMessage' && m.tool_call_id === toolId
            );
            
            if (!hasExistingToolMessage) {
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
      
      const context = createLLMContext(user, conversationId);
      

      let currentSelectedToolNames = selectedToolNames ? [...selectedToolNames] : [];
      const toolDefFile = require('../utils/llm-tools.helper');
      const COMMON_TOOLS = toolDefFile.COMMON_TOOLS || [];
      const availableToolNames = COMMON_TOOLS.map((t: any) => t.name);
      
      let tools = this.createTools(context, abortSignal, currentSelectedToolNames);
      let llmWithTools = (llm as any).bindTools(tools);
      const provider = config.provider;
      const canStream = typeof llmWithTools.stream === 'function';

      let conversationMessages = this.convertToLangChainMessages(messages);

      let fullContent = '';
      const allToolCalls: IToolCall[] = [];
      const allToolResults: IToolResult[] = [];

      const executedToolCalls = new Map<string, { toolId: string; result: any }>();
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
                accumulatedTokenUsage.outputTokens = Math.max(prevOutput, chunkUsage.outputTokens ?? 0);
                
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
                    
                    if (!streamedToolCallIds.has(streamKey)) {
                      streamedToolCallIds.add(streamKey);
                      onEvent({
                        type: 'tool_call',
                        data: {
                          id: toolId,
                          name: toolName,
                          arguments: toolCallArgs,
                          status: 'pending',
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


                if (delta) {
                  if (iterations > 1 && fullContent.trim().length > 0 && currentContent.length === 0 && !fullContent.endsWith('\n\n')) {
                    const newlineDelta = '\n\n' + delta;
                    currentContent += newlineDelta;
                    fullContent += newlineDelta;
                    if (!newlineDelta.includes('redacted_tool_calls_begin') && !newlineDelta.includes('<|redacted_tool_call')) {
                      onEvent({
                        type: 'text',
                        data: { delta: newlineDelta },
                      });
                    }
                  } else {
                    currentContent += delta;
                    fullContent += delta;
                    if (delta.length > 0 && !delta.includes('redacted_tool_calls_begin') && !delta.includes('<|redacted_tool_call')) {
                      onEvent({
                        type: 'text',
                        data: { delta },
                      });
                    }
                  }
                }
              }
            }

            if (aggregatedToolCalls.size > 0) {
              const uniqueToolCalls = new Map<string, any>();
              for (const tc of aggregatedToolCalls.values()) {
                const toolId = tc.id;
                if (!toolId) continue;
                
                const existing = uniqueToolCalls.get(toolId);
                if (existing) {
                  const existingArgs = existing.args || existing.function?.arguments || '';
                  const newArgs = tc.args || tc.function?.arguments || '';
                  
                  if (newArgs && newArgs !== '{}' && newArgs.trim() && (!existingArgs || existingArgs === '{}')) {
                    uniqueToolCalls.set(toolId, tc);
                  }
                } else {
                  uniqueToolCalls.set(toolId, tc);
                }
              }
              
              currentToolCalls = Array.from(uniqueToolCalls.values()).map((tc) => {
                const argsString = tc.args || tc.function?.arguments || '';
                const toolName = tc.function?.name || tc.name || 'unknown';
                const toolId = tc.id;
                
                if (argsString && typeof argsString === 'string' && argsString.trim() && argsString !== '{}') {
                  try {
                    const parsed = JSON.parse(argsString);
                    if (Object.keys(parsed).length > 0) {
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
                    }
                  } catch (e) {
                  }
                }
                
                return {
                  ...tc,
                  id: toolId,
                  function: {
                    ...tc.function,
                    name: toolName,
                    arguments: argsString && argsString !== '{}' ? argsString : undefined,
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


            if (currentToolCalls.length === 0 && fullContent && (fullContent.includes('redacted_tool_calls_begin') || fullContent.includes('<|redacted_tool_call'))) {
              
              try {
                const toolCallRegex = /<\|redacted_tool_call_begin\|>([^<]+)<\|redacted_tool_sep\|>([^<]+)<\|redacted_tool_call_end\|>/g;
                const matches = [...fullContent.matchAll(toolCallRegex)];
                
                
                if (matches.length > 0) {
                  
                  const parsedToolCalls = matches.map((match, index) => {
                    
                    const toolName = match[1].trim();
                    let toolArgs = {};
                    
                    try {
                      const argsString = match[2].trim();
                      toolArgs = JSON.parse(argsString);
                    } catch (parseError: any) {
                      this.logger.error(`[LLM Stream] ❌ Failed to parse tool args for ${toolName}: ${parseError.message}`);
                      this.logger.error(`[LLM Stream] ❌ argsString.length=${match[2]?.trim().length}, first 500 chars: ${match[2]?.substring(0, 500)}`);
                      this.logger.error(`[LLM Stream] ❌ last 100 chars: ${match[2]?.substring(Math.max(0, (match[2]?.length || 0) - 100))}`);
                    }
                    
                    const parsed = {
                      id: `call_${Date.now()}_${index}`,
                      name: toolName,
                      args: toolArgs,
                      function: {
                        name: toolName,
                        arguments: JSON.stringify(toolArgs),
                      },
                      type: 'tool_call' as const,
                    };
                    
                    
                    return parsed;
                  });
                  
                  currentToolCalls = parsedToolCalls;
                  
                  for (const tc of parsedToolCalls) {
                    allToolCalls.push({
                      id: tc.id,
                      type: 'function',
                      function: {
                        name: tc.name,
                        arguments: tc.function.arguments,
                      },
                    });
                    
                    onEvent({
                      type: 'tool_call',
                      data: {
                        id: tc.id,
                        name: tc.name,
                        arguments: tc.function.arguments,
                        status: 'pending',
                      },
                    });
                  }
                  

                  const beforeReplace = fullContent;
                  fullContent = fullContent.replace(/<\|redacted_tool_calls_begin\|>.*?<\|redacted_tool_calls_end\|>/gs, '').replace(/<\|redacted_tool_call_begin\|>.*?<\|redacted_tool_call_end\|>/g, '');
                  currentContent = fullContent;
                } else {
                }
              } catch (e: any) {
                this.logger.error(`[LLM Stream] Failed to parse tool calls from fullContent: ${e.message}`);
                this.logger.error(`[LLM Stream] Error stack: ${e.stack}`);
                this.logger.error(`[LLM Stream] fullContent at error: ${JSON.stringify(fullContent)}`);
              }
            }
          } else {
            aggregateResponse = await llmWithTools.invoke(conversationMessages);
            
            const usage = this.extractTokenUsage(aggregateResponse);
            if (usage) {
              accumulatedTokenUsage.inputTokens = Math.max(accumulatedTokenUsage.inputTokens, usage.inputTokens ?? 0);
              accumulatedTokenUsage.outputTokens = Math.max(accumulatedTokenUsage.outputTokens, usage.outputTokens ?? 0);
              
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
              let contentToStream = fullDelta;
              if (iterations > 1 && fullContent.trim().length > 0 && currentContent.length === 0 && !fullContent.endsWith('\n\n')) {
                contentToStream = '\n\n' + fullDelta;
              }
              await this.streamChunkedContent(contentToStream, abortSignal, (chunk) => {
                currentContent += chunk;
                fullContent += chunk;
                onEvent({
                  type: 'text',
                  data: { delta: chunk },
                });
              });
            } else {
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
                accumulatedTokenUsage.outputTokens = Math.max(accumulatedTokenUsage.outputTokens, finalUsage.outputTokens ?? 0);
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
            
            if (!canStream) {
              for (const tc of toolCalls) {
                const toolName = tc.function?.name || tc.name;

                if (!tc.id) {
                  tc.id = `call_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
                }
                const toolId = tc.id;
                const toolArgs = tc.function?.arguments || tc.args || tc.arguments || {};
                
                let toolCallArgs = {};
                if (typeof toolArgs === 'string' && toolArgs.trim() && toolArgs !== '{}') {
                  try {
                    toolCallArgs = JSON.parse(toolArgs);
                  } catch (e) {
                    toolCallArgs = {};
                  }
                } else if (toolArgs && typeof toolArgs === 'object') {
                  toolCallArgs = toolArgs;
                }
                
                const streamKey = toolId;
                if (toolName && toolId && !streamedToolCallIds.has(streamKey)) {
                  streamedToolCallIds.add(streamKey);
                  onEvent({
                    type: 'tool_call',
                    data: {
                      id: toolId,
                      name: toolName,
                      arguments: toolCallArgs,
                      status: 'pending',
                    },
                  });
                }
                
              }
            } else {
              for (const tc of toolCalls) {
                const toolName = tc.function?.name || tc.name;
                const toolId = tc.id || 'no-id';
                const toolArgs = tc.function?.arguments || tc.args || tc.arguments || {};
              }
            }
          } else if (!canStream) {
            const toolCallsValue = aggregateResponse?.tool_calls;
            const toolCallsType = toolCallsValue ? typeof toolCallsValue : 'undefined';
            const toolCallsIsArray = Array.isArray(toolCallsValue);
            const toolCallsLength = toolCallsIsArray ? toolCallsValue.length : 'N/A';
            if (toolCallsIsArray && toolCallsValue.length === 0) {
            }
          }
        }


        if (fullContent) {
        }


        if (currentToolCalls.length === 0) {
          const finalUsage = this.extractTokenUsage(aggregateResponse);
          if (finalUsage) {
            accumulatedTokenUsage.inputTokens = Math.max(accumulatedTokenUsage.inputTokens, finalUsage.inputTokens ?? 0);
            accumulatedTokenUsage.outputTokens = Math.max(accumulatedTokenUsage.outputTokens, finalUsage.outputTokens ?? 0);
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
          


          if (canStream) {


          } else {

            const finalContent = this.reduceContentToString(aggregateResponse?.content) || '';
            if (finalContent) {
              const previousFullContentLength = fullContent.length - currentContent.length;
              let expectedFullContent = fullContent.substring(0, previousFullContentLength) + finalContent;
              if (expectedFullContent.length > fullContent.length) {
                let newContent = expectedFullContent.substring(fullContent.length);
                if (newContent && iterations > 1 && fullContent.trim().length > 0 && !fullContent.endsWith('\n\n')) {
                  newContent = '\n\n' + newContent;
                  expectedFullContent = fullContent + newContent;
                }
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
            } else if (fullContent.length === 0 && allToolCalls.length === 0) {
              onEvent({
                type: 'text',
                data: {
                  delta: 'I apologize, but I was unable to generate a response. Please try again.',
                },
              });
              fullContent = 'I apologize, but I was unable to generate a response. Please try again.';
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
          accumulatedTokenUsage.outputTokens = Math.max(accumulatedTokenUsage.outputTokens, iterationUsage.outputTokens ?? 0);
          
          onEvent({
            type: 'tokens',
            data: {
              inputTokens: accumulatedTokenUsage.inputTokens,
              outputTokens: accumulatedTokenUsage.outputTokens,
            },
          });
        }

        if (abortSignal?.aborted) {
          throw new Error('Request aborted by client');
        }

        const validToolCalls: any[] = [];
        const toolCallIdMap = new Map<string, any>();
        const validToolNames = new Set(selectedToolNames || []);

        for (const tc of currentToolCalls) {
          if (abortSignal?.aborted) {
            throw new Error('Request aborted by client');
          }
          const toolName = tc.function?.name || tc.name;
          if (!toolName) {
            this.logger.error(`[LLM Stream] Tool name is undefined. Full tool call: ${JSON.stringify(tc)}`);
            continue;
          }

          if (selectedToolNames && selectedToolNames.length > 0 && !validToolNames.has(toolName)) {
            const ToolMessage = require('@langchain/core/messages').ToolMessage;
            const toolCallId = tc.id || `call_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
            const errorMsg = `Tool "${toolName}" is not available. Available tools: ${selectedToolNames.join(', ')}. You can ONLY call tools that are provided in your system prompt.`;
            this.logger.error(`[LLM Stream] ${errorMsg}`);
            
            const errorResult = {
              error: true,
              errorCode: 'TOOL_NOT_AVAILABLE',
              message: errorMsg,
              availableTools: selectedToolNames,
            };
            
            conversationMessages.push(
              new ToolMessage({
                content: JSON.stringify(errorResult),
                tool_call_id: toolCallId,
              })
            );
            
            allToolResults.push({
              toolCallId,
              result: errorResult,
            });
            
            onEvent({
              type: 'tool_call',
              data: {
                id: toolCallId,
                name: toolName,
                arguments: {},
                status: 'error',
              },
            });
            
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



          if (!canStream) {

            if (!streamedToolCallIds.has(toolCallId)) {
            } else {
            }
          } else {

            if (!streamedToolCallIds.has(toolCallId)) {
              streamedToolCallIds.add(toolCallId);
              onEvent({
                type: 'tool_call',
                data: {
                  id: toolCallId,
                  name: toolName,
                  arguments: toolCallArgs,
                  status: 'pending',
                },
              });
            } else {
            }
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



          let normalizedArgs: string;
          
          if (typeof parsedArgs === 'object' && parsedArgs !== null) {
            const sorted = Object.keys(parsedArgs).sort().reduce((acc: any, key) => {
              acc[key] = parsedArgs[key];
              return acc;
            }, {});
            normalizedArgs = JSON.stringify(sorted);
          } else {
            normalizedArgs = String(parsedArgs || '');
          }
          
          const toolCallKey = `${toolName}:${normalizedArgs}`;
          const existingCall = executedToolCalls.get(toolCallKey);
          
          if (existingCall) {
            

            allToolResults.push({
              toolCallId: toolId,
              result: existingCall.result,
            });

            onEvent({
              type: 'tool_call',
              data: {
                id: toolId,
                name: toolName,
                status: existingCall.result?.error ? 'error' : 'success',
              },
            });
            
            const hasExistingToolMessage = conversationMessages.some(
              (m: any) => m.constructor.name === 'ToolMessage' && m.tool_call_id === toolId
            );
            
            if (!hasExistingToolMessage) {
              const ToolMessage = require('@langchain/core/messages').ToolMessage;
              conversationMessages.push(
                new ToolMessage({
                  content: JSON.stringify(existingCall.result),
                  tool_call_id: toolId,
                }),
              );
            }
            
            continue;
          }
          


          

          const alreadyExecutedById = allToolCalls.some((tc) => tc.id === toolId);
          if (alreadyExecutedById) {
            continue;
          }

          let finalArgs: string;
          if (typeof toolArgs === 'object' && toolArgs !== null) {
            finalArgs = JSON.stringify(toolArgs);
          } else if (typeof toolArgs === 'string' && toolArgs.trim() && toolArgs !== '{}') {
            finalArgs = toolArgs;
          } else if (parsedArgs && typeof parsedArgs === 'object' && Object.keys(parsedArgs).length > 0) {
            finalArgs = JSON.stringify(parsedArgs);
          } else {
            finalArgs = JSON.stringify({});
          }

          allToolCalls.push({
            id: toolId,
            type: 'function',
            function: {
              name: toolName,
              arguments: finalArgs,
            },
          });

          try {
            let tool = tools.find((t) => t.name === toolName);
            if (!tool) {

              if (availableToolNames.includes(toolName) && !currentSelectedToolNames.includes(toolName)) {
                currentSelectedToolNames.push(toolName);
                tools = this.createTools(context, abortSignal, currentSelectedToolNames);
                llmWithTools = (llm as any).bindTools(tools);
                tool = tools.find((t) => t.name === toolName);
                
                if (tool) {

                } else {

                  const availableTools = tools.map((t: any) => t.name).join(', ');
                  this.logger.error(`[LLM Stream] Failed to bind tool ${toolName} even after adding to selected tools. Available: ${availableTools || 'none'}`);
                  const errorResult = {
                    error: true,
                    errorCode: 'TOOL_BIND_FAILED',
                    message: `Tool "${toolName}" binding failed. Available tools: ${availableTools || 'none'}.`,
                    suggestion: `Please use one of the available tools: ${availableTools || 'none'}.`,
                  };
                  allToolResults.push({
                    toolCallId: toolId,
                    result: errorResult,
                  });
                  
                  onEvent({
                    type: 'tool_call',
                    data: {
                      id: toolId,
                      name: toolName,
                      status: 'error',
                    },
                  });

                  const hasExistingToolMessage = conversationMessages.some(
                    (m: any) => m.constructor.name === 'ToolMessage' && m.tool_call_id === toolId
                  );
                  
                  if (!hasExistingToolMessage) {
                    const ToolMessage = require('@langchain/core/messages').ToolMessage;
                    conversationMessages.push(
                      new ToolMessage({
                        content: JSON.stringify(errorResult),
                        tool_call_id: toolId,
                      }),
                    );
                  }
                  
                  continue;
                }
              } else {

                const availableTools = tools.map((t: any) => t.name).join(', ');
                const errorResult = {
                  error: true,
                  errorCode: 'TOOL_NOT_FOUND',
                  message: `Tool "${toolName}" is not available in this conversation. Available tools: ${availableTools || 'none'}. Please use one of the available tools instead.`,
                  suggestion: `Use one of these available tools: ${availableTools || 'none'}. If you need ${toolName}, it was not selected for this conversation turn.`,
                };
                allToolResults.push({
                  toolCallId: toolId,
                  result: errorResult,
                });
                
                const hasExistingToolMessage = conversationMessages.some(
                  (m: any) => m.constructor.name === 'ToolMessage' && m.tool_call_id === toolId
                );
                
                if (!hasExistingToolMessage) {
                  const ToolMessage = require('@langchain/core/messages').ToolMessage;
                  conversationMessages.push(
                    new ToolMessage({
                      content: JSON.stringify(errorResult),
                      tool_call_id: toolId,
                    }),
                  );
                }
                
                continue;
              }
            }

            if (abortSignal?.aborted) {
              throw new Error('Request aborted by client');
            }

            const toolResult = await tool.func(parsedArgs);

            if (abortSignal?.aborted) {
              throw new Error('Request aborted by client');
            }

            const resultObj = typeof toolResult === 'string' ? JSON.parse(toolResult) : toolResult;

            executedToolCalls.set(toolCallKey, { toolId, result: resultObj });

            allToolResults.push({
              toolCallId: toolId,
              result: resultObj,
            });

            onEvent({
              type: 'tool_call',
              data: {
                id: toolId,
                name: toolName,
                status: resultObj?.error ? 'error' : 'success',
              },
            });

            const hasExistingToolMessage = conversationMessages.some(
              (m: any) => m.constructor.name === 'ToolMessage' && m.tool_call_id === toolId
            );
            
            if (!hasExistingToolMessage) {
              const ToolMessage = require('@langchain/core/messages').ToolMessage;
              conversationMessages.push(
                new ToolMessage({
                  content: JSON.stringify(resultObj),
                  tool_call_id: toolId,
                }),
              );
            }

          } catch (error: any) {
            this.logger.error(`Tool failed: ${toolName}`, error);

            const errorResult = { error: error.message || String(error) };
            allToolResults.push({
              toolCallId: toolId,
              result: errorResult,
            });

            onEvent({
              type: 'tool_call',
              data: {
                id: toolId,
                name: toolName,
                status: 'error',
              },
            });

            const hasExistingToolMessage = conversationMessages.some(
              (m: any) => m.constructor.name === 'ToolMessage' && m.tool_call_id === toolId
            );
            
            if (!hasExistingToolMessage) {
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
              return this.reduceContentToString(item.content);
            }
            if (item.type === 'text' && item.text) {
              return item.text;
            }
            if (item.type === 'text' && item.content) {
              return this.reduceContentToString(item.content);
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
            if (item.type && item.type !== 'text') {
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
          this.logger.error(`[LLM Stream] Failed to parse tool calls from text: ${e}`);
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


    if (response.invalid_tool_calls && Array.isArray(response.invalid_tool_calls) && response.invalid_tool_calls.length > 0) {
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
              const tableId = t.id ? `(id=${t.id})` : '';
              return `${t.name}${tableId ? ' ' + tableId : ''}(${colCount} cols, ${relCount} rels)`;
            }).join(', ');
            return `[get_table_details] ${tableCount} table(s): ${tablesSummary}${moreInfo}. Schema available in result if needed.`;
        }
          return `[get_table_details] ${tableCount} table(s): ${tableNames}${moreInfo}. Schema available in result if needed.`;
        }
        
        if (result?.table) {
          const table = result.table;
          const relCount = table.relations?.length || 0;
          const colCount = table.columnCount || table.columns?.length || 0;
          const tableId = table.id ? ` (id=${table.id})` : '';
          const relations = table.relations?.map((r: any) => `${r.type}:${r.targetTableName || r.targetTable}`).slice(0, 2).join(', ') || 'none';
          const moreRels = relCount > 2 ? ` (+${relCount - 2} more)` : '';
          const requiredCols = table.columns?.filter((c: any) => !c.isNullable && !c.isGenerated && !c.isPrimary).map((c: any) => c.name).slice(0, 3).join(', ') || 'none';
          const moreCols = colCount > 3 ? ` (+${colCount - 3} more)` : '';
          return `[get_table_details] ${tableName || 'unknown'}${tableId}: ${colCount} col(s), ${relCount} rel(s) [${relations}${moreRels}]. Required: ${requiredCols}${moreCols}. Schema available in result if needed.`;
        }
        
        return `[get_table_details] ${tableName || 'unknown'}: Schema retrieved. Details available in result if needed.`;
      }
      return `[${name}] Executed. Metadata retrieved. Full details available in result.`;
    }

    if (name === 'create_tables') {
      if (result?.error) {
        const message = typeof result.error === 'string' ? result.error : JSON.stringify(result.error);
        return `[create_tables] ${toolArgs?.tables?.[0]?.name || 'unknown'} -> ERROR: ${message.substring(0, 220)}`;
      }
      
      if (result?.failed && result.failed > 0) {
        const succeeded = result.succeeded || 0;
        const failed = result.failed || 0;
        const total = result.total || (succeeded + failed);
        const failedTables = result.results?.filter((r: any) => !r.success).map((r: any) => r.tableName).join(', ') || 'unknown';
        const errorMessages = result.results?.filter((r: any) => !r.success).map((r: any) => `${r.tableName}: ${r.message || r.error}`).join('; ') || 'Unknown error';
        return `[create_tables] PARTIAL SUCCESS: ${succeeded}/${total} succeeded, ${failed} failed. Failed tables: ${failedTables}. Errors: ${errorMessages.substring(0, 300)}`;
      }
      
      const succeeded = result?.succeeded || 0;
      const total = result?.total || (result?.results?.length || 0);
      const tableNames = result?.results?.filter((r: any) => r.success).map((r: any) => `${r.tableName}${r.tableId ? `(id=${r.tableId})` : ''}`).join(', ') || toolArgs?.tables?.map((t: any) => t.name).join(', ') || 'unknown';
      return `[create_tables] SUCCESS: Created ${succeeded}/${total} table(s): ${tableNames}`;
    }

    if (name === 'update_tables') {
      if (result?.error) {
        const message = typeof result.error === 'string' ? result.error : JSON.stringify(result.error);
        return `[update_tables] ${toolArgs?.tables?.[0]?.tableName || 'unknown'} -> ERROR: ${message.substring(0, 220)}`;
      }
      const tableName = result?.tableName || toolArgs?.tableName || 'unknown';
      const tableId = result?.tableId || result?.result?.id || result?.id;
      const updated = result?.updated || 'table metadata';
      const idInfo = tableId ? ` (id=${tableId})` : '';
      return `[update_tables] ${tableName}${idInfo} -> SUCCESS: Updated ${updated}`;
    }


    if (name === 'get_hint') {
      const category = toolArgs?.category || 'all';
      const hints = Array.isArray(result?.hints) ? result.hints : [];
      const hintsCount = hints.length;
      
      if (hintsCount === 0) {
        return `[get_hint] category=${category} -> No hints found`;
      }
      
      const hintsContent = hints.map((h: any) => {
        const title = h?.title || 'Untitled';
        const content = h?.content || '';
        return `## ${title}\n${content}`;
      }).join('\n\n');
      
      return `[get_hint] category=${category} -> ${hintsCount} hint(s)\n\n${hintsContent}`;
    }

    if (name === 'get_fields') {
      const table = toolArgs?.tableName || 'unknown';
      const fields = Array.isArray(result?.fields) ? result.fields : [];
      const sample = fields.slice(0, 5).join(', ');
      return `[get_fields] table=${table} -> ${fields.length} field(s) sample=[${sample}]`;
    }

    const serialized = JSON.stringify(result).substring(0, 200);
    return `[${name}] result=${serialized}${resultStr.length > 200 ? '...' : ''}`;
  }
}
