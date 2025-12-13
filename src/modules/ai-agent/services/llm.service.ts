
import { Injectable, Logger, BadRequestException, HttpException, HttpStatus } from '@nestjs/common';
const { HumanMessage, AIMessage, SystemMessage } = require('@langchain/core/messages');

import { AiConfigCacheService } from '../../../infrastructure/cache/services/ai-config-cache.service';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { IToolCall, IToolResult } from '../interfaces/message.interface';
import { createLLMContext } from '../utils/context.helper';
import { StreamEvent } from '../interfaces/stream-event.interface';
import { LLMMessage, LLMResponse } from '../utils/types';
import { LLMProviderService } from './llm-provider.service';
import { LLMToolFactoryService } from './llm-tool-factory.service';
import { convertToLangChainMessages } from '../utils/message-converter.helper';
import { reduceContentToString, streamChunkedContent, getToolCallsFromResponse } from '../utils/llm-response.helper';
import { reportTokenUsage, extractTokenUsage } from '../utils/token-usage.helper';
import { evaluateNeedsTools, EvaluateNeedsToolsParams } from '../utils/evaluate-needs-tools.helper';
import { aggregateToolCallsFromChunks, deduplicateToolCalls, parseRedactedToolCalls } from '../utils/stream-tool-aggregator.helper';
import { processStreamContentDelta, processTokenUsage, processNonStreamingContent } from '../utils/stream-content-processor.helper';
import { parseToolArguments, normalizeToolCallId, extractToolCallName, createToolCallCacheKey, parseToolArgsWithFallback } from '../utils/tool-call-parser.helper';
import { validateToolCallArguments, formatToolArgumentsForExecution } from '../utils/tool-call-validator.helper';

@Injectable()
export class LLMService {
  private readonly logger = new Logger(LLMService.name);

  constructor(
    private readonly aiConfigCacheService: AiConfigCacheService,
    private readonly queryBuilder: QueryBuilderService,
    private readonly llmProviderService: LLMProviderService,
    private readonly llmToolFactoryService: LLMToolFactoryService,
  ) {}


  async evaluateNeedsTools(params: {
    userMessage: string;
    configId: string | number;
    conversationHistory?: any[];
    conversationSummary?: string;
  }): Promise<{ toolNames: string[]; categories?: string[] }> {
    const { userMessage, configId, conversationHistory = [], conversationSummary } = params;

    const config = await this.aiConfigCacheService.getConfigById(configId);
    if (!config || !config.isEnabled) {
      return { toolNames: [] };
    }

    try {
      const llm = await this.llmProviderService.createLLM(config);
      const evaluateParams: EvaluateNeedsToolsParams = {
        userMessage,
        configId,
        conversationHistory,
        conversationSummary,
        config,
        llm,
        queryBuilder: this.queryBuilder,
      };
      return await evaluateNeedsTools(evaluateParams);
    } catch (error) {
      this.logger.error(`Error in evaluateNeedsTools: ${error instanceof Error ? error.message : String(error)}`);
      return { toolNames: [] };
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
      const llm = await this.llmProviderService.createLLM(config);
      
      const context = createLLMContext(user, conversationId);
      

      let currentSelectedToolNames = selectedToolNames ? [...selectedToolNames] : [];
      const toolDefFile = require('../utils/llm-tools.helper');
      const COMMON_TOOLS = toolDefFile.COMMON_TOOLS || [];
      const availableToolNames = COMMON_TOOLS.map((t: any) => t.name);
      
      let tools = this.llmToolFactoryService.createTools(context, abortSignal, currentSelectedToolNames);
      let llmWithTools = (llm as any).bindTools(tools);
      const provider = config.provider;
      const canStream = typeof llmWithTools.stream === 'function';

      let conversationMessages = convertToLangChainMessages(messages);

      let fullContent = '';
      const allToolCalls: IToolCall[] = [];
      const allToolResults: IToolResult[] = [];

      const executedToolCalls = new Map<string, { toolId: string; result: any }>();
      let iterations = 0;
      const maxIterations = config.maxToolIterations || 50;
      let accumulatedTokenUsage: { inputTokens: number; outputTokens: number } = { inputTokens: 0, outputTokens: 0 };
      const cacheKey = conversationId ? `conv_${conversationId}` : undefined;

      const failedToolCalls = new Map<string, number>();

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

            for await (const chunk of stream) {
              if (abortSignal?.aborted) {
                throw new Error('Request aborted by client');
              }

              allChunks.push(chunk);
              aggregateResponse = chunk;

              accumulatedTokenUsage = processTokenUsage(chunk, accumulatedTokenUsage, onEvent);

              const chunkToolCalls = getToolCallsFromResponse(chunk);
              if (chunkToolCalls.length > 0) {
                for (const tc of chunkToolCalls) {
                  const streamKey = tc.id || `index_${tc.index ?? allChunks.length}`;
                  if (tc.function?.name && tc.id && !streamedToolCallIds.has(streamKey)) {
                    streamedToolCallIds.add(streamKey);
                    const toolCallArgs = parseToolArguments(tc.function?.arguments || tc.args || '');
                    onEvent({
                      type: 'tool_call',
                      data: {
                        id: tc.id,
                        name: tc.function?.name,
                        arguments: toolCallArgs,
                        status: 'pending',
                      },
                    });
                  }
                }
              }

              if (chunk.content) {
                const contentResult = processStreamContentDelta(
                  chunk,
                  iterations,
                  fullContent,
                  currentContent,
                  onEvent,
                );
                currentContent = contentResult.newCurrentContent;
                fullContent = contentResult.newFullContent;
              }
            }

            const aggregatedToolCalls = aggregateToolCallsFromChunks(allChunks);
            if (aggregatedToolCalls.size > 0) {
              currentToolCalls = deduplicateToolCalls(aggregatedToolCalls);
            } else if (allChunks.length > 0) {
              for (let i = allChunks.length - 1; i >= 0; i--) {
                const chunk = allChunks[i];
                const toolCalls = getToolCallsFromResponse(chunk);
                if (toolCalls.length > 0) {
                  currentToolCalls = toolCalls;
                  break;
                }
              }
            }

            if (currentToolCalls.length === 0 && fullContent && (fullContent.includes('redacted_tool_calls_begin') || fullContent.includes('<|redacted_tool_call'))) {
              const parsedToolCalls = parseRedactedToolCalls(fullContent);
              if (parsedToolCalls.length > 0) {
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
                fullContent = fullContent.replace(/<\|redacted_tool_calls_begin\|>.*?<\|redacted_tool_calls_end\|>/gs, '').replace(/<\|redacted_tool_call_begin\|>.*?<\|redacted_tool_call_end\|>/g, '');
                currentContent = fullContent;
              }
            }
          } else {
            aggregateResponse = await llmWithTools.invoke(conversationMessages);
            const contentResult = await processNonStreamingContent(
              aggregateResponse,
              iterations,
              fullContent,
              currentContent,
              accumulatedTokenUsage,
              abortSignal,
              onEvent,
            );
            currentContent = contentResult.newCurrentContent;
            fullContent = contentResult.newFullContent;
            accumulatedTokenUsage = contentResult.newTokenUsage;
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
              const finalUsage = extractTokenUsage(aggregateResponse);
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
              
              reportTokenUsage('stream', aggregateResponse);
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

        if (currentToolCalls.length > 0) {
          const latestUserMessage = (() => {
            for (let i = messages.length - 1; i >= 0; i--) {
              const m = messages[i];
              if ((m as any).role === 'user') {
                return (m as any).content || '';
              }
            }
            return messages?.length ? messages[messages.length - 1]?.content || '' : '';
          })();
          const lowerMsg = typeof latestUserMessage === 'string' ? latestUserMessage.toLowerCase() : '';

          const intentShift = /stop|cancel|pause|hold|đừng|dừng|hủy|huỷ|đổi ý|đổi sang|chuyển|yêu cầu khác|task khác|làm việc khác|change request|switch task|new request/.test(lowerMsg);
          const isDestructiveTool = (name: string | undefined) =>
            name === 'delete_tables' || name === 'delete_records' || name === 'update_records';

          const needsConfirm = (tc: any) => {
            const toolName = extractToolCallName(tc);
            if (!isDestructiveTool(toolName)) return false;
            const argsRaw = tc.function?.arguments || tc.args || tc.arguments || {};
            let parsed: any = argsRaw;
            if (typeof argsRaw === 'string') {
              try { parsed = JSON.parse(argsRaw); } catch { parsed = {}; }
            }
            const ids = parsed?.ids;
            const updates = parsed?.updates;
            const bulkCount = Array.isArray(ids) ? ids.length : Array.isArray(updates) ? updates.length : 0;
            const hasExplicitDeleteKeyword = /delete|remove|xoá|xóa|destroy|drop|clear/.test(lowerMsg);
            const hasExplicitConfirm = /yes|đồng ý|ok|confirm|sure|go ahead|continue/.test(lowerMsg);
            if (!hasExplicitDeleteKeyword && !hasExplicitConfirm) return true;
            if (bulkCount === 0) return true;
            return false;
          };

          if (intentShift) {
            const clarification = `Phát hiện bạn muốn đổi yêu cầu. Đã tạm dừng tác vụ hiện tại. Bạn muốn tiếp tục yêu cầu mới hay tiếp tục tác vụ cũ? Trả lời "tiếp tục yêu cầu mới" hoặc "tiếp tục tác vụ cũ".`;
            fullContent += (fullContent.endsWith('\n') ? '' : '\n\n') + clarification;
            onEvent({
              type: 'text',
              data: {
                delta: (fullContent.endsWith('\n') ? '' : '\n\n') + clarification,
              },
            });

            const finalUsage = extractTokenUsage(aggregateResponse);
            if (finalUsage) {
              accumulatedTokenUsage.inputTokens = Math.max(accumulatedTokenUsage.inputTokens, finalUsage.inputTokens ?? 0);
              accumulatedTokenUsage.outputTokens = Math.max(accumulatedTokenUsage.outputTokens, finalUsage.outputTokens ?? 0);
              onEvent({
                type: 'tokens',
                data: {
                  inputTokens: accumulatedTokenUsage.inputTokens,
                  outputTokens: accumulatedTokenUsage.outputTokens,
                },
              });
            }

            reportTokenUsage('stream', aggregateResponse);
            return {
              content: fullContent,
              toolCalls: allToolCalls,
              toolResults: allToolResults,
              toolLoops: iterations,
            };
          }

          const pendingDestructive = currentToolCalls.find((tc: any) => needsConfirm(tc));
          if (pendingDestructive) {
            const toolName = extractToolCallName(pendingDestructive) || 'destructive action';
            const argsRaw = pendingDestructive.function?.arguments || pendingDestructive.args || pendingDestructive.arguments || {};
            let parsed: any = argsRaw;
            if (typeof argsRaw === 'string') {
              try { parsed = JSON.parse(argsRaw); } catch { parsed = {}; }
            }
            const ids = parsed?.ids;
            const updates = parsed?.updates;
            const targetTable = parsed?.table || parsed?.tableName || parsed?.tableNames;
            const count = Array.isArray(ids) ? ids.length : Array.isArray(updates) ? updates.length : undefined;
            const scopeDesc = [
              targetTable ? `table: ${JSON.stringify(targetTable)}` : null,
              count ? `items: ${count}` : null,
              ids && Array.isArray(ids) ? `ids: [${ids.slice(0, 5).join(', ')}${ids.length > 5 ? '...' : ''}]` : null,
            ].filter(Boolean).join(', ');

            const confirmText = scopeDesc ? `${toolName} → ${scopeDesc}` : toolName;

            const clarification = `Xác nhận thao tác phá hủy: ${confirmText}. Bạn có chắc muốn thực hiện không? Trả lời "yes" để tiếp tục hoặc cung cấp rõ phạm vi/ids.`;
            fullContent += (fullContent.endsWith('\n') ? '' : '\n\n') + clarification;
            onEvent({
              type: 'text',
              data: {
                delta: (fullContent.endsWith('\n') ? '' : '\n\n') + clarification,
              },
            });

            const finalUsage = extractTokenUsage(aggregateResponse);
            if (finalUsage) {
              accumulatedTokenUsage.inputTokens = Math.max(accumulatedTokenUsage.inputTokens, finalUsage.inputTokens ?? 0);
              accumulatedTokenUsage.outputTokens = Math.max(accumulatedTokenUsage.outputTokens, finalUsage.outputTokens ?? 0);
              onEvent({
                type: 'tokens',
                data: {
                  inputTokens: accumulatedTokenUsage.inputTokens,
                  outputTokens: accumulatedTokenUsage.outputTokens,
                },
              });
            }

            reportTokenUsage('stream', aggregateResponse);
            return {
              content: fullContent,
              toolCalls: allToolCalls,
              toolResults: allToolResults,
              toolLoops: iterations,
            };
          }
        }

        if (aggregateResponse && currentToolCalls.length === 0) {
          const toolCalls = getToolCallsFromResponse(aggregateResponse);
          if (toolCalls.length > 0) {
            currentToolCalls = toolCalls;
            
            if (!canStream) {
              for (const tc of toolCalls) {
                const toolName = extractToolCallName(tc);
                if (!tc.id) {
                  tc.id = normalizeToolCallId(tc);
                }
                const toolId = tc.id;
                const toolCallArgs = parseToolArguments(tc.function?.arguments || tc.args || tc.arguments || '');
                
                if (toolName && toolId && !streamedToolCallIds.has(toolId)) {
                  streamedToolCallIds.add(toolId);
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
          const finalUsage = extractTokenUsage(aggregateResponse);
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

            const finalContent = reduceContentToString(aggregateResponse?.content) || '';
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
                  await streamChunkedContent(newContent, abortSignal, (chunk) => {
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
          
          reportTokenUsage('stream', aggregateResponse);
          return {
            content: fullContent,
            toolCalls: allToolCalls,
            toolResults: allToolResults,
            toolLoops: iterations,
          };
        }

        const iterationUsage = extractTokenUsage(aggregateResponse);
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

          let toolCallId = normalizeToolCallId(tc, validToolCalls.length);
          
          if (!tc.id) {
            tc.id = toolCallId;
          }

          const toolCallArgs = parseToolArguments(toolArgs);
          
          if (!validateToolCallArguments(toolName, toolCallArgs)) {
            continue;
          }

          const parsedToolArgs = parseToolArgsWithFallback(toolArgs, toolCallArgs);



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
          reportTokenUsage('stream', aggregateResponse, onEvent);
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
          const toolCallKey = createToolCallCacheKey(toolName, parsedArgs);
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

          const finalArgs = formatToolArgumentsForExecution(toolArgs, parsedArgs);

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
                tools = this.llmToolFactoryService.createTools(context, abortSignal, currentSelectedToolNames);
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

                const availableToolsList = tools.map((t: any) => t.name);
                this.logger.warn('[LLMService-chatStream] tool not available for conversation', {
                  provider: config.provider,
                  conversationId,
                  requestedTool: toolName,
                  availableTools: availableToolsList,
                  selectedToolNames: currentSelectedToolNames,
                });
                this.logger.warn('[LLMService-chatStream] tool not in current tool list', {
                  provider: config.provider,
                  conversationId,
                  requestedTool: toolName,
                  availableTools: availableToolsList,
                  selectedToolNames: currentSelectedToolNames,
                });
                const errorResult = {
                  error: true,
                  errorCode: 'TOOL_NOT_FOUND',
                  message: `Tool "${toolName}" is not available in this conversation. Available tools: ${availableToolsList.join(', ') || 'none'}. Please use one of the available tools instead.`,
                  suggestion: `Use one of these available tools: ${availableToolsList.join(', ') || 'none'}. If you need ${toolName}, it was not selected for this conversation turn.`,
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

            const toolLogBase = {
              layer: 'llm_tool',
              provider: config.provider,
              conversationId: conversationId || null,
              toolCallId: toolId,
              toolName,
            };
            const argsPreview = (() => {
              try {
                return JSON.stringify(parsedArgs).substring(0, 500);
              } catch {
                return String(parsedArgs || '').substring(0, 500);
              }
            })();
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

            // Track failed tool calls to detect infinite loops
            if (resultObj?.error) {
              const failKey = toolCallKey;
              const failCount = (failedToolCalls.get(failKey) || 0) + 1;
              failedToolCalls.set(failKey, failCount);

              // If same tool with same args fails 3 times, stop to prevent infinite loop
              if (failCount >= 3) {
                this.logger.error('[LLMService-chatStream] infinite loop detected', {
                  provider: config.provider,
                  conversationId,
                  toolName,
                  failCount,
                  message: 'Same tool call failed 3 times with same arguments',
                });

                return {
                  content: `Error: Unable to complete the task. The AI agent tried calling "${toolName}" ${failCount} times with the same arguments but it kept failing. Last error: ${resultObj.message || 'Unknown error'}. Please try rephrasing your question or contact support.`,
                  toolCalls: allToolCalls,
                  toolResults: allToolResults,
                  toolLoops: iterations,
                };
              }
            }

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
            this.logger.error('[LLMService-chatStream] tool execution failed', {
              provider: config.provider,
              conversationId,
              toolName,
              toolCallId: toolId,
              message: error?.message || String(error),
            });
            const errorPayload = {
              layer: 'llm_tool',
              provider: config.provider,
              conversationId: conversationId || null,
              toolCallId: toolId,
              toolName,
              stage: 'error',
              message: error?.message || String(error),
            };
            this.logger.error(`[LLMTool] ${JSON.stringify(errorPayload)}`);
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
      const llm = await this.llmProviderService.createLLM(config);
      const lcMessages = convertToLangChainMessages(messages);

      const result: any = await (llm as any).invoke(lcMessages);

      reportTokenUsage('chatSimple', result);

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
