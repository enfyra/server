import { Injectable, Logger, BadRequestException, OnModuleInit } from '@nestjs/common';
import { Response } from 'express';
import { ConversationService } from './conversation.service';
import { LLMService, LLMMessage } from './llm.service';
import { MetadataCacheService } from '../../../infrastructure/cache/services/metadata-cache.service';
import { AiConfigCacheService } from '../../../infrastructure/cache/services/ai-config-cache.service';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { RedisPubSubService } from '../../../infrastructure/cache/services/redis-pubsub.service';
import { InstanceService } from '../../../shared/services/instance.service';
import { AI_AGENT_CANCEL_CHANNEL } from '../../../shared/utils/constant';
import { AgentRequestDto } from '../dto/agent-request.dto';
import { AgentResponseDto } from '../dto/agent-response.dto';
import { IConversation } from '../interfaces/conversation.interface';
import { IMessage } from '../interfaces/message.interface';
import { StreamEvent } from '../interfaces/stream-event.interface';
import { buildSystemPrompt } from '../prompts/prompt-builder';

@Injectable()
export class AiAgentService implements OnModuleInit {
  private readonly logger = new Logger(AiAgentService.name);
  private activeStreams = new Map<string | number, AbortController>();
  private streamCallbacks = new Map<string | number, { onClose: (eventSource?: string) => Promise<void> }>();

  constructor(
    private readonly conversationService: ConversationService,
    private readonly llmService: LLMService,
    private readonly metadataCacheService: MetadataCacheService,
    private readonly aiConfigCacheService: AiConfigCacheService,
    private readonly queryBuilder: QueryBuilderService,
    private readonly redisPubSubService: RedisPubSubService,
    private readonly instanceService: InstanceService,
  ) {
  }

  async onModuleInit() {
    this.redisPubSubService.subscribeWithHandler(
      AI_AGENT_CANCEL_CHANNEL,
      async (channel: string, message: string) => {
        try {
          const payload = JSON.parse(message);
          const myInstanceId = this.instanceService.getInstanceId();

          if (payload.instanceId === myInstanceId) {
            return;
          }

          if (!payload.conversationId) {
            return;
          }


          await this.handleCancelMessage(payload.conversationId);
        } catch (error) {
          this.logger.error({
            action: 'cancel_message_parse_error',
            error: error instanceof Error ? error.message : String(error),
          });
        }
      },
    );
  }

  private async handleCancelMessage(conversationId: string | number) {
    const abortController = this.activeStreams.get(conversationId);
    const callbacks = this.streamCallbacks.get(conversationId);
    
    if (abortController) {
      
      abortController.abort();
      
      if (callbacks) {
        try {
          await callbacks.onClose('redis.cancel');
        } catch (error) {
          this.logger.error({
            action: 'handleCancelMessage_onClose_error',
            conversationId,
            error: error instanceof Error ? error.message : String(error),
          });
        }
      }
      
      this.activeStreams.delete(conversationId);
      this.streamCallbacks.delete(conversationId);
    } else {
    }
  }

  async processRequest(params: {
    request: AgentRequestDto;
    userId?: string | number;
    user?: any;
  }): Promise<AgentResponseDto> {
    const { request, userId, user } = params;

    let conversation: IConversation;
    let configId: string | number;

    if (request.conversation) {
      conversation = await this.conversationService.getConversation({ id: request.conversation, userId });
      if (!conversation) {
        throw new BadRequestException(`Conversation with ID ${request.conversation} not found`);
      }
      configId = conversation.configId;
    } else {
      if (!request.config) {
        throw new BadRequestException('Config is required when creating a new conversation');
      }
      if (!request.message || !request.message.trim()) {
        throw new BadRequestException('Message cannot be empty');
      }
      const title = this.generateTitleFromMessage(request.message);
      if (!title || !title.trim()) {
        throw new BadRequestException('Failed to generate conversation title');
      }
      conversation = await this.conversationService.createConversation({
        data: {
          title,
          messageCount: 0,
          configId: request.config,
        },
        userId,
      });
      configId = request.config;
    }

    const config = await this.aiConfigCacheService.getConfigById(configId);
    if (!config) {
      throw new BadRequestException(`AI config with ID ${configId} not found`);
    }

    if (!config.isEnabled) {
      throw new BadRequestException(`AI config with ID ${configId} is disabled`);
    }

    const userMessageText = (request.message || '').toLowerCase().trim();
    const isDeleteRequest = /(xóa|delete|drop|remove)\s+(bảng|table|tables)/i.test(userMessageText) || 
                           /(xóa|delete|drop|remove)\s+\w+/i.test(userMessageText);
    const isCreateRequest = /(tạo|create|add)\s+(bảng|table|tables)/i.test(userMessageText);
    const isUpdateRequest = /(cập nhật|update|sửa|modify)\s+(bảng|table|tables)/i.test(userMessageText);

    let detectedTaskType: 'create_tables' | 'update_tables' | 'delete_tables' | 'custom' | null = null;
    if (isDeleteRequest) {
      detectedTaskType = 'delete_tables';
    } else if (isCreateRequest) {
      detectedTaskType = 'create_tables';
    } else if (isUpdateRequest) {
      detectedTaskType = 'update_tables';
    }

    if (conversation.task && 
        (conversation.task.status === 'pending' || conversation.task.status === 'in_progress') &&
        detectedTaskType &&
        detectedTaskType !== conversation.task.type) {
      
      await this.conversationService.updateConversation({
        id: conversation.id,
        data: {
          task: {
            ...conversation.task,
            status: 'cancelled',
            updatedAt: new Date(),
          },
        },
        userId,
      });
      
      conversation = await this.conversationService.getConversation({ id: conversation.id, userId });
    }

    const lastSequence = await this.conversationService.getLastSequence({ conversationId: conversation.id, userId });
    const userSequence = lastSequence + 1;

    const userMessage = await this.conversationService.createMessage({
      data: {
        conversationId: conversation.id,
        role: 'user',
        content: request.message,
        sequence: userSequence,
      },
      userId,
    });

    const fetchLimit = config.maxConversationMessages || 5;
    const allMessagesDesc = await this.conversationService.getMessages({
      conversationId: conversation.id,
      limit: fetchLimit,
      userId,
      sort: '-createdAt',
      since: conversation.lastSummaryAt,
    });
    const allMessages = [...allMessagesDesc].reverse();

    const hasUserMessage =
      allMessages.some((m) => m.sequence === userSequence && m.role === 'user') ||
      allMessages.some((m) => m.id === userMessage.id);
    if (!hasUserMessage) {
      allMessages.push(userMessage);
    }

    const limit = config.maxConversationMessages || 5;
    let messages = allMessages;

    if (messages.length === 0 || messages[messages.length - 1]?.role !== 'user') {
      messages = [...messages, userMessage];
    }

    if (messages.length >= limit) {
      await this.createSummary({ conversationId: conversation.id, configId, userId, triggerMessage: userMessage });
      const refreshed = await this.conversationService.getConversation({ id: conversation.id, userId });
      if (refreshed?.lastSummaryAt) {
        const recentDesc = await this.conversationService.getMessages({
          conversationId: conversation.id,
          limit,
          userId,
          sort: '-createdAt',
          since: refreshed.lastSummaryAt,
        });
        messages = [...recentDesc].reverse();
      }
    }

    const llmMessages = await this.buildLLMMessages({ conversation, messages, config, user, needsTools: true });

    const llmResponse = await this.llmService.chat({ messages: llmMessages, configId, user, conversationId: conversation.id });

    const assistantSequence = lastSequence + 2;
    const summarizedToolResults = llmResponse.toolResults && llmResponse.toolResults.length > 0
      ? this.summarizeToolResults(llmResponse.toolCalls || [], llmResponse.toolResults)
      : null;

    await this.conversationService.createMessage({
      data: {
        conversationId: conversation.id,
        role: 'assistant',
        content: llmResponse.content,
        toolCalls: llmResponse.toolCalls.length > 0 ? llmResponse.toolCalls : null,
        toolResults: summarizedToolResults,
        sequence: assistantSequence,
      },
      userId,
    });

    await this.conversationService.updateMessageCount({ conversationId: conversation.id, userId });

    await this.conversationService.updateConversation({
      id: conversation.id,
      data: {
        lastActivityAt: new Date(),
      },
      userId,
    });

    const updatedConversation = await this.conversationService.getConversation({ id: conversation.id, userId });
    if (!updatedConversation) {
      throw new BadRequestException('Failed to update conversation');
    }

    return {
      conversation: conversation.id,
      response: llmResponse.content || '',
      toolCalls: llmResponse.toolCalls.map((tc) => {
        const result = llmResponse.toolResults.find((tr) => tr.toolCallId === tc.id);
        return {
          id: tc.id,
          name: tc.function.name,
          arguments: JSON.parse(tc.function.arguments),
          result: result?.result,
        };
      }),
    };
  }

  async processRequestStream(params: {
    request: AgentRequestDto;
    req: any;
    res: Response;
    userId?: string | number;
    user?: any;
  }): Promise<void> {
    const { request, req, res, userId, user } = params;

    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache, no-transform');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('X-Accel-Buffering', 'no');
    
    if (typeof res.flushHeaders === 'function') {
      res.flushHeaders();
    }

    if (res.socket && typeof res.socket.setMaxListeners === 'function') {
      res.socket.setMaxListeners(20);
    }

    const abortController = new AbortController();
    let conversationIdForCleanup: string | number | undefined;
    let heartbeatInterval: NodeJS.Timeout;
    let fullContent = '';
    const allToolCalls: any[] = [];
    const allToolResults: any[] = [];
    let hasStartedStreaming = false;

    const cleanup = () => {
      if (heartbeatInterval) {
        clearInterval(heartbeatInterval);
        heartbeatInterval = undefined as any;
      }
      if (conversationIdForCleanup) {
        this.activeStreams.delete(conversationIdForCleanup);
        this.streamCallbacks.delete(conversationIdForCleanup);
      }
    };

    const savePartialMessage = async () => {
      const hasContent = fullContent.trim().length > 0;
      const hasToolCalls = allToolCalls.length > 0;
      const hasToolResults = allToolResults.length > 0;

      if (!hasContent && !hasToolCalls && !hasToolResults) {
        return;
      }

      if (!conversationIdForCleanup) {
        return;
      }

      try {
        const currentLastSequence = await this.conversationService.getLastSequence({
          conversationId: conversationIdForCleanup,
          userId,
        });

        const assistantSequence = currentLastSequence + 2;
        const contentToSave = fullContent.trim() || '(Message cancelled by user)';
        const uniqueToolCalls = allToolCalls.length > 0 ? (() => {
          const seen = new Set<string>();
          return allToolCalls.filter((tc) => {
            const id = tc.id;
            if (!id || seen.has(id)) {
              return false;
            }
            seen.add(id);
            return true;
          });
        })() : null;
        const toolCallsToSave = uniqueToolCalls;
        const toolResultsToSave = allToolResults.length > 0
          ? this.summarizeToolResults(uniqueToolCalls || [], allToolResults)
          : null;

        await this.conversationService.createMessage({
          data: {
            conversationId: conversationIdForCleanup,
            role: 'assistant',
            content: contentToSave,
            toolCalls: toolCallsToSave,
            toolResults: toolResultsToSave,
            sequence: assistantSequence,
          },
          userId,
        });

        await this.conversationService.updateMessageCount({
          conversationId: conversationIdForCleanup,
          userId,
        });

        await this.conversationService.updateConversation({
          id: conversationIdForCleanup,
          data: {
            lastActivityAt: new Date(),
          },
          userId,
        });
      } catch (error) {
        this.logger.error({
          action: 'save_partial_message_error',
          conversationId: conversationIdForCleanup,
          error: error instanceof Error ? error.message : String(error),
        });
      }
    };

    let isClosing = false;
    const onClose = async (eventSource?: string) => {
      if (isClosing || abortController.signal.aborted) {
        return;
      }
      isClosing = true;
      
      abortController.abort();
      
      try {
        await savePartialMessage();
      } catch (error) {
        this.logger.error({
          action: 'onClose_savePartialMessage_error',
          conversationId: conversationIdForCleanup,
          error: error instanceof Error ? error.message : String(error),
        });
      }
      
      cleanup();
    };

    let lastActivityTime = Date.now();
    const STREAM_TIMEOUT_MS = 120000; 

    const sendEvent = (event: StreamEvent) => {
      if (abortController.signal.aborted) {
        return;
      }

      try {
        lastActivityTime = Date.now();
        const data = `data: ${JSON.stringify(event)}\n\n`;
        
        const success = res.write(data);
        
        if (typeof (res as any).flush === 'function') {
          (res as any).flush();
        }
        
        if (!success) {
          onClose('sendEvent.write_failed');
          return;
        }
      } catch (error: any) {
        onClose('sendEvent.write_exception');
        return;
      }
    };


    heartbeatInterval = setInterval(() => {
      if (!abortController.signal.aborted) {
        const elapsed = Date.now() - lastActivityTime;
        if (elapsed < STREAM_TIMEOUT_MS) {
          try {
            res.write(': heartbeat\n\n');
          } catch (e) {
            clearInterval(heartbeatInterval);
          }
        }
      } else {
        clearInterval(heartbeatInterval);
      }
    }, 15000); 

    const sendErrorAndClose = async (errorMessage: string, conversationId?: string | number, lastSequence?: number) => {
      sendEvent({
        type: 'error',
        data: { error: errorMessage },
      });

      if (conversationId && lastSequence !== undefined) {
        try {
          await this.conversationService.createMessage({
            data: {
              conversationId,
              role: 'assistant',
              content: `Error: ${errorMessage}`,
              sequence: lastSequence + 1,
            },
            userId,
          });
          await this.conversationService.updateMessageCount({ conversationId, userId });
        } catch (dbError) {
          this.logger.error('Failed to save error message to database:', dbError);
        }
      }

      cleanup();
      await new Promise(resolve => setTimeout(resolve, 100));
      res.end();
    };

    let conversation: IConversation | undefined;
    let lastSequence: number | undefined;

    let configId: string | number;

    try {
      if (request.conversation) {
        conversation = await this.conversationService.getConversation({ id: request.conversation, userId });
        if (!conversation) {
          await sendErrorAndClose(`Conversation with ID ${request.conversation} not found`);
          return;
        }

        conversationIdForCleanup = conversation.id;
        this.activeStreams.set(conversation.id, abortController);
        this.streamCallbacks.set(conversation.id, { onClose });

        configId = conversation.configId;

      } else {
        if (!request.config) {
          await sendErrorAndClose('Config is required when creating a new conversation');
          return;
        }
        if (!request.message || !request.message.trim()) {
          await sendErrorAndClose('Message cannot be empty');
          return;
        }
        const title = this.generateTitleFromMessage(request.message);
        if (!title || !title.trim()) {
          await sendErrorAndClose('Failed to generate conversation title');
          return;
        }

        conversation = await this.conversationService.createConversation({
          data: {
            title,
            messageCount: 0,
            configId: request.config,
          },
          userId,
        });

        conversationIdForCleanup = conversation.id;
        this.activeStreams.set(conversation.id, abortController);
        this.streamCallbacks.set(conversation.id, { onClose });

        configId = request.config;

      }

      const config = await this.aiConfigCacheService.getConfigById(configId);
      if (!config) {
        await sendErrorAndClose(`AI config with ID ${configId} not found`);
        return;
      }

      if (!config.isEnabled) {
        await sendErrorAndClose(`AI config with ID ${configId} is disabled`);
        return;
      }


      lastSequence = await this.conversationService.getLastSequence({ conversationId: conversation.id, userId });
      const userSequence = lastSequence + 1;

      const userMessage = await this.conversationService.createMessage({
        data: {
          conversationId: conversation.id,
          role: 'user',
          content: request.message,
          sequence: userSequence,
        },
        userId,
      });
      const limit = config.maxConversationMessages || 5;
      const allMessagesDesc = await this.conversationService.getMessages({
        conversationId: conversation.id,
        limit,
        userId,
        sort: '-createdAt',
        since: conversation.lastSummaryAt,
      });
      const allMessages = [...allMessagesDesc].reverse();
      

      const hasUserMessage =
        allMessages.some((m) => m.sequence === userSequence && m.role === 'user') ||
        allMessages.some((m) => m.id === userMessage.id);
      if (!hasUserMessage) {
        allMessages.push(userMessage);
      }

      let messages = allMessages;

      if (messages.length === 0 || messages[messages.length - 1]?.role !== 'user') {
        this.logger.error(`Invalid conversation state: last message is not a user message. Last message role: ${messages[messages.length - 1]?.role}`);
        await sendErrorAndClose('Invalid conversation state: last message must be a user message', conversation.id, userSequence);
        return;
      }

      if (messages.length >= limit) {
        await this.createSummary({ conversationId: conversation.id, configId, userId, triggerMessage: userMessage });
        const refreshed = await this.conversationService.getConversation({ id: conversation.id, userId });
        if (refreshed?.lastSummaryAt) {
          const recentDesc = await this.conversationService.getMessages({
            conversationId: conversation.id,
            limit,
            userId,
            sort: '-createdAt',
            since: refreshed.lastSummaryAt,
          });
          messages = [...recentDesc].reverse();
        }
      }

      const latestUserMessage = messages.filter(m => m.role === 'user').pop()?.content || request.message;
      const userMessageStr = typeof latestUserMessage === 'string' ? latestUserMessage : JSON.stringify(latestUserMessage);

      const evaluateResult = await this.llmService.evaluateNeedsTools({
        userMessage: userMessageStr,
        configId,
        conversationHistory: messages,
        conversationSummary: conversation.summary,
      });
      
      const hintCategories = evaluateResult.categories || [];
      
      let selectedToolNames: string[] = [];
      if (hintCategories && hintCategories.length > 0) {
        const { buildHintContent, getHintTools } = require('../utils/executors/get-hint.executor');
        const dbType = this.queryBuilder.getDbType();
        const idFieldName = dbType === 'mongodb' ? '_id' : 'id';
        
        const hints = buildHintContent(dbType, idFieldName, hintCategories);
        selectedToolNames = getHintTools(hints);
        selectedToolNames = selectedToolNames.filter(tool => tool !== 'get_hint');
        selectedToolNames = Array.from(new Set(selectedToolNames));
      }

      if (selectedToolNames && selectedToolNames.length > 0) {
        const hasFindRecords = selectedToolNames.includes('find_records');
        const hasCreateRecord = selectedToolNames.includes('create_records');
        const hasUpdateRecord = selectedToolNames.includes('update_records');
        const hasGetTableDetails = selectedToolNames.includes('get_table_details');
        


        if ((hasCreateRecord || hasUpdateRecord || selectedToolNames.includes('delete_records')) && !hasFindRecords) {
          selectedToolNames = [...selectedToolNames, 'find_records'];
        }
        


        if ((hasCreateRecord || hasUpdateRecord) && !hasGetTableDetails) {
          selectedToolNames = [...selectedToolNames, 'get_table_details'];
        }
        if (hasFindRecords && !hasGetTableDetails) {
          selectedToolNames = [...selectedToolNames, 'get_table_details'];
        }
      }

      const needsTools = selectedToolNames && selectedToolNames.length > 0;
      const llmMessages = await this.buildLLMMessages({ conversation, messages, config, user, needsTools, hintCategories, selectedToolNames });

      let toolsDefSize = 0;
      if (selectedToolNames && selectedToolNames.length > 0) {
        const toolsDefFile = require('../utils/llm-tools.helper');
        const COMMON_TOOLS = toolsDefFile.COMMON_TOOLS || [];
        const selectedTools = COMMON_TOOLS.filter((tool: any) => selectedToolNames.includes(tool.name));
        
        const formatTools = toolsDefFile.formatToolsForProvider || ((provider: string, tools: any[]) => {
          if (provider === 'Anthropic') {
            return tools.map((tool) => ({
              name: tool.name,
              description: tool.description,
              input_schema: tool.parameters,
            }));
          }
          return tools.map((tool) => ({
            type: 'function' as const,
            function: {
              name: tool.name,
              description: tool.description,
              parameters: tool.parameters,
            },
          }));
        });
        const formattedTools = formatTools(config.provider, selectedTools);
        toolsDefSize = JSON.stringify(formattedTools).length;
      }
      
    let historyCount = 0;
    let toolCallsCount = 0;
    for (const msg of llmMessages) {
      if (msg.role === 'user') {
        historyCount++;
      } else if (msg.role === 'assistant') {
        if (msg.tool_calls) {
          toolCallsCount += msg.tool_calls.length;
        }
        historyCount++;
      }
    }

      let actualInputTokens = 0;
      let actualOutputTokens = 0;

      fullContent = '';
      allToolCalls.length = 0;
      allToolResults.length = 0;
      hasStartedStreaming = false;

      let llmResponse: any = null;
      try {
        llmResponse = await this.llmService.chatStream({
          messages: llmMessages,
          configId,
          abortSignal: abortController.signal,
          user,
          conversationId: conversation.id,
          selectedToolNames,
          onEvent: (event) => {
            if (!hasStartedStreaming) {
              hasStartedStreaming = true;
            }
            
            if (event.type === 'text' && event.data?.delta) {
              const delta = event.data.delta || '';
              fullContent = fullContent + delta;
              
              if (delta.length > 0) {
                sendEvent({
                  type: 'text',
                  data: {
                    delta: delta,
                  },
                });
              }
            } else if (event.type === 'tool_call') {
              const toolCallId = event.data.id;
              if (toolCallId) {
                const alreadyExists = allToolCalls.some((tc) => tc.id === toolCallId);
                if (!alreadyExists) {
                  let parsedArgs = {};
                  try {
                    parsedArgs = typeof event.data.arguments === 'string' 
                      ? JSON.parse(event.data.arguments) 
                      : event.data.arguments || {};
                  } catch (e) {
                    parsedArgs = {};
                  }

                  allToolCalls.push({
                    id: toolCallId,
                    type: 'function',
                    function: {
                      name: event.data.name,
                      arguments: typeof event.data.arguments === 'string' 
                        ? event.data.arguments 
                        : JSON.stringify(event.data.arguments || {}),
                    },
                  });
                }
              }
              sendEvent(event);
            } else if (event.type === 'tool_result') {
              allToolResults.push({
                toolCallId: event.data.toolCallId,
                result: event.data.result,
              });
              sendEvent(event);
            } else if (event.type === 'tokens') {
              actualInputTokens = event.data?.inputTokens || 0;
              actualOutputTokens = event.data?.outputTokens || 0;
              sendEvent(event);
            } else if (event.type === 'error') {
              sendEvent(event);
            }
          },
        });
      } catch (llmError: any) {
        const errorMsg = llmError?.message || String(llmError);
        
        const hasPartialContent = fullContent.trim().length > 0 || allToolCalls.length > 0 || allToolResults.length > 0;
        
        if (hasPartialContent) {
          await savePartialMessage();
        }

        cleanup();

        (async () => {
          try {
            if (conversation && lastSequence !== undefined && !hasPartialContent) {
              const assistantSequence = lastSequence + 2;
              await this.conversationService.createMessage({
                data: {
                  conversationId: conversation.id,
                  role: 'assistant',
                  content: `Error: ${errorMsg}`,
                  sequence: assistantSequence,
                },
                userId,
              });
              await this.conversationService.updateMessageCount({ conversationId: conversation.id, userId });
            }

            if (conversation) {
              sendEvent({
                type: 'done',
                data: {
                  delta: '',
                  metadata: {
                    conversation: conversation.id,
                  },
                },
              });
            }

            try {
              await new Promise(resolve => setTimeout(resolve, 100));
              if (!res.destroyed && !res.writableEnded) {
                res.end();
              }
            } catch (endError) {
            }
          } catch (dbError) {
            this.logger.error('Failed to save error message to database:', dbError);
            try {
              if (!res.destroyed && !res.writableEnded) {
                res.end();
              }
            } catch (endError) {
            }
          }
        })();
        return;
      }

      if (!llmResponse) {
        const hasPartialContent = fullContent.trim().length > 0 || allToolCalls.length > 0 || allToolResults.length > 0;
        if (hasPartialContent) {
          await savePartialMessage();
        }
        cleanup();

        (async () => {
          try {
            if (conversation) {
              sendEvent({
                type: 'done',
                data: {
                  delta: '',
                  metadata: {
                    conversation: conversation.id,
                  },
                },
              });
            }

            try {
              if (!res.destroyed && !res.writableEnded) {
                res.end();
              }
            } catch (endError) {
            }
          } catch (error) {
            this.logger.error(`[Stream] Failed to send conversationId:`, error);
            try {
              if (!res.destroyed && !res.writableEnded) {
                res.end();
              }
            } catch (endError) {
            }
          }
        })();
        return;
      }

      cleanup();

      const summary = {
        conversationId: conversation.id,
        actualInput: actualInputTokens,
        actualOutput: actualOutputTokens,
        historyTurns: historyCount,
        toolCallsCount: toolCallsCount + (llmResponse?.toolCalls?.length || 0),
        toolsDefSize: toolsDefSize,
        selectedToolsCount: selectedToolNames?.length || 0,
        selectedToolNames: selectedToolNames || [],
      };

      (async () => {
        try {
          let contentToSave = llmResponse.content;
          if (typeof contentToSave !== 'string') {
            contentToSave = JSON.stringify(contentToSave);
          }

          const assistantSequence = lastSequence + 2;
          const uniqueToolCalls = allToolCalls.length > 0 ? (() => {
            const seen = new Set<string>();
            return allToolCalls.filter((tc) => {
              const id = tc.id;
              if (!id || seen.has(id)) {
                return false;
              }
              seen.add(id);
              return true;
            });
          })() : (llmResponse.toolCalls || []);
          const toolCallsToSave = uniqueToolCalls;
          const toolResultsToSave = allToolResults.length > 0 ? allToolResults : (llmResponse.toolResults || []);
          const summarizedToolResults = toolResultsToSave.length > 0
            ? this.summarizeToolResults(toolCallsToSave, toolResultsToSave)
            : null;

          const latestUserMessage = messages.filter(m => m.role === 'user').pop()?.content || request.message;
          const userMessageStr = typeof latestUserMessage === 'string' ? latestUserMessage : JSON.stringify(latestUserMessage);
          const provider = config.provider || 'Unknown';

          await this.conversationService.createMessage({
            data: {
              conversationId: conversation.id,
              role: 'assistant',
              content: contentToSave,
              toolCalls: toolCallsToSave.length > 0 ? toolCallsToSave : null,
              toolResults: summarizedToolResults,
              sequence: assistantSequence,
            },
            userId,
            context: {
              userMessage: userMessageStr,
              boundTools: selectedToolNames || [],
              provider: provider,
              tokenUsage: {
                inputTokens: actualInputTokens > 0 ? actualInputTokens : undefined,
                outputTokens: actualOutputTokens > 0 ? actualOutputTokens : undefined,
              },
            },
          });

          await this.conversationService.updateMessageCount({ conversationId: conversation.id, userId });

          await this.conversationService.updateConversation({
            id: conversation.id,
            data: {
              lastActivityAt: new Date(),
            },
            userId,
          });


          sendEvent({
            type: 'done',
            data: {
              delta: '',
              metadata: {
                conversation: conversation.id,
              },
            },
          });

          try {
            if (!res.destroyed && !res.writableEnded) {
              res.end();
            }
          } catch (endError) {
          }
        } catch (error) {
          this.logger.error(`[Stream] Failed to save to DB after streaming response:`, error);
          try {
            if (!res.destroyed && !res.writableEnded) {
              res.end();
            }
          } catch (endError) {
          }
        }
      })();
    } catch (error: any) {
      cleanup();

      const errorMessage = error?.response?.data?.error?.message ||
                          error?.message ||
                          String(error);

      if (errorMessage === 'Request aborted by client') {
        
        const hasPartialContent = fullContent.trim().length > 0 || allToolCalls.length > 0 || allToolResults.length > 0;
        if (hasPartialContent) {
          await savePartialMessage();
        }
        
        cleanup();
        try {
          if (!res.destroyed && !res.writableEnded) {
            res.end();
          }
        } catch (endError) {
        }
        return;
      }

      this.logger.error('Stream error:', error);

      const hasPartialContent = fullContent.trim().length > 0 || allToolCalls.length > 0 || allToolResults.length > 0;
      if (hasPartialContent) {
        await savePartialMessage();
      }

      try {
        sendEvent({
          type: 'error',
          data: {
            error: errorMessage,
            details: error?.response?.data || error?.data,
          },
        });
      } catch (sendError) {
      }

      if (conversation && lastSequence !== undefined && !hasPartialContent) {
        try {
          const assistantSequence = lastSequence + 2;
          await this.conversationService.createMessage({
            data: {
              conversationId: conversation.id,
              role: 'assistant',
              content: `Error: ${errorMessage}`,
              sequence: assistantSequence,
            },
            userId,
          });
          await this.conversationService.updateMessageCount({ conversationId: conversation.id, userId });
        } catch (dbError) {
          this.logger.error('Failed to save error message to database:', dbError);
        }
      }

      cleanup();
      try {
        await new Promise(resolve => setTimeout(resolve, 100));
        if (!res.destroyed && !res.writableEnded) {
          res.end();
        }
      } catch (endError) {
      }
    }
  }

  async cancelStream(params: {
    conversation: string | number | { id: string | number } | null | undefined;
    userId?: string | number;
  }): Promise<{ success: boolean }> {
    const { conversation, userId } = params;

    if (!conversation) {
      return { success: false };
    }

    let conversationId: string | number;
    if (typeof conversation === 'object' && 'id' in conversation) {
      conversationId = conversation.id;
    } else {
      conversationId = conversation;
    }
    

    if (typeof conversationId === 'string' && /^\d+$/.test(conversationId)) {
      conversationId = parseInt(conversationId, 10);
    }

    const abortController = this.activeStreams.get(conversationId);
    if (!abortController) {
      return { success: false };
    }


    const instanceId = this.instanceService.getInstanceId();
    await this.redisPubSubService.publish(AI_AGENT_CANCEL_CHANNEL, {
      instanceId,
      conversationId,
    });

    abortController.abort();
    this.activeStreams.delete(conversationId);

    return { success: true };
  }

  private generateTitleFromMessage(message: string): string {
    const maxLength = 100;
    const trimmed = message.trim();
    if (trimmed.length <= maxLength) {
      return trimmed;
    }
    return trimmed.substring(0, maxLength - 3) + '...';
  }

  private async buildLLMMessages(params: {
    conversation: IConversation;
    messages: IMessage[];
    config: any;
    user?: any;
    needsTools?: boolean;
    hintCategories?: string[];
    selectedToolNames?: string[];
  }): Promise<LLMMessage[]> {
    const { conversation, messages, config, user, needsTools = true, hintCategories, selectedToolNames } = params;


    const latestUserMessage = messages.length > 0
      ? messages[messages.length - 1]?.content
      : undefined;

    const systemPrompt = await this.buildSystemPrompt({ conversation, config, user, latestUserMessage, needsTools, hintCategories, selectedToolNames });
    
    const llmMessages: LLMMessage[] = [];

    if (systemPrompt && systemPrompt.trim().length > 0) {
      llmMessages.push({
        role: 'system',
        content: systemPrompt,
      });
    }

    let lastPushedRole: 'user' | 'assistant' | null = null;
    let skippedCount = 0;
    let pushedCount = 0;
    let toolResultsPushed = 0;

    for (let i = 0; i < messages.length; i++) {
      const message = messages[i];
      try {
        if (message.role === 'user') {
          if (lastPushedRole === 'user') {
            skippedCount++;
            continue;
          }

          let userContent = message.content || '';

          if (typeof userContent === 'string' && userContent.includes('[object Object]')) {
            skippedCount++;
            continue;
          }

          if (typeof userContent === 'object') {
            userContent = JSON.stringify(userContent);
          }

          if (userContent.length > 1000) {
            userContent = userContent.substring(0, 1000) + '... [truncated for token limit]';
          }
          llmMessages.push({
            role: 'user',
            content: userContent,
          });
          pushedCount++;
          lastPushedRole = 'user';
        } else if (message.role === 'assistant') {
          let assistantContent = message.content || null;
          let parsedToolCalls = message.toolCalls || null;

          if (assistantContent && typeof assistantContent === 'string' && assistantContent.includes('[object Object]')) {
            skippedCount++;
            continue;
          }

          if (assistantContent && typeof assistantContent === 'string' &&
              (assistantContent.startsWith('Error:') || assistantContent.includes('BadRequestError') || assistantContent.includes('tool_use_id'))) {
            skippedCount++;
            continue;
          }

          if (!parsedToolCalls && assistantContent && typeof assistantContent === 'string' && 
              (assistantContent.includes('redacted_tool_calls_begin') || assistantContent.includes('<|redacted_tool_call'))) {
            
            try {
              const toolCallRegex = /<\|redacted_tool_call_begin\|>([^<]+)<\|redacted_tool_sep\|>([^<]+)<\|redacted_tool_call_end\|>/g;
              const matches = [...assistantContent.matchAll(toolCallRegex)];
              
              if (matches.length > 0) {
                
                parsedToolCalls = matches.map((match, index) => {
                  const toolName = match[1].trim();
                  let toolArgs = {};
                  try {
                    toolArgs = JSON.parse(match[2].trim());
                  } catch (parseError: any) {
                    this.logger.error(`[buildLLMMessages] Failed to parse tool args for ${toolName}: ${parseError.message}, raw: ${match[2]?.substring(0, 200)}`);
                  }
                  
                  return {
                    id: `call_${message.id}_${index}_${Date.now()}`,
                    type: 'function' as const,
                    function: {
                      name: toolName,
                      arguments: typeof toolArgs === 'string' ? toolArgs : JSON.stringify(toolArgs),
                    },
                  };
                });
                
                
                assistantContent = assistantContent.replace(/<\|redacted_tool_calls_begin\|>.*?<\|redacted_tool_calls_end\|>/gs, '').replace(/<\|redacted_tool_call_begin\|>.*?<\|redacted_tool_call_end\|>/g, '').trim();
                
                if (!assistantContent || assistantContent.length === 0) {
                  assistantContent = null;
                }
                
              } else {
              }
            } catch (parseError: any) {
              this.logger.error(`[buildLLMMessages] Failed to parse corrupt message: ${parseError.message}, stack: ${parseError.stack}`);
            }
          }

          if (message.toolResults && message.toolResults.length > 0) {
            const hasOrphanedResults = message.toolResults.some(
              (tr) => !parsedToolCalls?.find((tc) => tc.id === tr.toolCallId)
            );
            if (hasOrphanedResults) {
              continue;
            }
          }

          if (assistantContent && typeof assistantContent === 'object') {
            assistantContent = JSON.stringify(assistantContent);
          }

          const hasToolCalls = parsedToolCalls && parsedToolCalls.length > 0;



          if (!hasToolCalls && assistantContent) {
            const messageIndex = messages.indexOf(message);
            const isRecentMessage = messageIndex >= messages.length - 4; 

            if (!isRecentMessage) {
              skippedCount++;
              continue;
            }


            if (assistantContent.length > 400) {
              assistantContent = assistantContent.substring(0, 400) + '... [truncated]';
            }
          } else if (hasToolCalls && assistantContent && assistantContent.length > 800) {

            assistantContent = assistantContent.substring(0, 800) + '... [truncated for token limit]';
          }

          const assistantMessage: LLMMessage = {
            role: 'assistant',
            content: assistantContent,
          };

          if (hasToolCalls) {
            
            assistantMessage.tool_calls = parsedToolCalls.map((tc, tcIndex) => {
              
              let args = tc.function.arguments;
              if (typeof args === 'object' && args !== null) {
                args = JSON.stringify(args);
              }
              
              const formatted = {
                id: tc.id,
                type: 'function' as const,
                function: {
                  name: tc.function.name,
                  arguments: args,
                },
              };
              
              
              return formatted;
            });
            
          }

          const assistantPushed = !!(assistantMessage.content || assistantMessage.tool_calls);
          if (assistantPushed) {
            llmMessages.push(assistantMessage);
            pushedCount++;
            lastPushedRole = 'assistant';
          } else {
            skippedCount++;
          }

          if (!assistantPushed && message.toolResults && message.toolResults.length > 0) {
          }

          if (assistantPushed && assistantMessage.tool_calls && assistantMessage.tool_calls.length > 0) {
            const toolCallIds = new Set(assistantMessage.tool_calls.map(tc => tc.id));
            const toolResultsMap = new Map<string, any>();
            
            if (message.toolResults && message.toolResults.length > 0) {
              for (const toolResult of message.toolResults) {
                if (toolCallIds.has(toolResult.toolCallId)) {
                  toolResultsMap.set(toolResult.toolCallId, toolResult);
                } else {
                }
              }
            }

            for (const toolCall of assistantMessage.tool_calls) {
              const toolResult = toolResultsMap.get(toolCall.id);
              if (toolResult) {
                let resultContent: string;
                if (typeof toolResult.result === 'string') {
                  resultContent = toolResult.result;
                } else {
                  resultContent = JSON.stringify(toolResult.result || {});
                }

                llmMessages.push({
                  role: 'tool',
                  content: resultContent,
                  tool_call_id: toolCall.id,
                });
                toolResultsPushed++;
              } else {
                llmMessages.push({
                  role: 'tool',
                  content: JSON.stringify({ error: 'Tool result not found in conversation history' }),
                  tool_call_id: toolCall.id,
                });
                toolResultsPushed++;
              }
            }
          }
        }
      } catch (error) {
        skippedCount++;
        continue;
      }
    }

    return llmMessages;
  }

  private summarizeToolResults(toolCalls: any[], toolResults: any[]): any[] {
    if (!toolResults || toolResults.length === 0) {
      return toolResults || [];
    }



    return toolResults.map((toolResult) => {
      const toolCall = toolCalls?.find((tc) => tc.id === toolResult.toolCallId);
      const toolName = toolCall?.function?.name || '';
      let parsedArgs: any = {};
      if (toolCall?.function?.arguments) {
        try {
          parsedArgs =
            typeof toolCall.function.arguments === 'string'
              ? JSON.parse(toolCall.function.arguments)
              : toolCall.function.arguments;
        } catch {
          parsedArgs = {};
        }
      }


      if (toolName === 'get_table_details') {
        return toolResult;
      }

      const originalResultStr = JSON.stringify(toolResult.result || {});
      const originalResultSize = originalResultStr.length;



      if (originalResultSize < 100 && !toolResult.result?.error) {
        return toolResult;
      }

      const summary = this.formatToolResultSummary(toolName, parsedArgs, toolResult.result);
      return {
        ...toolResult,
        result: summary,
      };
    });
  }

  private formatToolResultSummary(toolName: string, toolArgs: any, result: any): string {
    const name = toolName || 'unknown_tool';

    if (name === 'get_metadata' || name === 'get_table_details') {
      if (name === 'get_table_details') {
        const tableName = toolArgs?.tableName;
        if (Array.isArray(tableName)) {
          const tableCount = tableName.length;
          const tableNames = tableName.slice(0, 3).join(', ');
          const moreInfo = tableCount > 3 ? ` (+${tableCount - 3} more)` : '';
          const resultKeys = result && typeof result === 'object' && !Array.isArray(result) ? Object.keys(result).filter(k => k !== '_errors') : [];
          const loadedCount = resultKeys.length;
          const errors = result?._errors;
          let summary = `[get_table_details] Executed for ${tableCount} table(s): ${tableNames}${moreInfo}. Loaded ${loadedCount} table(s)`;
          if (errors && Array.isArray(errors) && errors.length > 0) {
            summary += `, ${errors.length} error(s): ${errors.slice(0, 2).join('; ')}${errors.length > 2 ? '...' : ''}`;
          }
          summary += '. Schema details omitted to save tokens.';
          return summary;
        }
        return `[get_table_details] Executed for table: ${tableName || 'unknown'}. Schema details omitted to save tokens. Re-run the tool if you need the raw metadata.`;
      }
      return `[${name}] Executed. Schema details omitted to save tokens. Re-run the tool if you need the raw metadata.`;
    }

    if (name === 'update_tables') {
      if (result?.error) {
        const message = typeof result.error === 'string' ? result.error : JSON.stringify(result.error);
        return `[update_tables] ${toolArgs?.tables?.[0]?.tableName || 'unknown'} -> ERROR: ${this.truncateString(message, 220)}`;
      }
      const tableName = result?.tableName || toolArgs?.tables?.[0]?.tableName || 'unknown';
      const updated = result?.updated || 'table metadata';
      return `[update_tables] ${tableName} -> SUCCESS: Updated ${updated}`;
    }


    if (name === 'find_records') {
      const table = toolArgs?.table || 'unknown';
      const operation = 'find';

      if (result?.error) {
        if (result.errorCode === 'PERMISSION_DENIED') {
          const reason = result.reason || result.message || 'unknown';
          return `[${name}] ${table} -> PERMISSION DENIED: You MUST inform the user: "You do not have permission to find records in table ${table}. Reason: ${reason}. Please check your access rights or contact an administrator." Then STOP - do NOT retry this operation or call any other tools.`;
        }
        const message = typeof result.error === 'string' ? result.error : JSON.stringify(result.error);
        const errorMessage = this.truncateString(message, 500);
        const errorCode = result.errorCode ? ` (${result.errorCode})` : '';
        return `[${name}] ${table} -> ERROR${errorCode}: ${errorMessage}`;
      }

      if (Array.isArray(result?.data)) {
        const length = result.data.length;
        if (table === 'table_definition' && length > 0) {
          const allIds = result.data.map((r: any) => r.id).filter((id: any) => id !== undefined);
          const tableNames = result.data.map((r: any) => r.name).filter(Boolean).slice(0, 5);
          const tableIds = allIds.slice(0, 5);
          const namesStr = tableNames.length > 0 ? ` names=[${tableNames.join(', ')}]` : '';
          const idsStr = tableIds.length > 0 ? ` ids=[${tableIds.join(', ')}]` : '';
          const moreInfo = length > 5 ? ` (+${length - 5} more)` : '';
          if (length > 1) {
            return `[${name}] ${table} -> Found ${length} table(s)${namesStr}${idsStr}${moreInfo}. ALL IDs: [${allIds.join(', ')}]. CRITICAL: For table deletion, use delete_tables with ALL IDs in array: delete_tables({"ids":[${allIds.join(',')}]})`;
          }
          return `[${name}] ${table} -> Found ${length} table(s)${namesStr}${idsStr}${moreInfo}.`;
        }
        if (length > 1) {
          const allIds = result.data.map((r: any) => r.id).filter((id: any) => id !== undefined);
          const ids = allIds.slice(0, 5);
          const idsStr = ids.length > 0 ? ` ids=[${ids.join(', ')}]` : '';
          const moreInfo = length > 5 ? ` (+${length - 5} more)` : '';
          const allIdsStr = allIds.length > 0 ? ` ALL IDs: [${allIds.join(', ')}]` : '';
          return `[${name}] ${table} -> Found ${length} record(s)${idsStr}${moreInfo}.${allIdsStr} CRITICAL: For operations on 2+ records, use create_records, update_records, or delete_records with ALL ${allIds.length} IDs. Process ALL ${length} records, not just one.`;
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
      if (result?.totalCount !== undefined) {
        metaParts.push(`totalCount=${result.totalCount}`);
      }
      if (result?.filterCount !== undefined) {
        metaParts.push(`filterCount=${result.filterCount}`);
      }

      let dataInfo = '';
      if (Array.isArray(result?.data)) {
        const length = result.data.length;
        if (length > 0) {
          const sample = result.data.slice(0, 2);
          dataInfo = ` dataCount=${length} sample=${this.truncateString(JSON.stringify(sample), 160)}`;
        } else {
          dataInfo = ' dataCount=0';
        }
      } else if (result?.data) {
        dataInfo = ` data=${this.truncateString(JSON.stringify(result.data), 160)}`;
      }

      const metaInfo = metaParts.length > 0 ? ` ${metaParts.join(' ')}` : '';
      return `[${name}] ${table}${metaInfo}${dataInfo}`;
    }

    if (name === 'count_records') {
      const table = toolArgs?.table || 'unknown';

      if (result?.error) {
        if (result.errorCode === 'PERMISSION_DENIED') {
          const reason = result.reason || result.message || 'unknown';
          return `[count_records] ${table} -> PERMISSION DENIED: You MUST inform the user: "You do not have permission to count records in table ${table}. Reason: ${reason}. Please check your access rights or contact an administrator." Then STOP - do NOT retry this operation or call any other tools.`;
        }
        const message = typeof result.error === 'string' ? result.error : JSON.stringify(result.error);
        const errorMessage = this.truncateString(message, 500);
        const errorCode = result.errorCode ? ` (${result.errorCode})` : '';
        return `[count_records] ${table} -> ERROR${errorCode}: ${errorMessage}`;
      }

      const count = result?.totalCount !== undefined ? result.totalCount : (result?.filterCount !== undefined ? result.filterCount : (result?.count !== undefined ? result.count : 'unknown'));
      return `[count_records] ${table} -> Count: ${count}`;
    }

    if (name === 'create_records') {
      const table = toolArgs?.table || 'unknown';

      if (result?.error) {
        if (result.errorCode === 'PERMISSION_DENIED') {
          const reason = result.reason || result.message || 'unknown';
          return `[create_records] ${table} -> PERMISSION DENIED: You MUST inform the user: "You do not have permission to create records in table ${table}. Reason: ${reason}. Please check your access rights or contact an administrator." Then STOP - do NOT retry this operation or call any other tools.`;
        }
        const message = typeof result.error === 'string' ? result.error : JSON.stringify(result.error);
        const errorMessage = this.truncateString(message, 500);
        const errorCode = result.errorCode ? ` (${result.errorCode})` : '';
        return `[create_records] ${table} -> ERROR${errorCode}: ${errorMessage}`;
      }

      const essential: any = {};
      if (result?.data?.id !== undefined) essential.id = result.data.id;
      if (result?.data?.name !== undefined) essential.name = result.data.name;
      if (result?.data?.email !== undefined) essential.email = result.data.email;
      if (result?.data?.title !== undefined) essential.title = result.data.title;
      const dataInfo = Object.keys(essential).length > 0 ? ` essentialFields=${this.truncateString(JSON.stringify(essential), 120)}` : '';
      return `[create_records] ${table} -> CREATED${dataInfo}`;
    }

    if (name === 'update_records') {
      const table = toolArgs?.table || 'unknown';
      const id = toolArgs?.id || 'unknown';

      if (result?.error) {
        if (result.errorCode === 'PERMISSION_DENIED') {
          const reason = result.reason || result.message || 'unknown';
          return `[update_records] ${table} id=${id} -> PERMISSION DENIED: You MUST inform the user: "You do not have permission to update records in table ${table}. Reason: ${reason}. Please check your access rights or contact an administrator." Then STOP - do NOT retry this operation or call any other tools.`;
        }
        const message = typeof result.error === 'string' ? result.error : JSON.stringify(result.error);
        const errorMessage = this.truncateString(message, 500);
        const errorCode = result.errorCode ? ` (${result.errorCode})` : '';
        return `[update_records] ${table} id=${id} -> ERROR${errorCode}: ${errorMessage}`;
      }

      const essential: any = {};
      if (result?.data?.id !== undefined) essential.id = result.data.id;
      if (result?.data?.name !== undefined) essential.name = result.data.name;
      if (result?.data?.email !== undefined) essential.email = result.data.email;
      if (result?.data?.title !== undefined) essential.title = result.data.title;
      const dataInfo = Object.keys(essential).length > 0 ? ` essentialFields=${this.truncateString(JSON.stringify(essential), 120)}` : '';
      return `[update_records] ${table} id=${id} -> UPDATED${dataInfo}`;
    }

    if (name === 'delete_records') {
      const table = toolArgs?.table || 'unknown';
      const id = toolArgs?.id || 'unknown';

      if (result?.error) {
        if (result.errorCode === 'PERMISSION_DENIED') {
          const reason = result.reason || result.message || 'unknown';
          return `[delete_records] ${table} id=${id} -> PERMISSION DENIED: You MUST inform the user: "You do not have permission to delete records in table ${table}. Reason: ${reason}. Please check your access rights or contact an administrator." Then STOP - do NOT retry this operation or call any other tools.`;
        }
        const message = typeof result.error === 'string' ? result.error : JSON.stringify(result.error);
        const errorMessage = this.truncateString(message, 500);
        const errorCode = result.errorCode ? ` (${result.errorCode})` : '';
        return `[delete_records] ${table} id=${id} -> ERROR${errorCode}: ${errorMessage}`;
      }

      return `[delete_records] ${table} id=${id} -> DELETED`;
    }


    if (name === 'create_records' || name === 'update_records' || name === 'delete_records') {
      const table = toolArgs?.table || 'unknown';
      const operation = name.replace('_records', '');

      if (result?.error) {
        if (result.errorCode === 'PERMISSION_DENIED') {
          const reason = result.reason || result.message || 'unknown';
          return `[${name}] ${table} -> PERMISSION DENIED: You MUST inform the user: "You do not have permission to ${operation} records in table ${table}. Reason: ${reason}. Please check your access rights or contact an administrator." Then STOP - do NOT retry this operation or call any other tools.`;
        }
        const message = typeof result.error === 'string' ? result.error : JSON.stringify(result.error);
        const errorMessage = this.truncateString(message, 500);
        const errorCode = result.errorCode ? ` (${result.errorCode})` : '';
        return `[${name}] ${table} -> ERROR${errorCode}: ${errorMessage}`;
      }

      if (Array.isArray(result)) {
        const length = result.length;
        if (operation === 'batch_create' || operation === 'create') {
          const createdIds = result.map((r: any) => r?.data?.id || r?.id).filter((id: any) => id !== undefined).slice(0, 5);
          const idsStr = createdIds.length > 0 ? ` ids=[${createdIds.join(', ')}]` : '';
          const moreInfo = length > 5 ? ` (+${length - 5} more)` : '';
          return `[${name}] ${table} -> CREATED ${length} record(s)${idsStr}${moreInfo}`;
        }
        if (operation === 'batch_update' || operation === 'update') {
          const updatedIds = result.map((r: any) => r?.data?.id || r?.id).filter((id: any) => id !== undefined).slice(0, 5);
          const idsStr = updatedIds.length > 0 ? ` ids=[${updatedIds.join(', ')}]` : '';
          const moreInfo = length > 5 ? ` (+${length - 5} more)` : '';
          return `[${name}] ${table} -> UPDATED ${length} record(s)${idsStr}${moreInfo}`;
        }
        if (operation === 'batch_delete' || operation === 'delete') {
          const ids = Array.isArray(toolArgs?.ids) ? toolArgs.ids : [];
          const deletedCount = length;
          return `[${name}] ${table} -> DELETED ${deletedCount} record(s) (ids: ${ids.length})`;
        }
      }

      return `[${name}] ${table} -> Completed`;
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

    const serialized = this.truncateString(JSON.stringify(result), 200);
    return `[${name}] result=${serialized}`;
  }

  private truncateString(value: string, maxLength: number): string {
    if (!value) {
      return '';
    }
    if (value.length <= maxLength) {
      return value;
    }
    return `${value.slice(0, maxLength)}...`;
  }

  private async buildSystemPrompt(params: {
    conversation: IConversation;
    config: any;
    user?: any;
    latestUserMessage?: string;
    needsTools?: boolean;
    hintCategories?: string[];
    selectedToolNames?: string[];
  }): Promise<string> {
    const { conversation, user, latestUserMessage, needsTools = true, config, hintCategories, selectedToolNames } = params;
    const provider = config?.provider || 'OpenAI';

    let tablesList: string | undefined;
    const needsTableListForReference = needsTools && selectedToolNames && (
      selectedToolNames.includes('create_tables') ||
      selectedToolNames.includes('update_tables') ||
      selectedToolNames.includes('delete_tables') ||
      (selectedToolNames.includes('find_records') && !hintCategories?.includes('metadata_operations'))
    );
    if (needsTableListForReference) {
      const metadata = await this.metadataCacheService.getMetadata();
      tablesList = Array.from(metadata.tables.keys()).map(name => `- ${name}`).join('\n');
    }

    const dbType = this.queryBuilder.getDbType();
    const idFieldName = dbType === 'mongodb' ? '_id' : 'id';

    let hintContent: string | undefined;
    if (hintCategories && hintCategories.length > 0) {
      const { buildHintContent, getHintContentString } = require('../utils/executors/get-hint.executor');
      const hints = buildHintContent(dbType, idFieldName, hintCategories);
      hintContent = getHintContentString(hints);
    }

    const systemPrompt = buildSystemPrompt({
      provider,
      needsTools,
      tablesList,
      user,
      dbType,
      latestUserMessage,
      conversationSummary: conversation.summary,
      task: conversation.task,
      hintContent,
    });
    
    return systemPrompt;
  }


  private async createSummary(params: {
    conversationId: string | number;
    configId: string | number;
    userId?: string | number;
    triggerMessage?: IMessage;
  }): Promise<void> {
    const { conversationId, configId, userId, triggerMessage } = params;

    const conversation = await this.conversationService.getConversation({ id: conversationId, userId });
    if (!conversation) {
      return;
    }

    const limit = await this.aiConfigCacheService.getConfigById(configId).then(c => c?.maxConversationMessages || 5);
    const allMessagesDesc = await this.conversationService.getMessages({
      conversationId,
      limit,
      userId,
      sort: '-createdAt',
    });
    const allMessages = [...allMessagesDesc].reverse();

    const messagesToSummarize = triggerMessage
      ? [...allMessages, triggerMessage]
      : allMessages;

    if (messagesToSummarize.length === 0) {
      return;
    }

    const messagesText = messagesToSummarize
      .map((m) => {
        let content = m.content || '';
        
        if (m.toolCalls && m.toolCalls.length > 0) {
          const toolCallsInfo = m.toolCalls.map(tc => {
            const toolName = tc.function?.name || 'unknown';
            let argsStr = '';
            try {
              const args = typeof tc.function?.arguments === 'string' 
                ? JSON.parse(tc.function.arguments) 
                : tc.function?.arguments || {};
              if (toolName === 'create_records' || toolName === 'update_records' || toolName === 'delete_records') {
                argsStr = `${toolName.replace('_records', '')} on ${args.table || 'unknown'}`;
                if (args.ids) argsStr += ` (ids: [${args.ids.slice(0, 3).join(', ')}${args.ids.length > 3 ? '...' : ''}])`;
                if (args.dataArray) argsStr += ` (${args.dataArray.length} items)`;
                if (args.updates) argsStr += ` (${args.updates.length} updates)`;
              } else if (toolName === 'get_table_details') {
                if (Array.isArray(args.tableName)) {
                  const tableCount = args.tableName.length;
                  const tableNames = args.tableName.slice(0, 2).join(', ');
                  argsStr = tableCount > 2 ? `${tableNames}... (${tableCount} tables)` : `${tableNames} (${tableCount} tables)`;
                } else {
                argsStr = args.tableName || 'unknown';
                }
              } else {
                argsStr = JSON.stringify(args).substring(0, 100);
              }
            } catch {
              argsStr = '...';
            }
            return `${toolName}(${argsStr})`;
          }).join(', ');
          content += `\n[tool calls: ${toolCallsInfo}]`;
        }
        
        if (m.toolResults && m.toolResults.length > 0) {
          const toolResultsInfo = m.toolResults.map((tr: any) => {
            const toolCall = m.toolCalls?.find((tc: any) => tc.id === tr.toolCallId);
            const toolName = toolCall?.function?.name || 'unknown';
            let parsedArgs: any = {};
            if (toolCall?.function?.arguments) {
              try {
                parsedArgs = typeof toolCall.function.arguments === 'string'
                  ? JSON.parse(toolCall.function.arguments)
                  : toolCall.function.arguments;
              } catch {
                parsedArgs = {};
              }
            }
            return this.formatToolResultSummary(toolName, parsedArgs, tr.result);
          }).join('\n');
          content += `\n[tool results:\n${toolResultsInfo}]`;
        }
        
        return `${m.role}: ${content}`;
      })
      .join('\n\n');

    const previousContext = conversation.summary
      ? `Previous summary:\n${conversation.summary}\n\n`
      : '';

    const summaryPrompt = `Summarize the following conversation for Enfyra AI agent context. Be thorough but structured (10-15 sentences or structured format). CRITICAL: Preserve all technical details needed for continuation.

Focus on:
1. User's goals and workflow progress (what they're trying to accomplish)
2. Tables created/modified/deleted (include table NAMES and IDs if available)
3. Relations created (include source/target tables, property names, types)
4. Data operations (create/update/delete records, batch operations)
5. Errors encountered and how they were resolved
6. Current database schema state (which tables exist, their relations)
7. Important IDs discovered (table IDs, record IDs) - these are CRITICAL for relations
8. Permission checks and access patterns
9. Any pending work or incomplete operations

Format: Use structured sections if helpful (e.g., "Tables Created:", "Relations Setup:", "Errors Fixed:"). Preserve specific table names, IDs, and relation structures. This summary will be injected into the system prompt for continuation, so it must contain all context needed to resume work without losing information.

${previousContext}Full conversation history to summarize:
${messagesText}`;

    const summaryMessages: LLMMessage[] = [
      {
        role: 'system',
        content: 'You are a conversation summarizer for Enfyra AI agent. Create structured, thorough summaries (10-15 sentences or structured format) that preserve ALL technical details: table names/IDs, relation structures, errors and solutions, workflow progress, and database state. This summary will be used to continue conversations, so completeness is critical. Use structured sections if helpful.',
      },
      {
        role: 'user',
        content: summaryPrompt,
      },
    ];

    try {
      const summaryResponse = await this.llmService.chatSimple({ messages: summaryMessages, configId });
      let summary = summaryResponse.content || '';

      const maxSummaryLen = 2000;
      if (summary.length > maxSummaryLen) {
        summary = summary.slice(0, maxSummaryLen) + '...';
      }

      const summaryTimestamp = new Date();

      await this.conversationService.updateConversation({
        id: conversationId,
        data: {
          summary,
          lastSummaryAt: summaryTimestamp,
        },
        userId,
      });

      if (triggerMessage) {
        await this.conversationService.deleteMessage({ messageId: triggerMessage.id, userId });

        await this.conversationService.createMessage({
          data: {
            conversationId,
            role: triggerMessage.role,
            content: triggerMessage.content,
            sequence: triggerMessage.sequence,
          },
          userId,
        });
      }

      this.logger.log(`Summary created for conversation ${conversationId}. Summary stored in conversation.summary. Total messages summarized: ${messagesToSummarize.length}`);
    } catch (error) {
      this.logger.error('Failed to create conversation summary:', error);
      throw error;
    }
  }
}
