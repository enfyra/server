import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Response } from 'express';
import { ConversationService } from './conversation.service';
import { LLMService } from './llm.service';
import { StreamManagementService } from './stream-management.service';
import { ConversationSummaryService } from './conversation-summary.service';
import { MetadataCacheService } from '../../../infrastructure/cache/services/metadata-cache.service';
import { AiConfigCacheService } from '../../../infrastructure/cache/services/ai-config-cache.service';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { AgentRequestDto } from '../dto/agent-request.dto';
import { IConversation } from '../interfaces/conversation.interface';
import { StreamEvent } from '../interfaces/stream-event.interface';
import { generateTitleFromMessage } from '../utils/conversation-helper';
import { buildLLMMessages } from '../utils/message-builder.helper';
import { summarizeToolResults } from '../utils/tool-result-summarizer.helper';
import { selectToolsForRequest } from '../utils/tool-selection.helper';

@Injectable()
export class AiAgentService {
  private readonly logger = new Logger(AiAgentService.name);

  constructor(
    private readonly conversationService: ConversationService,
    private readonly llmService: LLMService,
    private readonly streamManagementService: StreamManagementService,
    private readonly conversationSummaryService: ConversationSummaryService,
    private readonly metadataCacheService: MetadataCacheService,
    private readonly aiConfigCacheService: AiConfigCacheService,
    private readonly queryBuilder: QueryBuilderService,
    private readonly configService: ConfigService,
  ) {}


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
        this.streamManagementService.unregisterStream(conversationIdForCleanup);
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
          ? summarizeToolResults(uniqueToolCalls || [], allToolResults)
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
        this.streamManagementService.registerStream(conversation.id, abortController, { onClose });

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
        const title = generateTitleFromMessage(request.message);
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
        this.streamManagementService.registerStream(conversation.id, abortController, { onClose });

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
        await this.conversationSummaryService.createSummary({ conversationId: conversation.id, configId, userId, triggerMessage: userMessage });
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

      const {
        selectedToolNames,
        toolsDefSize,
        hintCategories,
        needsTools,
      } = selectToolsForRequest({
        evaluateCategories: evaluateResult.categories || [],
        queryBuilder: this.queryBuilder,
        provider: config.provider,
      });

      const llmMessages = await buildLLMMessages({
        conversation,
        messages,
        config,
        user,
        needsTools,
        hintCategories,
        selectedToolNames,
        metadataCacheService: this.metadataCacheService,
        queryBuilder: this.queryBuilder,
        configService: this.configService,
      });
      
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
            ? summarizeToolResults(toolCallsToSave, toolResultsToSave)
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

    return await this.streamManagementService.cancelStream({ conversation });
  }

}
