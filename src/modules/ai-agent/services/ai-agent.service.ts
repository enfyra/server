import { Injectable, Logger, BadRequestException } from '@nestjs/common';
import { Response } from 'express';
import { ConversationService } from './conversation.service';
import { LLMService, LLMMessage } from './llm.service';
import { MetadataCacheService } from '../../../infrastructure/cache/services/metadata-cache.service';
import { AiConfigCacheService } from '../../../infrastructure/cache/services/ai-config-cache.service';
import { AgentRequestDto } from '../dto/agent-request.dto';
import { AgentResponseDto } from '../dto/agent-response.dto';
import { IConversation, IConversationCreate } from '../interfaces/conversation.interface';
import { IMessage } from '../interfaces/message.interface';
import { StreamEvent } from '../interfaces/stream-event.interface';

@Injectable()
export class AiAgentService {
  private readonly logger = new Logger(AiAgentService.name);

  constructor(
    private readonly conversationService: ConversationService,
    private readonly llmService: LLMService,
    private readonly metadataCacheService: MetadataCacheService,
    private readonly aiConfigCacheService: AiConfigCacheService,
  ) {}

  async processRequest(params: {
    request: AgentRequestDto;
    userId?: string | number;
  }): Promise<AgentResponseDto> {
    const { request, userId } = params;

    let conversation: IConversation;
    let configId: string | number;

    if (request.conversation) {
      // Get conversation and config from it
      conversation = await this.conversationService.getConversation({ id: request.conversation, userId });
      if (!conversation) {
        throw new BadRequestException(`Conversation with ID ${request.conversation} not found`);
      }
      configId = conversation.configId;
    } else {
      // Create new conversation - require config from request
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
      this.logger.debug(`User message not found in loaded messages, adding it manually. Expected sequence: ${userSequence}`);
      allMessages.push(userMessage);
    }

    const limit = config.maxConversationMessages || 5;
    let messages = allMessages;

    this.logger.debug(`[Token Debug] Loading ${messages.length} messages from ${allMessages.length} total messages (limit: ${limit})`);

    if (messages.length === 0 || messages[messages.length - 1]?.role !== 'user') {
      this.logger.debug(`Fixing conversation state: appending just-created user message (sequence ${userSequence}). Last role: ${messages[messages.length - 1]?.role}`);
      messages = [...messages, userMessage];
    }

    if (messages.length >= limit) {
      this.logger.debug(`[Summary Trigger] Reached maxConversationMessages=${limit}. Creating summary and recreating trigger message.`);
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
        this.logger.debug(`[Summary Applied] Reloaded ${messages.length} messages since lastSummaryAt (summary + trigger message)`);
      }
    }

    const llmMessages = await this.buildLLMMessages({ conversation, messages, config });

    // Debug: Estimate tokens
    const estimatedTokens = llmMessages.reduce((total, msg) => {
      const content = typeof msg.content === 'string' ? msg.content : JSON.stringify(msg.content);
      return total + Math.ceil(content.length / 4); // Rough estimate: ~4 chars per token
    }, 0);
    this.logger.debug(`[Token Debug] Built ${llmMessages.length} LLM messages, estimated ~${estimatedTokens} tokens`);

    const llmResponse = await this.llmService.chat({ messages: llmMessages, configId });

    const assistantSequence = lastSequence + 2;
    await this.conversationService.createMessage({
      data: {
        conversationId: conversation.id,
        role: 'assistant',
        content: llmResponse.content,
        toolCalls: llmResponse.toolCalls.length > 0 ? llmResponse.toolCalls : null,
        toolResults: llmResponse.toolResults.length > 0 ? llmResponse.toolResults : null,
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
    res: Response;
    userId?: string | number;
  }): Promise<void> {
    const { request, res, userId } = params;

    // Setup SSE headers first
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');

    const sendEvent = (event: StreamEvent) => {
      res.write(`data: ${JSON.stringify(event)}\n\n`);
    };

    const sendErrorAndClose = async (errorMessage: string, conversationId?: string | number, lastSequence?: number) => {
      sendEvent({
        type: 'error',
        data: { error: errorMessage },
      });

      // Save error message to database if conversation exists
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

      await new Promise(resolve => setTimeout(resolve, 100));
      res.end();
    };

    let conversation: IConversation | undefined;
    let lastSequence: number | undefined;

    let configId: string | number;

    try {
      if (request.conversation) {
        // Get conversation and config from it
        conversation = await this.conversationService.getConversation({ id: request.conversation, userId });
        if (!conversation) {
          await sendErrorAndClose(`Conversation with ID ${request.conversation} not found`);
          return;
        }
        configId = conversation.configId;
      } else {
        // Create new conversation - require config from request
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

      this.logger.log(`[AI-Agent][Stream] Using config ${configId} provider=${config.provider} model=${config.model}`);

      sendEvent({
        type: 'text',
        data: { delta: '', text: '', metadata: { conversation: conversation.id } },
      });

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
        this.logger.debug(`User message not found in loaded messages, adding it manually. Expected sequence: ${userSequence}`);
        allMessages.push(userMessage);
      }

      let messages = allMessages;

      this.logger.debug(`[Token Debug - Stream] Loading ${messages.length} messages from ${allMessages.length} total messages (limit: ${limit})`);
      if (messages.length === 0 || messages[messages.length - 1]?.role !== 'user') {
        this.logger.error(`Invalid conversation state: last message is not a user message. Last message role: ${messages[messages.length - 1]?.role}`);
        await sendErrorAndClose('Invalid conversation state: last message must be a user message', conversation.id, userSequence);
        return;
      }

      if (messages.length >= limit) {
        this.logger.debug(`[Summary Trigger - Stream] Reached maxConversationMessages=${limit}. Creating summary and recreating trigger message.`);
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
          this.logger.debug(`[Summary Applied - Stream] Reloaded ${messages.length} messages since lastSummaryAt (summary + trigger message)`);
        }
      }

      const llmMessages = await this.buildLLMMessages({ conversation, messages, config });

      // Debug: Estimate tokens
      const estimatedTokens = llmMessages.reduce((total, msg) => {
        const content = typeof msg.content === 'string' ? msg.content : JSON.stringify(msg.content);
        return total + Math.ceil(content.length / 4); // Rough estimate: ~4 chars per token
      }, 0);
      this.logger.debug(`[Token Debug - Stream] Built ${llmMessages.length} LLM messages, estimated ~${estimatedTokens} tokens`);

      let fullContent = '';
      const allToolResults: any[] = [];

      const llmResponse = await this.llmService.chatStream({
        messages: llmMessages,
        configId,
        onEvent: (event) => {
          if (event.type === 'text' && event.data?.delta) {
            fullContent = event.data.text || fullContent;
            sendEvent(event);
          } else if (event.type === 'tool_result') {
            allToolResults.push(event.data);
            sendEvent(event);
          } else if (event.type === 'tokens') {
            sendEvent(event);
          } else if (event.type === 'error') {
            sendEvent(event);
          }
        },
      });

      sendEvent({
        type: 'done',
        data: {
          conversation: conversation.id,
          finalResponse: llmResponse.content || '',
          toolCalls: llmResponse.toolCalls.map((tc) => {
            const result = llmResponse.toolResults.find((tr) => tr.toolCallId === tc.id);
            return {
              id: tc.id,
              name: tc.function.name,
              arguments: JSON.parse(tc.function.arguments),
              result: result?.result,
            };
          }),
        },
      });

      res.end();

      (async () => {
        try {
          const assistantSequence = lastSequence + 2;
          await this.conversationService.createMessage({
            data: {
              conversationId: conversation.id,
              role: 'assistant',
              content: llmResponse.content,
              toolCalls: llmResponse.toolCalls.length > 0 ? llmResponse.toolCalls : null,
              toolResults: llmResponse.toolResults.length > 0 ? llmResponse.toolResults : null,
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

          this.logger.log(`[Stream] DB save completed for conversation ${conversation.id}`);
        } catch (error) {
          this.logger.error(`[Stream] Failed to save to DB after streaming response:`, error);
        }
      })();
    } catch (error: any) {
      this.logger.error('Stream error:', error);

      // Ensure detailed error message
      const errorMessage = error?.response?.data?.error?.message ||
                          error?.message ||
                          String(error);

      sendEvent({
        type: 'error',
        data: {
          error: errorMessage,
          details: error?.response?.data || error?.data,
        },
      });

      // Save error message to database if conversation exists
      if (conversation && lastSequence !== undefined) {
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

      // Add small delay to ensure error event is flushed before closing stream
      await new Promise(resolve => setTimeout(resolve, 100));

      res.end();
    }
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
  }): Promise<LLMMessage[]> {
    const { conversation, messages, config } = params;

    const systemPrompt = await this.buildSystemPrompt({ conversation, config });
    const llmMessages: LLMMessage[] = [
      {
        role: 'system',
        content: systemPrompt,
      },
    ];

    for (const message of messages) {
      if (message.role === 'user') {
        let userContent = message.content || '';
        const originalLength = userContent.length;
        if (userContent.length > 1000) {
          userContent = userContent.substring(0, 1000) + '... [truncated for token limit]';
          this.logger.debug(`[Token Debug] User message truncated: ${originalLength} -> 1000 chars`);
        }
        llmMessages.push({
          role: 'user',
          content: userContent,
        });
      } else if (message.role === 'assistant') {
        let assistantContent = message.content || null;
        const originalLength = assistantContent?.length || 0;
        if (assistantContent && assistantContent.length > 800) {
          assistantContent = assistantContent.substring(0, 800) + '... [truncated for token limit]';
          this.logger.debug(`[Token Debug] Assistant message truncated: ${originalLength} -> 800 chars`);
        }
        const assistantMessage: LLMMessage = {
          role: 'assistant',
          content: assistantContent,
        };

        if (message.toolCalls && message.toolCalls.length > 0) {
          assistantMessage.tool_calls = message.toolCalls.map((tc) => ({
            id: tc.id,
            type: 'function' as const,
            function: {
              name: tc.function.name,
              arguments: tc.function.arguments,
            },
          }));
        }

        if (assistantMessage.content || assistantMessage.tool_calls) {
          llmMessages.push(assistantMessage);
        }

        if (message.toolResults && message.toolResults.length > 0) {
          for (const toolResult of message.toolResults) {
            const toolCall = message.toolCalls?.find((tc) => tc.id === toolResult.toolCallId);
            const toolName = toolCall?.function?.name || '';

            let resultContent: string;

            if (toolName === 'get_metadata' || toolName === 'get_table_details') {
              // Fully truncate metadata tools - can be re-fetched easily
              const originalSize = JSON.stringify(toolResult.result).length;
              resultContent = JSON.stringify({
                _truncated: true,
                _message: `Tool ${toolName} executed successfully. Details are not included in history to save tokens. Call the tool again if you need the information.`,
              });
              this.logger.debug(`[Token Debug] Tool result ${toolName} fully truncated: ${originalSize} -> ${resultContent.length} chars`);
            } else if (toolName === 'dynamic_repository') {
              // Smart truncation for dynamic_repository
              const result = toolResult.result;
              const resultStr = JSON.stringify(result);
              const hasError = result?.error || result?.message?.includes('Error') || result?.message?.includes('Failed');

              if (hasError) {
                // Keep errors intact
                resultContent = resultStr;
                this.logger.debug(`[Token Debug] Tool result ${toolName} kept (error): ${resultStr.length} chars`);
              } else if (resultStr.length <= 2000) {
                // Keep small results intact
                resultContent = resultStr;
                this.logger.debug(`[Token Debug] Tool result ${toolName} kept (small): ${resultStr.length} chars`);
              } else {
                // Smart truncation for large results
                const smartResult: any = {
                  _truncated: true,
                  success: result.success !== undefined ? result.success : true,
                };

                // Preserve count/total
                if (result.count !== undefined) {
                  smartResult.count = result.count;
                }
                if (result.total !== undefined) {
                  smartResult.total = result.total;
                }

                // Preserve data summary
                if (result.data && Array.isArray(result.data)) {
                  smartResult.dataCount = result.data.length;
                  // Keep first 3 and last 2 items as samples
                  if (result.data.length > 5) {
                    smartResult.dataSample = {
                      first: result.data.slice(0, 3),
                      last: result.data.slice(-2),
                      _note: `Showing ${3 + 2} of ${result.data.length} records. Full data omitted to save tokens.`,
                    };
                  } else {
                    smartResult.data = result.data;
                  }
                } else if (result.data) {
                  smartResult.data = result.data;
                }

                resultContent = JSON.stringify(smartResult);
                this.logger.debug(`[Token Debug] Tool result ${toolName} smart truncated: ${resultStr.length} -> ${resultContent.length} chars`);
              }
            } else if (toolName === 'get_hint') {
              // Keep hints intact - they're already optimized
              resultContent = JSON.stringify(toolResult.result);
              this.logger.debug(`[Token Debug] Tool result ${toolName} kept (hint): ${resultContent.length} chars`);
            } else {
              // Generic truncation for unknown tools
              const resultStr = JSON.stringify(toolResult.result);
              if (resultStr.length > 1000) {
                resultContent = JSON.stringify({
                  _truncated: true,
                  _message: 'Result retrieved successfully.',
                  _size: resultStr.length,
                });
                this.logger.debug(`[Token Debug] Tool result ${toolName} truncated: ${resultStr.length} -> ${resultContent.length} chars`);
              } else {
                resultContent = resultStr;
                this.logger.debug(`[Token Debug] Tool result ${toolName} kept: ${resultStr.length} chars`);
              }
            }

            llmMessages.push({
              role: 'tool',
              content: resultContent,
              tool_call_id: toolResult.toolCallId,
            });
          }
        }
      }
    }

    return llmMessages;
  }

  private async buildSystemPrompt(params: {
    conversation: IConversation;
    config: any;
  }): Promise<string> {
    const { conversation, config } = params;

    let prompt = `You are a helpful AI assistant for database operations.

**Task Management:**
- Multi-step requests: LIST all tasks explicitly
- Track progress: "✅ Done, ⏳ Pending"
- On "continue"/"next": refer to pending tasks from context

**Tool Usage:**
- For greetings/simple questions: respond with text ONLY, NO tools
- Use get_metadata to discover tables
- Use get_table_details to understand table structure
- Use dynamic_repository for CRUD operations
- Use get_hint for detailed guidance (call on-demand when needed)

**CRITICAL: Nested Relations (Avoid Multiple Queries):**
- ALWAYS use nested fields instead of separate queries for related data
- Nested fields: Use "relation.field" (e.g., "roles.name", "roles.*")
- Nested filters: Use { relation: { field: { _eq: value } } }
- Example: "route id 20 roles" → ONE query: dynamic_repository(table="route_definition", where={id:{_eq:20}}, fields="id,path,roles.name,roles.id")
- DON'T query route first, then query role_definition separately
- For complex cases: call get_hint(category="nested_relations")

**CRITICAL: Query Optimization (MUST FOLLOW):**
1. Before ANY data fetch: call get_table_details first to see available fields
2. Fetch ONLY needed fields:
   - Count/total queries: fields="id", limit=0
   - List names: fields="id,name", limit=0
   - Specific data: fields="[only what user asked]"
3. limit=0 means fetch ALL (no limit). Use this for "all", "how many", "total" questions
4. Example: "How many routes?" → get_table_details("route_definition") → dynamic_repository(table="route_definition", operation="find", fields="id", limit=0)

**Error Handling:**
- If tool returns error:true, STOP immediately and report to user
- DO NOT call more tools after errors

**Get More Details via get_hint:**
- Nested relations/queries → get_hint(category="nested_relations")
- Route access control flow → get_hint(category="route_access")
- Table operations → get_hint(category="table_operations")
- Relations → get_hint(category="relations")
- Table discovery → get_hint(category="table_discovery")
- Metadata/auto-fields → get_hint(category="metadata")
- DB type/primary key → get_hint(category="database_type")`;

    if (conversation.summary) {
      prompt += `\n\n[Previous conversation summary]: ${conversation.summary}`;
    }

    return prompt;
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
      this.logger.debug(`No messages to summarize for conversation ${conversationId}`);
      return;
    }

    const messagesText = messagesToSummarize
      .map((m) => {
        let content = m.content || '';
        if (m.toolCalls && m.toolCalls.length > 0) {
          const toolCallsInfo = m.toolCalls.map(tc => `${tc.function.name}(${tc.function.arguments})`).join(', ');
          content += ` [tool calls: ${toolCallsInfo}]`;
        }
        if (m.toolResults && m.toolResults.length > 0) {
          content += ` [tool results: ${m.toolResults.length} results]`;
        }
        return `${m.role}: ${content}`;
      })
      .join('\n');

    const previousContext = conversation.summary
      ? `Previous summary:\n${conversation.summary}\n\n`
      : '';

    const summaryPrompt = `Summarize the following conversation concisely (5-8 sentences max). Focus on:
1. Main topics/goals discussed
2. Key actions completed (tables created, data modified, etc.)
3. Important discoveries (table structures, relations, errors encountered)
4. Current context needed for continuation

Capture ALL important information, but be concise. Use bullet points if helpful.

${previousContext}Full conversation history to summarize:
${messagesText}`;

    const summaryMessages: LLMMessage[] = [
      {
        role: 'system',
        content: 'You are a conversation summarizer. Create concise summaries (5-8 sentences max) capturing: main topics, completed actions, key discoveries, and context for continuation. Be thorough but concise.',
      },
      {
        role: 'user',
        content: summaryPrompt,
      },
    ];

    this.logger.debug(`[createSummary] Preparing to call chatSimple with ${summaryMessages.length} messages`);
    this.logger.debug(`[createSummary] Messages to summarize: ${messagesToSummarize.length}`);
    this.logger.debug(`[createSummary] Total text length: ${messagesText.length} chars`);

    try {
      const summaryResponse = await this.llmService.chatSimple({ messages: summaryMessages, configId });
      let summary = summaryResponse.content || '';

      const maxSummaryLen = 1200;
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
        this.logger.debug(`[createSummary] Recreating trigger message (sequence ${triggerMessage.sequence}) with new createdAt`);

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

        this.logger.debug(`[createSummary] Trigger message recreated successfully`);
      }

      this.logger.log(`Summary created for conversation ${conversationId}. Summary stored in conversation.summary. Total messages summarized: ${messagesToSummarize.length}`);
    } catch (error) {
      this.logger.error('Failed to create conversation summary:', error);
      throw error;
    }
  }
}
