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
import { StreamEvent } from '../utils/anthropic-stream-client.helper';

@Injectable()
export class AiAgentService {
  private readonly logger = new Logger(AiAgentService.name);

  constructor(
    private readonly conversationService: ConversationService,
    private readonly llmService: LLMService,
    private readonly metadataCacheService: MetadataCacheService,
    private readonly aiConfigCacheService: AiConfigCacheService,
  ) {}

  async processRequest(request: AgentRequestDto, userId?: string | number): Promise<AgentResponseDto> {
    const config = await this.aiConfigCacheService.getConfigById(request.config);
    if (!config) {
      throw new BadRequestException(`AI config with ID ${request.config} not found`);
    }

    if (!config.isEnabled) {
      throw new BadRequestException(`AI config with ID ${request.config} is disabled`);
    }

    let conversation: IConversation;
    if (request.conversation) {
      conversation = await this.conversationService.getConversation(request.conversation, userId);
      if (!conversation) {
        throw new BadRequestException(`Conversation with ID ${request.conversation} not found`);
      }
    } else {
      if (!request.message || !request.message.trim()) {
        throw new BadRequestException('Message cannot be empty');
      }
      const title = this.generateTitleFromMessage(request.message);
      if (!title || !title.trim()) {
        throw new BadRequestException('Failed to generate conversation title');
      }
      conversation = await this.conversationService.createConversation(
        {
          title,
          messageCount: 0,
          configId: request.config,
        },
        userId,
      );
    }

    const lastSequence = await this.conversationService.getLastSequence(conversation.id, userId);
    const userSequence = lastSequence + 1;

    const userMessage = await this.conversationService.createMessage(
      {
        conversationId: conversation.id,
        role: 'user',
        content: request.message,
        sequence: userSequence,
      },
      userId,
    );

    const fetchLimit = config.maxConversationMessages || 5;
    const allMessagesDesc = await this.conversationService.getMessages(
      conversation.id,
      fetchLimit,
      userId,
      '-createdAt',
    );
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

    // Nếu đã chạm trần tối đa messages gửi lên → tạo summary, xóa message trigger, tạo lại message trigger
    if (messages.length >= limit) {
      this.logger.debug(`[Summary Trigger] Reached maxConversationMessages=${limit}. Creating summary and recreating trigger message.`);
      // createSummary sẽ: tạo summary message, xóa userMessage, tạo lại userMessage
      await this.createSummary(conversation.id, request.config, userId, userMessage);

      // Query lại messages từ lastSummaryAt (sẽ có summary message + userMessage mới tạo)
      const refreshed = await this.conversationService.getConversation(conversation.id, userId);
      if (refreshed?.lastSummaryAt) {
        const recentDesc = await this.conversationService.getMessages(
          conversation.id,
          limit,
          userId,
          '-createdAt',
          refreshed.lastSummaryAt,
        );
        messages = [...recentDesc].reverse();
        this.logger.debug(`[Summary Applied] Reloaded ${messages.length} messages since lastSummaryAt (summary + trigger message)`);
      }
    }

    const llmMessages = await this.buildLLMMessages(conversation, messages, config);

    // Debug: Estimate tokens
    const estimatedTokens = llmMessages.reduce((total, msg) => {
      const content = typeof msg.content === 'string' ? msg.content : JSON.stringify(msg.content);
      return total + Math.ceil(content.length / 4); // Rough estimate: ~4 chars per token
    }, 0);
    this.logger.debug(`[Token Debug] Built ${llmMessages.length} LLM messages, estimated ~${estimatedTokens} tokens`);

    const llmResponse = await this.llmService.chat(llmMessages, request.config);

    const assistantSequence = lastSequence + 2;
    await this.conversationService.createMessage(
      {
        conversationId: conversation.id,
        role: 'assistant',
        content: llmResponse.content,
        toolCalls: llmResponse.toolCalls.length > 0 ? llmResponse.toolCalls : null,
        toolResults: llmResponse.toolResults.length > 0 ? llmResponse.toolResults : null,
        sequence: assistantSequence,
      },
      userId,
    );

    await this.conversationService.updateMessageCount(conversation.id, userId);
    
    await this.conversationService.updateConversation(
      conversation.id,
      {
        lastActivityAt: new Date(),
      },
      userId,
    );

    const updatedConversation = await this.conversationService.getConversation(conversation.id, userId);
    if (!updatedConversation) {
      throw new BadRequestException('Failed to update conversation');
    }

    this.logger.debug(`[Summary Check] messageCount=${updatedConversation.messageCount}, threshold=${config.summaryThreshold}, lastSummaryAt=${updatedConversation.lastSummaryAt || 'none'}`);
    if (this.shouldCreateSummary(updatedConversation, config)) {
      await this.createSummary(conversation.id, request.config, userId);
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

  async processRequestStream(request: AgentRequestDto, res: Response, userId?: string | number): Promise<void> {
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
          await this.conversationService.createMessage(
            {
              conversationId,
              role: 'assistant',
              content: `Error: ${errorMessage}`,
              sequence: lastSequence + 1,
            },
            userId,
          );
          await this.conversationService.updateMessageCount(conversationId, userId);
        } catch (dbError) {
          this.logger.error('Failed to save error message to database:', dbError);
        }
      }

      await new Promise(resolve => setTimeout(resolve, 100));
      res.end();
    };

    let conversation: IConversation | undefined;
    let lastSequence: number | undefined;

    try {
      const config = await this.aiConfigCacheService.getConfigById(request.config);
      if (!config) {
        await sendErrorAndClose(`AI config with ID ${request.config} not found`);
        return;
      }

      if (!config.isEnabled) {
        await sendErrorAndClose(`AI config with ID ${request.config} is disabled`);
        return;
      }

      this.logger.log(`[AI-Agent][Stream] Using config ${request.config} provider=${config.provider} model=${config.model}`);

      if (request.conversation) {
        conversation = await this.conversationService.getConversation(request.conversation, userId);
        if (!conversation) {
          await sendErrorAndClose(`Conversation with ID ${request.conversation} not found`);
          return;
        }
      } else {
        if (!request.message || !request.message.trim()) {
          await sendErrorAndClose('Message cannot be empty');
          return;
        }
        const title = this.generateTitleFromMessage(request.message);
        if (!title || !title.trim()) {
          await sendErrorAndClose('Failed to generate conversation title');
          return;
        }
        conversation = await this.conversationService.createConversation(
          {
            title,
            messageCount: 0,
            configId: request.config,
          },
          userId,
        );
      }

      sendEvent({
        type: 'text',
        data: { delta: '', text: '', metadata: { conversation: conversation.id } },
      });

      lastSequence = await this.conversationService.getLastSequence(conversation.id, userId);
      const userSequence = lastSequence + 1;

      const userMessage = await this.conversationService.createMessage(
        {
          conversationId: conversation.id,
          role: 'user',
          content: request.message,
          sequence: userSequence,
        },
        userId,
      );
      const limit = config.maxConversationMessages || 5;
      const allMessagesDesc = await this.conversationService.getMessages(
        conversation.id,
        limit,
        userId,
        '-createdAt',
      );
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

      // Nếu đã chạm trần tối đa messages gửi lên → tạo summary, xóa message trigger, tạo lại message trigger
      if (messages.length >= limit) {
        this.logger.debug(`[Summary Trigger - Stream] Reached maxConversationMessages=${limit}. Creating summary and recreating trigger message.`);
        // createSummary sẽ: tạo summary message, xóa userMessage, tạo lại userMessage
        await this.createSummary(conversation.id, request.config, userId, userMessage);

        // Query lại messages từ lastSummaryAt (sẽ có summary message + userMessage mới tạo)
        const refreshed = await this.conversationService.getConversation(conversation.id, userId);
        if (refreshed?.lastSummaryAt) {
          const recentDesc = await this.conversationService.getMessages(
            conversation.id,
            limit,
            userId,
            '-createdAt',
            refreshed.lastSummaryAt,
          );
          messages = [...recentDesc].reverse();
          this.logger.debug(`[Summary Applied - Stream] Reloaded ${messages.length} messages since lastSummaryAt (summary + trigger message)`);
        }
      }

      const llmMessages = await this.buildLLMMessages(conversation, messages, config);

      // Debug: Estimate tokens
      const estimatedTokens = llmMessages.reduce((total, msg) => {
        const content = typeof msg.content === 'string' ? msg.content : JSON.stringify(msg.content);
        return total + Math.ceil(content.length / 4); // Rough estimate: ~4 chars per token
      }, 0);
      this.logger.debug(`[Token Debug - Stream] Built ${llmMessages.length} LLM messages, estimated ~${estimatedTokens} tokens`);

      let fullContent = '';
      const allToolCalls: any[] = [];
      const allToolResults: any[] = [];

      const llmResponse = await this.llmService.chatStream(llmMessages, request.config, (event) => {
        if (event.type === 'text' && event.data?.delta) {
          fullContent = event.data.text || fullContent;
          sendEvent(event);
        } else if (event.type === 'tool_call') {
          allToolCalls.push(event.data);
          sendEvent(event);
        } else if (event.type === 'tool_result') {
          allToolResults.push(event.data);
          sendEvent(event);
        } else if (event.type === 'tokens') {
          sendEvent(event);
        } else if (event.type === 'error') {
          sendEvent(event);
        }
        // NOTE: 'done' event from LLM is NOT forwarded here - we send our own 'done' after DB save
      });

      // GỬI 'done' event NGAY sau khi stream xong, TRƯỚC KHI lưu DB
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

      // LƯU DB ASYNC sau khi đã gửi response cho client
      // Không await để không block việc kết thúc response
      (async () => {
        try {
          const assistantSequence = lastSequence + 2;
          await this.conversationService.createMessage(
            {
              conversationId: conversation.id,
              role: 'assistant',
              content: llmResponse.content,
              toolCalls: llmResponse.toolCalls.length > 0 ? llmResponse.toolCalls : null,
              toolResults: llmResponse.toolResults.length > 0 ? llmResponse.toolResults : null,
              sequence: assistantSequence,
            },
            userId,
          );

          await this.conversationService.updateMessageCount(conversation.id, userId);

          await this.conversationService.updateConversation(
            conversation.id,
            {
              lastActivityAt: new Date(),
            },
            userId,
          );

          const updatedConversation = await this.conversationService.getConversation(conversation.id, userId);
          if (updatedConversation) {
            this.logger.debug(`[Summary Check - Stream] messageCount=${updatedConversation.messageCount}, threshold=${config.summaryThreshold}, lastSummaryAt=${updatedConversation.lastSummaryAt || 'none'}`);
          }
          if (updatedConversation && this.shouldCreateSummary(updatedConversation, config)) {
            await this.createSummary(conversation.id, request.config, userId);
          }

          this.logger.log(`[Stream] DB save completed for conversation ${conversation.id}`);
        } catch (error) {
          this.logger.error(`[Stream] Failed to save to DB after streaming response:`, error);
          // Response đã được gửi cho client, không thể gửi error nữa
          // Chỉ log error để admin biết
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
          await this.conversationService.createMessage(
            {
              conversationId: conversation.id,
              role: 'assistant',
              content: `Error: ${errorMessage}`,
              sequence: assistantSequence,
            },
            userId,
          );
          await this.conversationService.updateMessageCount(conversation.id, userId);
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

  private async buildLLMMessages(
    conversation: IConversation,
    messages: IMessage[],
    config: any,
  ): Promise<LLMMessage[]> {
    const systemPrompt = await this.buildSystemPrompt(conversation, config);
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
              const originalSize = JSON.stringify(toolResult.result).length;
              resultContent = JSON.stringify({
                _truncated: true,
                _message: `Tool ${toolName} executed successfully. Details are not included in history to save tokens. Call the tool again if you need the information.`,
              });
              this.logger.debug(`[Token Debug] Tool result ${toolName} fully truncated: ${originalSize} -> ${resultContent.length} chars`);
            } else if (toolName === 'dynamic_repository') {
              const resultStr = JSON.stringify(toolResult.result);
              const hasError = toolResult.result?.error || toolResult.result?.message?.includes('Error') || toolResult.result?.message?.includes('Failed');

              if (hasError) {
                resultContent = resultStr;
                this.logger.debug(`[Token Debug] Tool result ${toolName} kept (error): ${resultStr.length} chars`);
              } else if (resultStr.length > 300) {
                resultContent = JSON.stringify({
                  _truncated: true,
                  _message: 'Operation completed successfully.',
                });
                this.logger.debug(`[Token Debug] Tool result ${toolName} truncated: ${resultStr.length} -> ${resultContent.length} chars`);
              } else {
                resultContent = resultStr;
                this.logger.debug(`[Token Debug] Tool result ${toolName} kept (small): ${resultStr.length} chars`);
              }
            } else {
              const resultStr = JSON.stringify(toolResult.result);
              if (resultStr.length > 300) {
                resultContent = JSON.stringify({
                  _truncated: true,
                  _message: 'Result retrieved.',
                });
                this.logger.debug(`[Token Debug] Tool result ${toolName} truncated: ${resultStr.length} -> ${resultContent.length} chars`);
              } else {
                resultContent = resultStr;
                this.logger.debug(`[Token Debug] Tool result ${toolName} kept (small): ${resultStr.length} chars`);
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

  private async buildSystemPrompt(conversation: IConversation, config: any): Promise<string> {
    let prompt = `You are a helpful AI assistant for database operations.

**Task Management - CRITICAL:**
- When user gives multi-step requests (e.g., "create ecom with tables A, B, C"), LIST all tasks explicitly
- Track completed vs pending tasks in your responses
- When user says "continue" or "next", refer to pending tasks from context
- Example format: "Tasks: ✅ A, ✅ B, ⏳ C, ⏳ D, ⏳ E"

**Tool Usage:**
- Only use tools for database operations or system info requests.
- For greetings/questions: respond with text only.
- **CRITICAL: If you are unsure about ANYTHING (database type, field names, relation behavior, best practices, etc.), ALWAYS call get_hint FIRST to get guidance.**
- Before ANY create/update/delete: call get_hint first.
- Use get_metadata to discover tables.
- Use get_table_details for table structure.
- Use dynamic_repository for CRUD operations.
- NEVER create tables without user confirmation.

**CRITICAL: Error Handling - YOU MUST FOLLOW THIS!**
- If ANY tool returns error: true, YOU MUST STOP ALL OPERATIONS IMMEDIATELY
- DO NOT call any additional tools after receiving an error
- DO NOT attempt to recover or fix errors automatically
- IMMEDIATELY tell the user about the error and ask what to do next
- Violating this rule is STRICTLY FORBIDDEN

**Key Rules:**
- createdAt/updatedAt are auto-added to all tables.
- FK columns are auto-indexed.
- Check get_hint for database type (MongoDB uses "_id", SQL uses "id").
- Table discovery policy:
  - NEVER assume table names from user phrasing. If unsure, CALL get_metadata to fetch the list of tables.
  - Infer the closest table name from the returned list (e.g., "route" → "route_definition" if present).
  - For detailed structure before operating, CALL get_table_details with the chosen table name.`;

    if (conversation.summary) {
      const contextSummary = conversation.summary.length > 1200 ? `${conversation.summary.slice(0, 1200)}...` : conversation.summary;
      prompt += `\n\n[Context]: ${contextSummary}`;
    }

    return prompt;
  }

  private shouldCreateSummary(conversation: IConversation, config: any): boolean {
    if (conversation.messageCount <= config.summaryThreshold) {
      return false;
    }

    if (!conversation.lastSummaryAt) {
      return true;
    }

    const oneHourAgo = new Date(Date.now() - 3600000);
    return new Date(conversation.lastSummaryAt) < oneHourAgo;
  }

  private async createSummary(conversationId: string | number, configId: string | number, userId?: string | number, triggerMessage?: IMessage): Promise<void> {
    const conversation = await this.conversationService.getConversation(conversationId, userId);
    if (!conversation) {
      return;
    }

    // Lấy tất cả messages cũ (KHÔNG BAO GỒM triggerMessage) để tóm tắt
    const allMessagesDesc = await this.conversationService.getMessages(
      conversationId,
      undefined, // Lấy tất cả
      userId,
      '-createdAt',
    );
    const allMessages = [...allMessagesDesc].reverse();

    // Lọc ra các messages cũ (sequence < triggerMessage.sequence)
    const oldMessages = triggerMessage
      ? allMessages.filter(m => m.sequence < triggerMessage.sequence)
      : allMessages;

    if (oldMessages.length === 0) {
      this.logger.debug(`No old messages to summarize for conversation ${conversationId}`);
      return;
    }

    const recentText = oldMessages
      .map((m) => `${m.role}: ${m.content || '[tool calls]'}`)
      .join('\n');
    // Giới hạn độ dài prompt tóm tắt để tránh quá nhiều token
    const recentTextTrimmed = recentText.length > 2000 ? `${recentText.slice(0, 2000)}\n...[truncated]` : recentText;

    // Bao gồm summary cũ (nếu có) để giữ context từ các lần summary trước
    const previousContext = conversation.summary
      ? `Previous summary:\n${conversation.summary}\n\n`
      : '';

    const summaryPrompt = `Create a summary focusing on TASKS and PROGRESS:

1. User's main goal/project
2. Completed tasks (with ✅)
3. Pending tasks (with ⏳)
4. Important context (tables, data discovered)

Format: "Goal: [X]. Completed: ✅ A, ✅ B. Pending: ⏳ C, ⏳ D. Context: [important info]"

${previousContext}Recent conversation history:
${recentTextTrimmed}`;

    const summaryMessages: LLMMessage[] = [
      {
        role: 'system',
        content: 'You are a task-focused summarizer. Create concise summaries that prioritize tracking completed and pending tasks. Use format: "Goal: [X]. Completed: ✅ [tasks]. Pending: ⏳ [tasks]. Context: [key info]"',
      },
      {
        role: 'user',
        content: summaryPrompt,
      },
    ];

    try {
      // Dùng chatSimple để không gửi tools và giảm tokens
      const summaryResponse = await this.llmService.chatSimple(summaryMessages, configId);
      let summary = summaryResponse.content || '';

      // CHỈ GIỮ summary mới, vì nó đã tóm tắt TẤT CẢ messages (bao gồm cả context từ summary cũ nếu có)
      // KHÔNG append summary cũ vì sẽ dài ra và phần mới nhất (quan trọng nhất) sẽ bị cắt

      // Cắt ngắn summary nếu quá dài (giữ phần ĐẦU vì đó là phần quan trọng nhất)
      const maxSummaryLen = 1200;
      if (summary.length > maxSummaryLen) {
        summary = summary.slice(0, maxSummaryLen) + '...';
      }

      const summaryTimestamp = new Date();

      // Update conversation với summary + lastSummaryAt
      // KHÔNG xóa messages cũ - giữ toàn bộ messages trong DB để audit/history
      // Query sẽ dùng `createdAt >= lastSummaryAt` để chỉ load messages gần đây cho LLM
      await this.conversationService.updateConversation(
        conversationId,
        {
          summary,
          lastSummaryAt: summaryTimestamp,
        },
        userId,
      );

      this.logger.log(`Summary created for conversation ${conversationId}. Summary stored in conversation.summary. Old messages preserved in DB. Total messages summarized: ${oldMessages.length}`);
    } catch (error) {
      this.logger.error('Failed to create conversation summary:', error);
      throw error;
    }
  }
}

