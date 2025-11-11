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

  async processRequest(request: AgentRequestDto, userId?: number): Promise<AgentResponseDto> {
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

    const allMessages = await this.conversationService.getMessages(
      conversation.id,
      undefined,
      userId,
    );
    
    if (allMessages.length === 0 || allMessages[allMessages.length - 1]?.sequence !== userSequence) {
      this.logger.warn(`User message not found in loaded messages, adding it manually. Expected sequence: ${userSequence}`);
      allMessages.push(userMessage);
    }
    
    const limit = config.maxConversationMessages || 5;
    const messages = allMessages.slice(-limit);

    this.logger.debug(`[Token Debug] Loading ${messages.length} messages from ${allMessages.length} total messages (limit: ${limit})`);

    if (messages.length === 0 || messages[messages.length - 1]?.role !== 'user') {
      this.logger.error(`Invalid conversation state: last message is not a user message. Last message role: ${messages[messages.length - 1]?.role}`);
      throw new BadRequestException('Invalid conversation state: last message must be a user message');
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

  async processRequestStream(request: AgentRequestDto, res: Response, userId?: number): Promise<void> {
    // Setup SSE headers first
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');

    const sendEvent = (event: StreamEvent) => {
      res.write(`data: ${JSON.stringify(event)}\n\n`);
    };

    const sendErrorAndClose = async (errorMessage: string) => {
      sendEvent({
        type: 'error',
        data: { error: errorMessage },
      });
      await new Promise(resolve => setTimeout(resolve, 100));
      res.end();
    };

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

      let conversation: IConversation;
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
          },
          userId,
        );
      }

      sendEvent({
        type: 'text',
        data: { delta: '', text: '', metadata: { conversation: conversation.id } },
      });

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

      const allMessages = await this.conversationService.getMessages(
        conversation.id,
        undefined,
        userId,
      );
      
      if (allMessages.length === 0 || allMessages[allMessages.length - 1]?.sequence !== userSequence) {
        this.logger.warn(`User message not found in loaded messages, adding it manually. Expected sequence: ${userSequence}`);
        allMessages.push(userMessage);
      }
      
      const limit = config.maxConversationMessages || 5;
      const messages = allMessages.slice(-limit);

      this.logger.debug(`[Token Debug - Stream] Loading ${messages.length} messages from ${allMessages.length} total messages (limit: ${limit})`);

      if (messages.length === 0 || messages[messages.length - 1]?.role !== 'user') {
        this.logger.error(`Invalid conversation state: last message is not a user message. Last message role: ${messages[messages.length - 1]?.role}`);
        await sendErrorAndClose('Invalid conversation state: last message must be a user message');
        return;
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
        } else if (event.type === 'done') {
          sendEvent(event);
        } else if (event.type === 'error') {
          sendEvent(event);
        }
      });

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
      if (updatedConversation && this.shouldCreateSummary(updatedConversation, config)) {
        await this.createSummary(conversation.id, request.config, userId);
      }

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
- Check get_hint for database type (MongoDB uses "_id", SQL uses "id").`;

    if (conversation.summary) {
      prompt += `\n\n[Context]: ${conversation.summary}`;
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

  private async createSummary(conversationId: number, configId: number, userId?: number): Promise<void> {
    const messages = await this.conversationService.getMessages(conversationId, undefined, userId);
    const conversation = await this.conversationService.getConversation(conversationId, userId);
    if (!conversation) {
      return;
    }

    const summaryPrompt = `Create a summary focusing on TASKS and PROGRESS:

1. User's main goal/project
2. Completed tasks (with ✅)
3. Pending tasks (with ⏳)
4. Important context (tables, data discovered)

Format: "Goal: [X]. Completed: ✅ A, ✅ B. Pending: ⏳ C, ⏳ D. Context: [important info]"

Conversation history:
${messages.map((msg) => `${msg.role}: ${msg.content || '[tool calls]'}`).join('\n')}`;

    const lastSequence = await this.conversationService.getLastSequence(conversationId, userId);

    await this.conversationService.createMessage(
      {
        conversationId: conversationId,
        role: 'user',
        content: summaryPrompt,
        sequence: lastSequence + 1,
      },
      userId,
    );

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
      const summaryResponse = await this.llmService.chat(summaryMessages, configId);
      const summary = summaryResponse.content || '';

      await this.conversationService.createMessage(
        {
          conversationId: conversationId,
          role: 'assistant',
          content: summary,
          sequence: lastSequence + 2,
        },
        userId,
      );

      // Update conversation with summary only, keep all messages
      await this.conversationService.updateConversation(
        conversationId,
        {
          summary,
          lastSummaryAt: new Date(),
        },
        userId,
      );

      this.logger.log(`Summary created for conversation ${conversationId}`);
    } catch (error) {
      this.logger.error('Failed to create conversation summary:', error);
      throw error;
    }
  }
}

