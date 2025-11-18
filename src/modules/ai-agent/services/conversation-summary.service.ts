import { Injectable, Logger } from '@nestjs/common';
import { ConversationService } from './conversation.service';
import { LLMService } from './llm.service';
import { AiConfigCacheService } from '../../../infrastructure/cache/services/ai-config-cache.service';
import { IMessage } from '../interfaces/message.interface';
import { LLMMessage } from '../utils/types';
import { formatToolResultSummary } from '../utils/tool-result-summarizer.helper';

@Injectable()
export class ConversationSummaryService {
  private readonly logger = new Logger(ConversationSummaryService.name);

  constructor(
    private readonly conversationService: ConversationService,
    private readonly llmService: LLMService,
    private readonly aiConfigCacheService: AiConfigCacheService,
  ) {}

  async createSummary(params: {
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
            return formatToolResultSummary(toolName, parsedArgs, tr.result);
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

