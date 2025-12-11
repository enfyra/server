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

    const summaryPrompt = `Summarize this conversation for Enfyra AI agent. Keep it compact but complete so the agent can continue work without missing context.

CRITICAL - Capture current flow:
- Goal & progress (what the user wants + what is already done)
- Active task: type/status/priority + key data or results
- Schema changes: tables created/updated/deleted with names + IDs
- Relations: source→target, fields/types, and related IDs
- Data ops: create/update/delete counts, ids, filters used
- Routes/API: any route_definition lookups, paths, base URLs
- Tools: tools invoked + purpose (1 line), note failed/retried calls
- Errors & fixes: what failed, how it was resolved or next action
- Pending/next steps: what remains or is blocked

Output as one compact structured paragraph. Example:
"Goal: build blog. Task: system_workflows in_progress. Tables: users(id:1), posts(id:2 updated). Relations: posts.userId→users.id. Data: created posts (count 3). Routes: checked route_definition (no match). Tools: find_records(posts), create_records(posts). Errors: FK missing, fixed by creating users. Pending: add comments table."

${previousContext}Conversation to summarize:
${messagesText}`;

    const summaryMessages: LLMMessage[] = [
      {
        role: 'system',
        content: 'You are the conversation summarizer for Enfyra AI agent. Produce ultra-compact, structured summaries that keep the agent aligned with the current workflow. Always include goal, active task, schema/data changes with IDs, route/API findings, tool usage highlights, errors/resolutions, and pending next steps. No fluff, no lists—return a tight paragraph the agent can reuse directly.',
      },
      {
        role: 'user',
        content: summaryPrompt,
      },
    ];

    try {
      const summaryResponse = await this.llmService.chatSimple({ messages: summaryMessages, configId });
      let summary = summaryResponse.content || '';

      const maxSummaryLen = 3000;
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

