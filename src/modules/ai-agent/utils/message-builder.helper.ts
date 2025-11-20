import { Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { IConversation } from '../interfaces/conversation.interface';
import { IMessage } from '../interfaces/message.interface';
import { LLMMessage } from './types';
import { buildSystemPromptForLLM } from './system-prompt-builder.helper';
import { MetadataCacheService } from '../../../infrastructure/cache/services/metadata-cache.service';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';

const logger = new Logger('MessageBuilder');

export async function buildLLMMessages(params: {
  conversation: IConversation;
  messages: IMessage[];
  config: any;
  user?: any;
  needsTools?: boolean;
  hintCategories?: string[];
  selectedToolNames?: string[];
  metadataCacheService: MetadataCacheService;
  queryBuilder: QueryBuilderService;
  configService: ConfigService;
}): Promise<LLMMessage[]> {
  const { conversation, messages, config, user, needsTools = true, hintCategories, selectedToolNames, metadataCacheService, queryBuilder, configService } = params;

  const latestUserMessage = messages.length > 0
    ? messages[messages.length - 1]?.content
    : undefined;

  const systemPrompt = await buildSystemPromptForLLM({ 
    conversation, 
    config, 
    user, 
    latestUserMessage, 
    needsTools, 
    hintCategories, 
    selectedToolNames,
    metadataCacheService,
    queryBuilder,
    configService,
  });
  
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
                  logger.error(`[buildLLMMessages] Failed to parse tool args for ${toolName}: ${parseError.message}, raw: ${match[2]?.substring(0, 200)}`);
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
            }
          } catch (parseError: any) {
            logger.error(`[buildLLMMessages] Failed to parse corrupt message: ${parseError.message}, stack: ${parseError.stack}`);
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

        if (assistantPushed && assistantMessage.tool_calls && assistantMessage.tool_calls.length > 0) {
          const toolCallIds = new Set(assistantMessage.tool_calls.map(tc => tc.id));
          const toolResultsMap = new Map<string, any>();
          
          if (message.toolResults && message.toolResults.length > 0) {
            for (const toolResult of message.toolResults) {
              if (toolCallIds.has(toolResult.toolCallId)) {
                toolResultsMap.set(toolResult.toolCallId, toolResult);
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

