import { Logger } from '@nestjs/common';
import { LLMMessage } from './types';

const { HumanMessage, AIMessage, SystemMessage } = require('@langchain/core/messages');

const logger = new Logger('MessageConverter');

export function convertToLangChainMessages(messages: LLMMessage[]): any[] {
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
                logger.error(`[convertToLangChainMessages] Tool args string appears truncated: length=${toolArgs.length}, last 100 chars: ${toolArgs.substring(Math.max(0, toolArgs.length - 100))}`);
              }
              toolArgs = JSON.parse(toolArgs);
            } catch (e) {
              logger.error(`[convertToLangChainMessages] Failed to parse tool args for ${toolName}: ${e}, argsLength=${toolArgs?.length || 0}, first 500 chars: ${toolArgs?.substring(0, 500)}, last 100 chars: ${toolArgs?.substring(Math.max(0, (toolArgs?.length || 0) - 100))}`);
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
    }
  }

  return result;
}

