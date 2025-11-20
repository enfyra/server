import { Logger } from '@nestjs/common';
const { ToolMessage } = require('@langchain/core/messages');
import { IToolCall, IToolResult } from '../interfaces/message.interface';

const logger = new Logger('ToolCallProcessorHelper');

export interface ProcessToolCallParams {
  toolCall: any;
  tools: any[];
  selectedToolNames: string[];
  conversationMessages: any[];
  config: any;
  conversationId?: string | number;
}

export interface ProcessToolCallResult {
  toolCall: IToolCall | null;
  toolResult: IToolResult | null;
  error?: any;
}

export function processToolCall(params: ProcessToolCallParams): ProcessToolCallResult {
  const { toolCall, tools, selectedToolNames, conversationMessages, config, conversationId } = params;
  const toolName = toolCall.function?.name || toolCall.name;
  const toolArgs = toolCall.function?.arguments || toolCall.arguments;
  const toolId = toolCall.id;

  if (!toolName) {
    logger.error(`[LLM Chat] Tool name is undefined. Full tool call: ${JSON.stringify(toolCall)}`);
    return { toolCall: null, toolResult: null };
  }

  if (!toolId) {
    logger.error(`[LLM Chat] Tool ID is missing for ${toolName}. Full tool call: ${JSON.stringify(toolCall)}`);
    return { toolCall: null, toolResult: null };
  }

  const validToolNames = new Set(selectedToolNames);
  if (selectedToolNames.length > 0 && !validToolNames.has(toolName)) {
    const errorMsg = `Tool "${toolName}" is not available. Available tools: ${selectedToolNames.join(', ')}. You can ONLY call tools that are provided in your system prompt.`;
    logger.error(`[LLM Chat] ${errorMsg}`);
    
    const errorResult = {
      error: true,
      errorCode: 'TOOL_NOT_AVAILABLE',
      message: errorMsg,
      availableTools: selectedToolNames,
    };
    
    conversationMessages.push(
      new ToolMessage({
        content: JSON.stringify(errorResult),
        tool_call_id: toolId,
      })
    );
    
    return {
      toolCall: {
        id: toolId,
        type: 'function',
        function: {
          name: toolName,
          arguments: typeof toolArgs === 'string' ? toolArgs : JSON.stringify(toolArgs || {}),
        },
      },
      toolResult: {
        toolCallId: toolId,
        result: errorResult,
      },
    };
  }

  const toolCallObj: IToolCall = {
    id: toolId,
    type: 'function',
    function: {
      name: toolName,
      arguments: typeof toolArgs === 'string' ? toolArgs : JSON.stringify(toolArgs || {}),
    },
  };

  return { toolCall: toolCallObj, toolResult: null };
}

export async function executeToolCall(params: {
  toolCall: IToolCall;
  tools: any[];
  conversationMessages: any[];
  config: any;
  conversationId?: string | number;
}): Promise<IToolResult> {
  const { toolCall, tools, conversationMessages, config, conversationId } = params;
  const toolName = toolCall.function.name;
  const toolArgs = toolCall.function.arguments;
  const toolId = toolCall.id;

  try {
    const tool = tools.find((t) => t.name === toolName);
    if (!tool) {
      throw new Error(`Tool ${toolName} not found`);
    }

    let parsedArgs: any = {};
    if (typeof toolArgs === 'string') {
      try {
        parsedArgs = JSON.parse(toolArgs);
      } catch (parseError: any) {
        logger.error(`[LLM Chat] Failed to parse tool args string: ${toolArgs}`);
        throw new Error(`Invalid JSON in tool arguments: ${parseError.message}`);
      }
    } else if (typeof toolArgs === 'object' && toolArgs !== null) {
      parsedArgs = toolArgs;
    } else {
      parsedArgs = {};
    }

    const toolResult = await tool.func(parsedArgs);
    const resultObj = typeof toolResult === 'string' ? JSON.parse(toolResult) : toolResult;

    const hasExistingToolMessage = conversationMessages.some(
      (m: any) => m.constructor.name === 'ToolMessage' && m.tool_call_id === toolId
    );
    
    if (!hasExistingToolMessage) {
      conversationMessages.push(
        new ToolMessage({
          content: JSON.stringify(resultObj),
          tool_call_id: toolId,
        }),
      );
    }

    return {
      toolCallId: toolId,
      result: resultObj,
    };
  } catch (error: any) {
    logger.error('[LLMService-chat] tool execution failed', {
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
    logger.error(`[LLMTool] ${JSON.stringify(errorPayload)}`);
    
    const errorResult = { error: error.message || String(error) };
    
    const hasExistingToolMessage = conversationMessages.some(
      (m: any) => m.constructor.name === 'ToolMessage' && m.tool_call_id === toolId
    );
    
    if (!hasExistingToolMessage) {
      conversationMessages.push(
        new ToolMessage({
          content: JSON.stringify(errorResult),
          tool_call_id: toolId,
        }),
      );
    }

    return {
      toolCallId: toolId,
      result: errorResult,
    };
  }
}

