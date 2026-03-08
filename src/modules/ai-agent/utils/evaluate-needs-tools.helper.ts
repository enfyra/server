import { Logger } from '@nestjs/common';
const { HumanMessage, AIMessage, SystemMessage } = require('@langchain/core/messages');
import { buildEvaluateToolSelectionPrompt } from '../prompts/prompt-builder';
import { TOOL_SHORT_DESCRIPTIONS } from '../prompts/base/evaluate-tool-selection.base';

const logger = new Logger('EvaluateNeedsToolsHelper');

export interface EvaluateNeedsToolsParams {
  userMessage: string;
  configId: string | number;
  conversationHistory?: any[];
  conversationSummary?: string;
  config: any;
  llm: any;
  queryBuilder: any;
}

function extractJsonBlock(input: string): string | null {
  if (!input) {
    return null;
  }
  const fenceMatch = input.match(/```(?:json)?\s*([\s\S]*?)```/i);
  if (fenceMatch && fenceMatch[1]) {
    return fenceMatch[1].trim();
  }
  const startIndex = input.indexOf('{"tools"');
  if (startIndex === -1) {
    return null;
  }
  let depth = 0;
  for (let i = startIndex; i < input.length; i++) {
    const char = input[i];
    if (char === '{') {
      depth += 1;
    } else if (char === '}') {
      depth -= 1;
      if (depth === 0) {
        return input.substring(startIndex, i + 1).trim();
      }
    }
  }
  return null;
}

function shouldSkipEvaluation(
  userMessage: string,
  hasConversationHistory?: boolean,
): { skip: boolean; reason?: string } {
  const userMessageLower = userMessage.toLowerCase().trim();
  const isGreeting = /^(hello|hi|hey|greetings|good (morning|afternoon|evening)|how are you|how do you do|what's up|sup)$/i.test(userMessageLower);
  const isCapabilityQuestion = /^(what can|can you|what do you|what are you|capabilities|abilities|help)/i.test(userMessageLower);
  const isCasual = userMessageLower.length < 20 && !/[a-z]{3,}/i.test(userMessageLower.replace(/[^a-z]/gi, ''));

  if (isGreeting) {
    return { skip: true, reason: 'greeting' };
  }
  if (isCapabilityQuestion) {
    return { skip: true, reason: 'capability_question' };
  }
  if (isCasual && !hasConversationHistory) {
    return { skip: true, reason: 'casual_message' };
  }
  return { skip: false };
}

function buildMessages(params: EvaluateNeedsToolsParams): any[] {
  const { conversationHistory = [], conversationSummary, userMessage, config } = params;
  const provider = config.provider || 'Unknown';
  const systemPrompt = buildEvaluateToolSelectionPrompt(provider);

  const messages: any[] = [
    new SystemMessage(systemPrompt),
  ];

  if (conversationSummary) {
    messages.push(new AIMessage(`[Previous conversation summary]: ${conversationSummary}`));
  }

  if (conversationHistory && conversationHistory.length > 0) {
    for (const msg of conversationHistory) {
      if (msg.role === 'user') {
        messages.push(new HumanMessage(msg.content || ''));
      } else if (msg.role === 'assistant') {
        const content = msg.content || '';
        if (content) {
          messages.push(new AIMessage(content));
        }
      }
    }
  }

  messages.push(new HumanMessage(userMessage));
  return messages;
}

function parseResponseContent(response: any): string {
  const rawContent = response?.content;
  let responseContent: string;
  
  if (Array.isArray(rawContent)) {
    const textBlocks = rawContent
      .map((block: any) => {
        if (typeof block === 'string') {
          return block.trim();
        }
        if (block && typeof block.text === 'string') {
          return block.text.trim();
        }
        return '';
      })
      .filter((text: string) => text.length > 0);
    responseContent = textBlocks.join('\n');
  } else if (typeof rawContent === 'string') {
    responseContent = rawContent;
  } else {
    responseContent = JSON.stringify(rawContent || '');
  }
  
  responseContent = responseContent.trim();
  if (/^json\s*/i.test(responseContent)) {
    responseContent = responseContent.replace(/^json\s*/i, '');
  }
  
  return responseContent;
}

const VALID_TOOL_NAMES = new Set(Object.keys(TOOL_SHORT_DESCRIPTIONS));

export async function evaluateNeedsTools(params: EvaluateNeedsToolsParams): Promise<{ tools: string[] }> {
  const { userMessage, config, llm, conversationHistory } = params;

  const hasConversationHistory = !!(conversationHistory && conversationHistory.length > 0);
  const skipCheck = shouldSkipEvaluation(userMessage, hasConversationHistory);
  if (skipCheck.skip) {
    return { tools: [] };
  }

  try {
    const messages = buildMessages(params);
    const response = await llm.invoke(messages);

    const responseContent = parseResponseContent(response);

    let parsedContent: any = null;
    try {
      parsedContent = JSON.parse(responseContent);
    } catch (e) {
      const extractedJson = extractJsonBlock(responseContent);
      if (extractedJson) {
        try {
          parsedContent = JSON.parse(extractedJson);
        } catch (inner) {
          logger.error(`Failed to parse extracted JSON: ${extractedJson.substring(0, 200)}`);
        }
      }
    }

    if (!parsedContent || !Array.isArray(parsedContent.tools)) {
      return { tools: [] };
    }

    const tools = (parsedContent.tools as string[])
      .filter((t): t is string => typeof t === 'string' && VALID_TOOL_NAMES.has(t));
    return { tools };
  } catch (error: any) {
    const provider = config?.provider || 'Unknown';
    const model = config?.model || 'N/A';
    const baseUrl = config?.baseUrl ? '(set)' : '(default)';
    const status = error?.response?.status ?? error?.status ?? error?.statusCode;
    const errMsg = error?.message ?? String(error);
    logger.warn(
      '[evaluateNeedsTools] provider=' + provider + ' model=' + model + ' baseUrl=' + baseUrl + ' status=' + status + ' error=' + errMsg,
    );
    if (error?.response?.data) {
      logger.warn('[evaluateNeedsTools] response.data=' + JSON.stringify(error.response.data)?.slice(0, 300));
    }
    return { tools: [] };
  }
}

