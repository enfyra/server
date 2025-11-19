import { Logger } from '@nestjs/common';
const { HumanMessage, AIMessage, SystemMessage } = require('@langchain/core/messages');
import { buildEvaluateNeedsToolsPrompt } from '../prompts/prompt-builder';
import { extractTokenUsage } from './token-usage.helper';

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
  const startIndex = input.indexOf('{"categories"');
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

function shouldSkipEvaluation(userMessage: string): { skip: boolean; reason?: string } {
  const userMessageLower = userMessage.toLowerCase().trim();
  const isGreeting = /^(xin chào|hello|hi|hey|chào|greetings|good (morning|afternoon|evening)|how are you|how do you do|what's up|sup)$/i.test(userMessageLower);
  const isCapabilityQuestion = /^(bạn làm|what can|can you|what do you|what are you|capabilities|abilities|help|giúp gì|bạn giúp)/i.test(userMessageLower);
  const isCasual = userMessageLower.length < 20 && !/[a-z]{3,}/i.test(userMessageLower.replace(/[^a-z]/gi, ''));

  if (isGreeting) {
    return { skip: true, reason: 'greeting' };
  }
  if (isCapabilityQuestion) {
    return { skip: true, reason: 'capability_question' };
  }
  if (isCasual) {
    return { skip: true, reason: 'casual_message' };
  }
  return { skip: false };
}

function buildMessages(params: EvaluateNeedsToolsParams): any[] {
  const { conversationHistory = [], conversationSummary, userMessage, config } = params;
  const provider = config.provider || 'Unknown';
  const systemPrompt = buildEvaluateNeedsToolsPrompt(provider);

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

export async function evaluateNeedsTools(params: EvaluateNeedsToolsParams): Promise<{ toolNames: string[]; categories?: string[] }> {
  const { userMessage, config, llm } = params;

  const skipCheck = shouldSkipEvaluation(userMessage);
  if (skipCheck.skip) {
    return { toolNames: [], categories: [] };
  }

  try {
    const messages = buildMessages(params);
    const response = await llm.invoke(messages);
    
    const responseContent = parseResponseContent(response);
    const tokenUsage = extractTokenUsage(response);

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

    if (!parsedContent) {
      return { toolNames: [], categories: [] };
    }

    if (parsedContent.categories !== undefined) {
      const selectedCategories = Array.isArray(parsedContent.categories) ? parsedContent.categories : [];
      if (selectedCategories.length === 0) {
        return { toolNames: [], categories: [] };
      }
      return { toolNames: [], categories: selectedCategories };
    }

    return { toolNames: [], categories: [] };
  } catch (error) {
    logger.error(`Error in evaluateNeedsTools: ${error instanceof Error ? error.message : String(error)}`);
    return { toolNames: [] };
  }
}

