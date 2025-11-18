import { Injectable, BadRequestException } from '@nestjs/common';
import { ChatOpenAI } from '@langchain/openai';
import { ChatAnthropic } from '@langchain/anthropic';
import { ChatDeepSeek } from '@langchain/deepseek';

@Injectable()
export class LLMProviderService {
  async createLLM(config: any): Promise<any> {
    if (config.provider === 'OpenAI') {
      return new ChatOpenAI({
        apiKey: config.apiKey,
        model: config.model?.trim(),
        timeout: config.llmTimeout || 30000,
        streaming: true,
      });
    }

    if (config.provider === 'Anthropic') {
      return new ChatAnthropic({
        apiKey: config.apiKey,
        model: config.model,
        temperature: 0.7,
        maxTokens: 4096,
      });
    }

    if (config.provider === 'Google') {
      const { ChatGoogleGenerativeAI } = require('@langchain/google-genai');
      return new ChatGoogleGenerativeAI({
        apiKey: config.apiKey,
        model: config.model?.trim() || 'gemini-2.0-flash-exp',
        temperature: 0.7,
        maxOutputTokens: 8192,
        streaming: true,
      });
    }

    if (config.provider === 'DeepSeek') {
      return new ChatDeepSeek({
        apiKey: config.apiKey,
        model: config.model?.trim() || 'deepseek-chat',
        timeout: config.llmTimeout || 30000,
        streaming: true,
      });
    }

    throw new BadRequestException(`Unsupported LLM provider: ${config.provider}`);
  }
}

