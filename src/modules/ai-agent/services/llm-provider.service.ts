import { Injectable, BadRequestException } from '@nestjs/common';
import { ChatOpenAI } from '@langchain/openai';
import { ChatAnthropic } from '@langchain/anthropic';

@Injectable()
export class LLMProviderService {
  async createLLM(config: any): Promise<any> {
    if (config.provider === 'OpenAI') {
      const baseURL = config.baseUrl
        ? String(config.baseUrl).replace(/\/$/, '')
        : undefined;
      return new ChatOpenAI({
        apiKey: config.apiKey,
        model: config.model?.trim(),
        timeout: config.llmTimeout || 30000,
        streaming: true,
        configuration: baseURL ? { baseURL } : undefined,
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
        model: config.model?.trim(),
        temperature: 0.7,
        maxOutputTokens: 8192,
        streaming: true,
      });
    }

    throw new BadRequestException(`Unsupported LLM provider: ${config.provider}`);
  }
}

