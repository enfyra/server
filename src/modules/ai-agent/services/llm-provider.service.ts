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
        model: config.model?.trim(),
        temperature: 0.7,
        maxOutputTokens: 8192,
        streaming: true,
      });
    }

    if (config.provider === 'DeepSeek') {
      return new ChatDeepSeek({
        apiKey: config.apiKey,
        model: config.model?.trim(),
        timeout: config.llmTimeout || 30000,
        streaming: true,
      });
    }

    if (config.provider === 'GLM') {
      if (!config.apiKey) {
        throw new BadRequestException('GLM provider requires an API key');
      }
      const baseUrl = (config.baseUrl || 'https://open.bigmodel.cn/api/paas/v4').replace(/\/$/, '');
      return new ChatOpenAI({
        apiKey: config.apiKey,
        model: config.model?.trim(),
        timeout: config.llmTimeout || 30000,
        streaming: true,
        configuration: {
          baseURL: baseUrl,
        },
      });
    }

    throw new BadRequestException(`Unsupported LLM provider: ${config.provider}`);
  }
}

