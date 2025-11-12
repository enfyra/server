import { BadRequestException } from '@nestjs/common';
import OpenAI from 'openai';
import Anthropic from '@anthropic-ai/sdk';
import { AiConfig } from '../../../infrastructure/cache/services/ai-config-cache.service';

export async function createLLMClient(config: AiConfig): Promise<OpenAI | Anthropic> {
  if (config.provider === 'OpenAI') {
    if (!config.apiKey) {
      throw new BadRequestException('OpenAI API key is not configured for this config');
    }

    return new OpenAI({
      apiKey: config.apiKey,
    });
  }

  if (config.provider === 'Anthropic') {
    if (!config.apiKey) {
      throw new BadRequestException('Anthropic API key is not configured for this config');
    }

    return new Anthropic({
      apiKey: config.apiKey,
    });
  }

  throw new BadRequestException(`Unsupported LLM provider: ${config.provider}`);
}






