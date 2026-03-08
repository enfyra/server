import { StreamEvent } from '../interfaces/stream-event.interface';

export interface TokenUsageResult {
  inputTokens?: number;
  outputTokens?: number;
  /** OpenAI: prompt_tokens_details.cached_tokens; Anthropic: cache_read_input_tokens; Gemini: cached_content_token_count; LangChain/Google: input_token_details.cache_read, cachedContentTokenCount */
  cacheHitTokens?: number;
  /** Anthropic: cache_creation_input_tokens */
  cacheCreationTokens?: number;
}

export function extractTokenUsage(source: any): TokenUsageResult | null {
  if (!source) {
    return null;
  }

  const candidates = [
    source,
    source.usage_metadata,
    source.message?.usage_metadata,
    source.usage,
    source.additional_kwargs?.usage_metadata,
    source.response_metadata?.tokenUsage,
    source.response_metadata?.usage,
    source.response_metadata?.usage_metadata,
    source.response_metadata?.metadata?.tokenUsage,
    source.metadata?.tokenUsage,
  ];

  for (const usage of candidates) {
    if (!usage) {
      continue;
    }

    const input =
      usage.input_tokens ??
      usage.prompt_tokens ??
      usage.promptTokens ??
      usage.promptTokenCount ??
      usage.prompt_token_count ??
      usage.inputTokens ??
      usage.total_input_tokens ??
      usage.total_prompt_tokens;

    const output =
      usage.output_tokens ??
      usage.completion_tokens ??
      usage.completionTokens ??
      usage.candidatesTokenCount ??
      usage.candidates_token_count ??
      usage.outputTokens ??
      usage.total_output_tokens ??
      usage.total_completion_tokens;

    const cacheHit =
      usage.prompt_tokens_details?.cached_tokens ??
      usage.input_token_details?.cache_read ??
      usage.cache_read_input_tokens ??
      usage.cacheReadInputTokens ??
      usage.cached_content_token_count ??
      usage.cachedContentTokenCount;
    const cacheCreation =
      usage.cache_creation_input_tokens ??
      usage.input_token_details?.cache_creation ??
      usage.cacheCreationInputTokens;

    if (input !== undefined || output !== undefined) {
      const result: TokenUsageResult = {
        inputTokens: input ?? 0,
        outputTokens: output ?? 0,
      };
      if (cacheHit != null && cacheHit > 0) result.cacheHitTokens = cacheHit;
      if (cacheCreation != null && cacheCreation > 0) result.cacheCreationTokens = cacheCreation;
      return result;
    }
  }

  return null;
}

export function reportTokenUsage(context: string, source: any, onEvent?: (event: StreamEvent) => void) {
  const usage = extractTokenUsage(source);
  if (!usage) {
    return;
  }

  const inputTokens = usage.inputTokens ?? 0;
  const outputTokens = usage.outputTokens ?? 0;

  if (onEvent) {
    const data: {
      inputTokens: number;
      outputTokens: number;
      cacheHitTokens?: number;
      cacheCreationTokens?: number;
    } = { inputTokens, outputTokens };
    if (usage.cacheHitTokens != null) data.cacheHitTokens = usage.cacheHitTokens;
    if (usage.cacheCreationTokens != null) data.cacheCreationTokens = usage.cacheCreationTokens;
    onEvent({
      type: 'tokens',
      data,
    });
  }
}

