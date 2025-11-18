import { StreamEvent } from '../interfaces/stream-event.interface';

export function extractTokenUsage(source: any): { inputTokens?: number; outputTokens?: number } | null {
  if (!source) {
    return null;
  }

  const candidates = [
    source.usage_metadata,
    source.usage,
    source.response_metadata?.tokenUsage,
    source.response_metadata?.usage,
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
      usage.inputTokens ??
      usage.total_input_tokens ??
      usage.total_prompt_tokens;

    const output =
      usage.output_tokens ??
      usage.completion_tokens ??
      usage.completionTokens ??
      usage.outputTokens ??
      usage.total_output_tokens ??
      usage.total_completion_tokens;

    if (input !== undefined || output !== undefined) {
      return {
        inputTokens: input ?? 0,
        outputTokens: output ?? 0,
      };
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
    onEvent({
      type: 'tokens',
      data: {
        inputTokens,
        outputTokens,
      },
    });
  }
}

