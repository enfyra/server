import { StreamEvent } from '../interfaces/stream-event.interface';
import { extractTokenUsage } from './token-usage.helper';
import { reduceContentToString, streamChunkedContent } from './llm-response.helper';

export function processStreamContentDelta(
  chunk: any,
  iterations: number,
  fullContent: string,
  currentContent: string,
  onEvent: (event: StreamEvent) => void,
): { delta: string; newFullContent: string; newCurrentContent: string } {
  if (!chunk.content) {
    return { delta: '', newFullContent: fullContent, newCurrentContent: currentContent };
  }

  let delta = chunk.content;
  if (typeof delta !== 'string') {
    if (Array.isArray(delta)) {
      delta = delta
        .filter((block) => block.type === 'text' && block.text)
        .map((block) => block.text)
        .join('');
    } else if (typeof delta === 'object' && delta.text) {
      delta = delta.text;
    } else {
      delta = JSON.stringify(delta);
    }
  }

  if (!delta) {
    return { delta: '', newFullContent: fullContent, newCurrentContent: currentContent };
  }

  let newFullContent = fullContent;
  let newCurrentContent = currentContent;
  let contentToEmit = delta;

  if (iterations > 1 && fullContent.trim().length > 0 && currentContent.length === 0 && !fullContent.endsWith('\n\n')) {
    contentToEmit = '\n\n' + delta;
    newCurrentContent += contentToEmit;
    newFullContent += contentToEmit;
  } else {
    newCurrentContent += delta;
    newFullContent += delta;
  }

  if (contentToEmit.length > 0 && !contentToEmit.includes('redacted_tool_calls_begin') && !contentToEmit.includes('<|redacted_tool_call')) {
    onEvent({
      type: 'text',
      data: { delta: contentToEmit },
    });
  }

  return { delta: contentToEmit, newFullContent, newCurrentContent };
}

export function processTokenUsage(
  chunk: any,
  accumulatedTokenUsage: { inputTokens: number; outputTokens: number },
  onEvent: (event: StreamEvent) => void,
): { inputTokens: number; outputTokens: number } {
  const chunkUsage = extractTokenUsage(chunk);
  if (chunkUsage && (chunkUsage.inputTokens || chunkUsage.outputTokens)) {
    const prevInput = accumulatedTokenUsage.inputTokens;
    const prevOutput = accumulatedTokenUsage.outputTokens;
    const newInput = Math.max(prevInput, chunkUsage.inputTokens ?? 0);
    const newOutput = Math.max(prevOutput, chunkUsage.outputTokens ?? 0);

    if (newInput > prevInput || newOutput > prevOutput) {
      accumulatedTokenUsage.inputTokens = newInput;
      accumulatedTokenUsage.outputTokens = newOutput;
      onEvent({
        type: 'tokens',
        data: {
          inputTokens: newInput,
          outputTokens: newOutput,
        },
      });
    }
  }

  return accumulatedTokenUsage;
}

export async function processNonStreamingContent(
  aggregateResponse: any,
  iterations: number,
  fullContent: string,
  currentContent: string,
  accumulatedTokenUsage: { inputTokens: number; outputTokens: number },
  abortSignal: AbortSignal | undefined,
  onEvent: (event: StreamEvent) => void,
): Promise<{ newFullContent: string; newCurrentContent: string; newTokenUsage: { inputTokens: number; outputTokens: number } }> {
  const usage = extractTokenUsage(aggregateResponse);
  let newTokenUsage = { ...accumulatedTokenUsage };
  
  if (usage) {
    newTokenUsage.inputTokens = Math.max(newTokenUsage.inputTokens, usage.inputTokens ?? 0);
    newTokenUsage.outputTokens = Math.max(newTokenUsage.outputTokens, usage.outputTokens ?? 0);

    onEvent({
      type: 'tokens',
      data: {
        inputTokens: newTokenUsage.inputTokens,
        outputTokens: newTokenUsage.outputTokens,
      },
    });
  }

  const fullDelta = reduceContentToString(aggregateResponse?.content);
  let newFullContent = fullContent;
  let newCurrentContent = currentContent;

  if (fullDelta) {
    let contentToStream = fullDelta;
    if (iterations > 1 && fullContent.trim().length > 0 && currentContent.length === 0 && !fullContent.endsWith('\n\n')) {
      contentToStream = '\n\n' + fullDelta;
    }
    
    await streamChunkedContent(contentToStream, abortSignal, (chunk) => {
      newCurrentContent += chunk;
      newFullContent += chunk;
      onEvent({
        type: 'text',
        data: { delta: chunk },
      });
    });
  }

  return { newFullContent, newCurrentContent, newTokenUsage };
}

