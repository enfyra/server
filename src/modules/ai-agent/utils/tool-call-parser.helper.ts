export function parseToolArguments(args: string | any): any {
  if (!args) {
    return {};
  }
  if (typeof args === 'object' && args !== null) {
    return args;
  }
  if (typeof args === 'string') {
    const trimmed = args.trim();
    if (!trimmed || trimmed === '{}') {
      return {};
    }
    try {
      return JSON.parse(trimmed);
    } catch (e) {
      return {};
    }
  }
  return {};
}

export function normalizeToolCallId(toolCall: any, fallbackIndex?: number): string {
  if (toolCall.id) {
    return toolCall.id;
  }
  if (toolCall.tool_call_id) {
    return toolCall.tool_call_id;
  }
  const timestamp = Date.now();
  const random = Math.random().toString(36).substr(2, 9);
  const index = fallbackIndex !== undefined ? fallbackIndex : 0;
  return `call_${timestamp}_${index}_${random}`;
}

export function extractToolCallName(toolCall: any): string | null {
  return toolCall.function?.name || toolCall.name || null;
}

export function createToolCallCacheKey(toolName: string, parsedArgs: any): string {
  let normalizedArgs: string;
  if (typeof parsedArgs === 'object' && parsedArgs !== null) {
    const sorted = Object.keys(parsedArgs).sort().reduce((acc: any, key) => {
      acc[key] = parsedArgs[key];
      return acc;
    }, {});
    normalizedArgs = JSON.stringify(sorted);
  } else {
    normalizedArgs = String(parsedArgs || '');
  }
  return `${toolName}:${normalizedArgs}`;
}

export function parseToolArgsWithFallback(toolArgs: string | any, fallback: any): any {
  if (typeof toolArgs === 'string') {
    try {
      return JSON.parse(toolArgs);
    } catch (e) {
      return fallback;
    }
  }
  return fallback;
}

