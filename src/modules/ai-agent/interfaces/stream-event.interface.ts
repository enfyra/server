/**
 * Shared StreamEvent interface for AI Agent streaming
 * Used by OpenAI, Anthropic stream clients and AI agent service
 */
export interface StreamEvent {
  type: 'text' | 'tool_result' | 'done' | 'error' | 'tokens';
  data?: any;
}

/**
 * Tool result data structure
 */
export interface ToolResultData {
  toolCallId: string;
  name: string;
  result: any;
}

/**
 * Token usage data structure
 */
export interface TokenUsageData {
  inputTokens: number;
  outputTokens: number;
}

/**
 * Text delta data structure
 */
export interface TextDeltaData {
  delta: string;
  text: string;
}

/**
 * Error data structure
 */
export interface ErrorData {
  error: string;
}
