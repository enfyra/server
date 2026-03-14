import { IToolCall, IToolResult } from './tool.types';

export type MessageRole = 'user' | 'assistant' | 'system';

export interface LLMMessage {
  role: 'system' | 'user' | 'assistant' | 'tool';
  content: string | null;
  tool_calls?: IToolCall[];
  tool_call_id?: string;
}

export interface IMessageMetadata {
  boundTools?: string[];
  usedTools?: string[];
  usedToolsCount?: number;
  toolLoops?: number;
  provider?: string;
  model?: string;
  toolsAddedOnDemand?: string[];
  evaluateTools?: string[];
  durationMs?: number;
  cacheHitTokens?: number;
  cacheHitPct?: number;
}

export interface IMessage {
  id: string | number;
  conversationId: string | number;
  role: MessageRole;
  content?: string | null;
  toolCalls?: IToolCall[] | null;
  toolResults?: IToolResult[] | null;
  sequence: number;
  inputTokens?: number | null;
  outputTokens?: number | null;
  metadata?: IMessageMetadata | null;
  createdAt: Date;
}

export interface IMessageCreate {
  conversationId: string | number;
  role: MessageRole;
  content?: string | null;
  toolCalls?: IToolCall[] | null;
  toolResults?: IToolResult[] | null;
  sequence: number;
  inputTokens?: number | null;
  outputTokens?: number | null;
  metadata?: IMessageMetadata | null;
}
