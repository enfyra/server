import { ITask } from './conversation.types';

export interface StreamTextEvent {
  type: 'text';
  data: {
    delta: string;
    metadata?: Record<string, any>;
  };
}

export interface StreamToolCallEvent {
  type: 'tool_call';
  data: {
    id: string;
    name: string;
    arguments?: any;
    status: 'pending' | 'success' | 'error';
  };
}

export interface StreamToolResultEvent {
  type: 'tool_result';
  data: {
    toolCallId: string;
    name: string;
    result: any;
  };
}

export interface StreamTokenEvent {
  type: 'tokens';
  data: {
    inputTokens: number;
    outputTokens: number;
    cacheHitTokens?: number;
    cacheCreationTokens?: number;
  };
}

export interface StreamErrorEvent {
  type: 'error';
  data: {
    error: string;
    details?: any;
  };
}

export interface StreamDoneEvent {
  type: 'done';
  data: {
    delta: string;
    metadata: {
      conversation: string | number;
    };
  };
}

export interface StreamTaskEvent {
  type: 'task';
  data: {
    task: ITask | null;
  };
}

export type StreamEvent =
  | StreamTextEvent
  | StreamToolCallEvent
  | StreamToolResultEvent
  | StreamTokenEvent
  | StreamErrorEvent
  | StreamDoneEvent
  | StreamTaskEvent;
