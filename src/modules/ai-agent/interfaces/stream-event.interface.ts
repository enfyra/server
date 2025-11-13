export interface StreamTextEvent {
  type: 'text';
  data: {
    delta: string;
    text: string;
    metadata?: Record<string, any>;
  };
}

export interface StreamToolCallEvent {
  type: 'tool_call';
  data: {
    id: string;
    name: string;
    arguments: any;
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
    conversation: string | number;
    finalResponse: string;
    toolCalls: Array<{
      id: string;
      name: string;
      arguments: any;
      result?: any;
    }>;
  };
}

export type StreamEvent =
  | StreamTextEvent
  | StreamToolCallEvent
  | StreamToolResultEvent
  | StreamTokenEvent
  | StreamErrorEvent
  | StreamDoneEvent;
