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
    delta: string;
    metadata: {
      conversation: string | number;
    };
  };
}

export interface StreamTaskEvent {
  type: 'task';
  data: {
    task: {
      type: string;
      status: 'pending' | 'in_progress' | 'completed' | 'cancelled' | 'failed';
      priority?: number;
      data?: any;
      result?: any;
      error?: string;
      createdAt?: Date;
      updatedAt?: Date;
    } | null;
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
