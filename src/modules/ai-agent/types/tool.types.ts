export interface IToolCall {
  id: string;
  type: 'function';
  function: {
    name: string;
    arguments: string;
  };
}

export interface IToolResult {
  toolCallId: string;
  result: any;
  /** 'success' | 'error' - explicit status for filtering/analytics. Derived from result?.error when saving. */
  status?: 'success' | 'error';
}

export interface LLMResponse {
  content: string | null;
  toolCalls: IToolCall[];
  toolResults: IToolResult[];
  toolLoops?: number;
  toolsAddedOnDemand?: string[];
}
