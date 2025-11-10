export type MessageRole = 'user' | 'assistant' | 'system';

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
}

export interface IMessage {
  id: number;
  conversationId: number;
  role: MessageRole;
  content?: string | null;
  toolCalls?: IToolCall[] | null;
  toolResults?: IToolResult[] | null;
  sequence: number;
  createdAt: Date;
}

export interface IMessageCreate {
  conversationId: number;
  role: MessageRole;
  content?: string | null;
  toolCalls?: IToolCall[] | null;
  toolResults?: IToolResult[] | null;
  sequence: number;
}

