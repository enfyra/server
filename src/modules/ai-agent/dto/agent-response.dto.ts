export class AgentResponseDto {
  conversationId: number;
  response: string;
  toolCalls?: Array<{
    id: string;
    name: string;
    arguments: any;
    result?: any;
  }>;
}

