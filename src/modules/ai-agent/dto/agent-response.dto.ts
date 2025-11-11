export class AgentResponseDto {
  conversation: number;
  response: string;
  toolCalls?: Array<{
    id: string;
    name: string;
    arguments: any;
    result?: any;
  }>;
}

