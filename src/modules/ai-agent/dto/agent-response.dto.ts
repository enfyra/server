export class AgentResponseDto {
  conversation: string | number;
  response: string;
  toolCalls?: Array<{
    id: string;
    name: string;
    arguments: any;
    result?: any;
  }>;
}

