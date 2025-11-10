export interface IConversation {
  id: number;
  userId?: number;
  title: string;
  messageCount: number;
  summary?: string;
  lastSummaryAt?: Date;
  createdAt: Date;
  updatedAt: Date;
}

export interface IConversationCreate {
  userId?: number;
  title: string;
  messageCount?: number;
  summary?: string;
  lastSummaryAt?: Date;
}

export interface IConversationUpdate {
  title?: string;
  messageCount?: number;
  summary?: string;
  lastSummaryAt?: Date;
}

