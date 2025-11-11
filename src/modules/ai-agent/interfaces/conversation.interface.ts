export interface IConversation {
  id: string | number;
  userId?: string | number;
  title: string;
  messageCount: number;
  summary?: string;
  lastSummaryAt?: Date;
  lastActivityAt?: Date;
  createdAt: Date;
  updatedAt: Date;
}

export interface IConversationCreate {
  userId?: string | number;
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
  lastActivityAt?: Date;
}

