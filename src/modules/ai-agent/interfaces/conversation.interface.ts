export interface IConversation {
  id: string | number;
  userId?: string | number;
  configId: string | number;
  title: string;
  messageCount: number;
  summary?: string;
  lastSummaryAt?: Date;
  lastActivityAt?: Date;
  task?: {
    type: string;
    status: 'pending' | 'in_progress' | 'completed' | 'cancelled' | 'failed';
    priority?: number;
    data?: any;
    result?: any;
    error?: string;
    createdAt?: Date;
    updatedAt?: Date;
  } | null;
  createdAt: Date;
  updatedAt: Date;
}

export interface IConversationCreate {
  userId?: string | number;
  configId: string | number;
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
  task?: {
    type: string;
    status: 'pending' | 'in_progress' | 'completed' | 'cancelled' | 'failed';
    priority?: number;
    data?: any;
    result?: any;
    error?: string;
    createdAt?: Date;
    updatedAt?: Date;
  } | null;
}

