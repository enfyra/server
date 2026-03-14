export interface CompactFormat {
  fields: string[];
  data: any[][];
}

export interface HintContent {
  category: string;
  title: string;
  content: string;
  tools?: string[];
}

export interface BuildSystemPromptParams {
  provider: string;
  needsTools?: boolean;
  tablesList?: string;
  user?: {
    id?: string | number;
    _id?: string | number;
    email?: string;
    roles?: any;
    isRootAdmin?: boolean;
  };
  dbType?: 'postgres' | 'mysql' | 'mongodb' | 'sqlite';
  conversationId?: string | number;
  latestUserMessage?: string;
  conversationSummary?: string;
  task?: {
    type?: string;
    status?: string;
    priority?: number;
    data?: any;
    error?: string;
    result?: any;
  };
}
