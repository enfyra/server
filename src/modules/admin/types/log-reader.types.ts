export interface LogFile {
  name: string;
  size: number;
  createdAt: Date;
  lastModified: Date;
}

export interface ParsedLogEntry {
  id: string;
  timestamp: string;
  level: string;
  context?: string;
  correlationId?: string;
  message: string;
  data?: any;
  trace?: string;
  stack?: string;
}

export interface LogContent {
  file: string;
  lines: ParsedLogEntry[];
  rawLines?: string[];
  page: number;
  pageSize: number;
  hasMore: boolean;
}
