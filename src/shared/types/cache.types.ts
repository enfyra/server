export interface TCacheInvalidationPayload {
  table: string;
  action: 'reload';
  timestamp: number;
  scope: 'full' | 'partial';
  ids?: (string | number)[];
  affectedTables?: string[];
}

export interface TTableHandlerResult {
  id: string | number;
  name?: string;
  affectedTables?: string[];
  [key: string]: any;
}
