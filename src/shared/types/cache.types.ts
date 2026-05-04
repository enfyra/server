export interface TCacheInvalidationPayload {
  table: string;
  action: 'reload';
  timestamp: number;
  scope: 'full' | 'partial';
  ids?: (string | number)[];
  affectedTables?: string[];
  tableRenames?: Array<{
    id: string | number;
    oldName: string;
    newName: string;
  }>;
}

export interface TTableHandlerResult {
  id: string | number;
  name?: string;
  affectedTables?: string[];
  tableRenames?: TCacheInvalidationPayload['tableRenames'];
  [key: string]: any;
}
