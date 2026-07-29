export interface TCacheInvalidationPayload {
  table: string;
  action: 'reload';
  timestamp: number;
  scope: 'full' | 'partial';
  ids?: (string | number)[];
  affectedTables?: string[];
  critical?: boolean;
  tableRenames?: Array<{
    id: string | number;
    oldName: string;
    newName: string;
  }>;
}
