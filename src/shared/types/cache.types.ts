export interface TableMutationPayload {
  table: string;
  action: 'create' | 'update' | 'delete';
  ids?: (string | number)[];
  data?: unknown;
  userId?: string | number | null;
}

export interface RouteExecutedPayload {
  routePath: string;
  method: string;
  userId?: string | number | null;
  result?: unknown;
}

export interface TCacheInvalidationPayload {
  table: string;
  action: 'reload';
  timestamp: number;
  scope: 'full' | 'partial';
  ids?: (string | number)[];
  affectedTables?: string[];
  critical?: boolean;
  policyMetadataChanged?: boolean;
  tableRenames?: Array<{
    id: string | number;
    oldName: string;
    newName: string;
  }>;
}
