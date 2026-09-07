export type RuntimeLogTable = 'enfyra_system_error' | 'enfyra_user_log';

export interface RuntimeLogRecord {
  eventId: string;
  occurredAt: string;
  correlationId: string | null;
  instanceId: string | null;
  component: string;
  sourceKind: string | null;
  sourceId: string | null;
  statusCode: number | null;
  [key: string]: unknown;
}

export interface PendingRuntimeLog {
  table: RuntimeLogTable;
  record: RuntimeLogRecord;
  bytes: number;
}
