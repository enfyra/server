export type RuntimeLogTable = 'enfyra_system_error' | 'enfyra_user_log';

export type RuntimeLogMetadata = Record<string, unknown>;

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

export interface PersistedRuntimeLogRecord extends RuntimeLogMetadata {
  eventId: string;
  occurredAt: Date;
  createdAt: Date;
  updatedAt: Date;
}

export interface PendingRuntimeLog {
  table: RuntimeLogTable;
  record: RuntimeLogRecord;
  bytes: number;
}
