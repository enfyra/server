export type FlowScheduleReconcileStatus =
  'idle' | 'running' | 'ok' | 'degraded';

export interface FlowScheduleReconcileState {
  status: FlowScheduleReconcileStatus;
  startedAt?: string;
  completedAt?: string;
  error?: string;
  registeredCount?: number;
}
