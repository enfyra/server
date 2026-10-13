export type DynamicDeliveryStage =
  | 'dispatch_start'
  | 'upstream_headers'
  | 'upstream_first_frame'
  | 'upstream_terminal'
  | 'upstream_usage'
  | 'upstream_done'
  | 'upstream_eof'
  | 'upstream_read_error'
  | 'protocol_error'
  | 'public_terminal_ready'
  | 'public_done_ready'
  | 'settlement_start'
  | 'settlement_committed';

export interface DynamicDeliveryFields {
  statusCode?: number;
  bytes?: number;
}

export type DynamicDeliveryRecorder = (
  stage: DynamicDeliveryStage,
  fields?: DynamicDeliveryFields,
) => void;
