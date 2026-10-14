export type DynamicDeliveryStage =
  | 'dispatch_start'
  | 'upstream_headers'
  | 'upstream_body_first_byte'
  | 'upstream_body_progress'
  | 'upstream_body_end'
  | 'buffered_preflight_start'
  | 'buffered_preflight_end'
  | 'buffered_read_start'
  | 'buffered_read_end'
  | 'frame_inspection_start'
  | 'frame_inspection_end'
  | 'transform_start'
  | 'transform_end'
  | 'transform_error'
  | 'public_relay_start'
  | 'public_relay_end'
  | 'upstream_first_frame'
  | 'upstream_terminal'
  | 'upstream_usage'
  | 'upstream_done'
  | 'upstream_eof'
  | 'upstream_protocol_complete'
  | 'upstream_read_error'
  | 'protocol_error'
  | 'public_terminal_ready'
  | 'public_done_ready'
  | 'settlement_start'
  | 'settlement_committed'
  | 'settlement_metadata_start'
  | 'settlement_metadata_end'
  | 'settlement_lease_start'
  | 'settlement_lease_acquired'
  | 'settlement_transaction_start'
  | 'settlement_user_lock_start'
  | 'settlement_user_lock_acquired'
  | 'settlement_publication_start'
  | 'settlement_publication_end'
  | 'settlement_lease_release_start'
  | 'settlement_lease_release_end';

export interface DynamicDeliveryFields {
  statusCode?: number;
  bytes?: number;
  chunks?: number;
  clientStreaming?: boolean;
  upstreamStreaming?: boolean;
  protocol?: 'chat' | 'responses' | 'messages';
  contentKind?: 'sse' | 'json' | 'other';
  errorCode?: string;
}

export type DynamicDeliveryRecorder = (
  stage: DynamicDeliveryStage,
  fields?: DynamicDeliveryFields,
) => void;
