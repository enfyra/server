export type ErrorType =
  | 'TIMEOUT'
  | 'RATE_LIMIT'
  | 'PERMISSION_DENIED'
  | 'RESOURCE_NOT_FOUND'
  | 'RESOURCE_EXISTS'
  | 'INVALID_INPUT'
  | 'NETWORK_ERROR'
  | 'SERVER_ERROR'
  | 'UNKNOWN_ERROR';

export type RecoveryAction =
  | 'retry'
  | 'ask_user'
  | 'escalate_to_user'
  | 'ask_clarification'
  | 'wait_and_retry'
  | 'fail';

export interface RecoveryStrategy {
  maxRetries: number;
  backoffMs?: number[];
  fallback: RecoveryAction;
  message: string;
  suggestion?: string;
}

export interface RecoveryResult {
  action: RecoveryAction;
  maxRetries: number;
  backoffMs?: number[];
  message: string;
  suggestion?: string;
  errorType: ErrorType;
}

export interface EscalationTrigger {
  shouldEscalate: boolean;
  reason?: string;
  context?: string;
  suggestedActions?: string[];
}
