export interface GuardEvalContext {
  clientIp: string;
  routePath: string;
  userId?: string | null;
}

export type GuardRejectReason = 'rate_limit' | 'ip_not_allowed' | 'ip_blocked';
export type GuardRateLimitScope = 'ip' | 'user' | 'route';

export interface GuardRateLimitDetails {
  reason: 'rate_limit';
  scope: GuardRateLimitScope;
  limit: number;
  remaining: number;
  windowSeconds: number;
  retryAfterSeconds: number;
  resetAt: number;
}

export interface GuardIpRejectDetails {
  reason: Exclude<GuardRejectReason, 'rate_limit'>;
}

export type GuardRejectDetails =
  | GuardRateLimitDetails
  | GuardIpRejectDetails;

export interface GuardRejectInfo {
  guardName: string;
  ruleType: string;
  statusCode: number;
  errorCode: string;
  message: string;
  details: GuardRejectDetails;
  headers?: Record<string, string>;
}

export interface GuardRateLimitSnapshot {
  ruleId: number | string;
  scope: GuardRateLimitScope;
  limit: number;
  remaining: number;
  windowSeconds: number;
  resetAt: number;
}

export interface GuardEvaluationResult {
  reject: GuardRejectInfo | null;
  rateLimitSnapshots: GuardRateLimitSnapshot[];
}

export interface GuardAlertInput {
  scope: GuardRateLimitScope;
  scopeKey: string;
  routePath: string;
  method: string;
  errorCode: string;
  guardName: string;
}
