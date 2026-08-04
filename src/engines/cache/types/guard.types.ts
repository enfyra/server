import type { GraphqlOperationName } from '../../../shared/types/graphql.types';

export type GuardRuleType =
  | 'rate_limit_by_ip'
  | 'rate_limit_by_user'
  | 'rate_limit_by_route'
  | 'rate_limit_by_operation'
  | 'ip_whitelist'
  | 'ip_blacklist';

export type GuardPosition = 'pre_auth' | 'post_auth';
export type GuardCombinator = 'and' | 'or';
export type GuardTargetType = 'route' | 'graphql';

export interface GuardRuleNode {
  id: number;
  type: GuardRuleType;
  config: any;
  priority: number;
  isEnabled: boolean;
  userIds: string[];
}

export interface GuardNode {
  id: number;
  name: string;
  position: GuardPosition | null;
  combinator: GuardCombinator;
  priority: number;
  isEnabled: boolean;
  isGlobal: boolean;
  type: GuardTargetType;
  gqlOperation: GraphqlOperationName | null;
  tableName: string | null;
  parentId: number | null;
  routeId: number | null;
  routePath: string | null;
  methodIds: number[];
  methods: string[];
  children: GuardNode[];
  rules: GuardRuleNode[];
}

export interface GuardCache {
  preAuthGlobal: GuardNode[];
  postAuthGlobal: GuardNode[];
  preAuthByRoute: Map<string, GuardNode[]>;
  postAuthByRoute: Map<string, GuardNode[]>;
  gqlPreAuthGlobal: GuardNode[];
  gqlPostAuthGlobal: GuardNode[];
  gqlPreAuthByTable: Map<string, GuardNode[]>;
  gqlPostAuthByTable: Map<string, GuardNode[]>;
  gqlPreAuthByOperation: Map<string, GuardNode[]>;
  gqlPostAuthByOperation: Map<string, GuardNode[]>;
  gqlPreAuthExact: Map<string, GuardNode[]>;
  gqlPostAuthExact: Map<string, GuardNode[]>;
}

export interface GuardEvalContext {
  clientIp: string;
  routePath: string;
  userId?: string | null;
  /** GraphQL-only: tableName targeted by the operation (null for global guards). */
  tableName?: string | null;
  /** GraphQL-only: operation name QUERY/CREATE/UPDATE/DELETE. */
  operation?: GraphqlOperationName | null;
  /** REST vs GraphQL. Only set for GraphQL evaluation. */
  targetType?: 'route' | 'graphql';
}

export type GuardRejectReason = 'rate_limit' | 'ip_not_allowed' | 'ip_blocked';
export type GuardRateLimitScope = 'ip' | 'user' | 'route' | 'operation';

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

export type GuardRejectDetails = GuardRateLimitDetails | GuardIpRejectDetails;

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
