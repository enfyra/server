import { Logger } from '../../../shared/logger';
import { RateLimitService, RateLimitResult } from './rate-limit.service';
import type {
  GuardNode,
  GuardRuleNode,
  GuardEvalContext,
  GuardRateLimitScope,
  GuardRateLimitDetails,
  GuardIpRejectDetails,
  GuardRejectDetails,
  GuardRejectInfo,
  GuardRateLimitSnapshot,
  GuardEvaluationResult,
} from '../types/guard.types';

export type {
  GuardEvalContext,
  GuardRejectReason,
  GuardRateLimitScope,
  GuardRateLimitDetails,
  GuardIpRejectDetails,
  GuardRejectDetails,
  GuardRejectInfo,
  GuardRateLimitSnapshot,
  GuardEvaluationResult,
} from '../types/guard.types';

const RATE_LIMIT_SCOPE_BY_RULE: Record<string, GuardRateLimitScope> = {
  rate_limit_by_ip: 'ip',
  rate_limit_by_user: 'user',
  rate_limit_by_route: 'route',
  rate_limit_by_operation: 'operation',
};

function buildGuardHeaders(
  details: GuardRejectDetails,
): Record<string, string> {
  const headers: Record<string, string> = {
    'X-Enfyra-Guard-Reason': details.reason,
    'X-Enfyra-Guard-Error-Code': errorCodeForDetails(details),
  };
  if (details.reason === 'rate_limit') {
    headers['Retry-After'] = String(details.retryAfterSeconds);
    headers['X-RateLimit-Limit'] = String(details.limit);
    headers['X-RateLimit-Remaining'] = String(details.remaining);
    headers['X-RateLimit-Reset'] = String(details.resetAt);
    headers['X-RateLimit-Window'] = String(details.windowSeconds);
    headers['X-RateLimit-Scope'] = details.scope;
    headers['X-RateLimit-Used'] = String(
      Math.max(0, details.limit - details.remaining),
    );
    headers['X-Enfyra-Guard-Scope'] = details.scope;
  }
  return headers;
}

function errorCodeForDetails(details: GuardRejectDetails): string {
  switch (details.reason) {
    case 'rate_limit':
      return 'RATE_LIMIT_EXCEEDED';
    case 'ip_not_allowed':
      return 'IP_NOT_ALLOWED';
    case 'ip_blocked':
      return 'IP_BLOCKED';
  }
}

export class GuardEvaluatorService {
  private readonly logger = new Logger(GuardEvaluatorService.name);
  private readonly rateLimitService: RateLimitService;

  constructor(deps: { rateLimitService: RateLimitService }) {
    this.rateLimitService = deps.rateLimitService;
  }

  async evaluateGuard(
    guard: GuardNode,
    evalCtx: GuardEvalContext,
  ): Promise<GuardEvaluationResult> {
    const rateLimitSnapshots: GuardRateLimitSnapshot[] = [];
    const reject = await this.evaluateNode(
      guard,
      evalCtx,
      guard.name,
      rateLimitSnapshots,
      guard.tableName,
    );
    return { reject, rateLimitSnapshots };
  }

  private readonly RULE_COST: Record<string, number> = {
    ip_whitelist: 0,
    ip_blacklist: 0,
    rate_limit_by_ip: 1,
    rate_limit_by_user: 1,
    rate_limit_by_route: 1,
    rate_limit_by_operation: 1,
  };

  private async evaluateNode(
    node: GuardNode,
    evalCtx: GuardEvalContext,
    rootName: string,
    rateLimitSnapshots: GuardRateLimitSnapshot[],
    targetTableName: string | null,
  ): Promise<GuardRejectInfo | null> {
    const items: Array<() => Promise<GuardRejectInfo | null>> = [];

    const sortedRules = [...node.rules].sort(
      (a, b) => (this.RULE_COST[a.type] ?? 0) - (this.RULE_COST[b.type] ?? 0),
    );

    for (const rule of sortedRules) {
      items.push(() =>
        this.evaluateRule(
          rule,
          evalCtx,
          rootName,
          rateLimitSnapshots,
          targetTableName,
        ),
      );
    }

    for (const child of node.children) {
      if (!child.isEnabled) continue;
      items.push(() =>
        this.evaluateNode(
          child,
          evalCtx,
          rootName,
          rateLimitSnapshots,
          targetTableName,
        ),
      );
    }

    if (items.length === 0) return null;

    if (node.combinator === 'and') {
      for (const item of items) {
        const reject = await item();
        if (reject) return reject;
      }
      return null;
    } else {
      let lastReject: GuardRejectInfo | null = null;
      for (const item of items) {
        const reject = await item();
        if (!reject) return null;
        lastReject = reject;
      }
      return lastReject;
    }
  }

  private async evaluateRule(
    rule: GuardRuleNode,
    evalCtx: GuardEvalContext,
    guardName: string,
    rateLimitSnapshots: GuardRateLimitSnapshot[],
    targetTableName: string | null,
  ): Promise<GuardRejectInfo | null> {
    if (rule.userIds.length > 0) {
      if (!evalCtx.userId || !rule.userIds.includes(evalCtx.userId)) {
        return null;
      }
    }

    switch (rule.type) {
      case 'rate_limit_by_ip':
        return this.evalRateLimit(
          `guard_rule:${rule.id}:ip:${evalCtx.clientIp}`,
          rule,
          guardName,
          rateLimitSnapshots,
        );
      case 'rate_limit_by_user':
        return this.evalRateLimit(
          `guard_rule:${rule.id}:user:${evalCtx.userId || 'anonymous'}`,
          rule,
          guardName,
          rateLimitSnapshots,
        );
      case 'rate_limit_by_route':
        return this.evalRateLimit(
          `guard_rule:${rule.id}:route:${evalCtx.routePath}`,
          rule,
          guardName,
          rateLimitSnapshots,
        );
      case 'rate_limit_by_operation':
        return this.evalRateLimit(
          `guard_rule:${rule.id}:operation:${targetTableName || '*'}:${evalCtx.operation || '*'}`,
          rule,
          guardName,
          rateLimitSnapshots,
        );
      case 'ip_whitelist':
        return this.evalIpWhitelist(evalCtx.clientIp, rule, guardName);
      case 'ip_blacklist':
        return this.evalIpBlacklist(evalCtx.clientIp, rule, guardName);
      default:
        return null;
    }
  }

  private async evalRateLimit(
    key: string,
    rule: GuardRuleNode,
    guardName: string,
    rateLimitSnapshots: GuardRateLimitSnapshot[],
  ): Promise<GuardRejectInfo | null> {
    const { maxRequests, perSeconds } = rule.config || {};
    if (!maxRequests || !perSeconds) return null;

    const result: RateLimitResult = await this.rateLimitService.check(key, {
      maxRequests,
      perSeconds,
    });

    const scope = RATE_LIMIT_SCOPE_BY_RULE[rule.type] ?? 'ip';
    rateLimitSnapshots.push({
      ruleId: rule.id,
      scope,
      limit: result.limit,
      remaining: result.remaining,
      windowSeconds: result.window,
      resetAt: result.resetAt,
    });

    if (result.allowed) return null;

    const details: GuardRateLimitDetails = {
      reason: 'rate_limit',
      scope,
      limit: result.limit,
      remaining: result.remaining,
      windowSeconds: result.window,
      retryAfterSeconds: result.retryAfter,
      resetAt: result.resetAt,
    };

    return {
      guardName,
      ruleType: rule.type,
      statusCode: 429,
      errorCode: 'RATE_LIMIT_EXCEEDED',
      message: 'Too Many Requests',
      details,
      headers: buildGuardHeaders(details),
    };
  }

  private evalIpWhitelist(
    clientIp: string,
    rule: GuardRuleNode,
    guardName: string,
  ): GuardRejectInfo | null {
    const ips: string[] = rule.config?.ips || [];
    if (ips.length === 0) return null;

    if (this.matchIp(clientIp, ips)) return null;

    const details: GuardIpRejectDetails = {
      reason: 'ip_not_allowed',
    };
    return {
      guardName,
      ruleType: rule.type,
      statusCode: 403,
      errorCode: 'IP_NOT_ALLOWED',
      message: 'Forbidden',
      details,
      headers: buildGuardHeaders(details),
    };
  }

  private evalIpBlacklist(
    clientIp: string,
    rule: GuardRuleNode,
    guardName: string,
  ): GuardRejectInfo | null {
    const ips: string[] = rule.config?.ips || [];
    if (ips.length === 0) return null;

    if (!this.matchIp(clientIp, ips)) return null;

    const details: GuardIpRejectDetails = {
      reason: 'ip_blocked',
    };
    return {
      guardName,
      ruleType: rule.type,
      statusCode: 403,
      errorCode: 'IP_BLOCKED',
      message: 'Forbidden',
      details,
      headers: buildGuardHeaders(details),
    };
  }

  private normalizeIp(ip: string): string {
    if (ip.startsWith('::ffff:')) {
      const v4 = ip.slice(7);
      if (this.ipToNum(v4) !== null) return v4;
    }
    return ip;
  }

  private matchIp(clientIp: string, patterns: string[]): boolean {
    const normalized = this.normalizeIp(clientIp);
    for (const pattern of patterns) {
      const normalizedPattern = this.normalizeIp(pattern);
      if (normalizedPattern.includes('/')) {
        if (this.matchCidr(normalized, normalizedPattern)) return true;
      } else {
        if (normalized === normalizedPattern) return true;
      }
    }
    return false;
  }

  private matchCidr(ip: string, cidr: string): boolean {
    const [range, bitsStr] = cidr.split('/');
    const bits = parseInt(bitsStr, 10);
    if (isNaN(bits) || bits < 0 || bits > 32) return false;

    const ipNum = this.ipToNum(ip);
    const rangeNum = this.ipToNum(range);
    if (ipNum === null || rangeNum === null) return false;

    const mask = bits === 0 ? 0 : (~0 << (32 - bits)) >>> 0;
    return (ipNum & mask) === (rangeNum & mask);
  }

  private ipToNum(ip: string): number | null {
    const parts = ip.split('.');
    if (parts.length !== 4) return null;
    let num = 0;
    for (const part of parts) {
      const n = parseInt(part, 10);
      if (isNaN(n) || n < 0 || n > 255) return null;
      num = (num << 8) | n;
    }
    return num >>> 0;
  }
}
