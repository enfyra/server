import { Injectable, Logger } from '@nestjs/common';
import { RateLimitService, RateLimitResult } from './rate-limit.service';
import { GuardNode, GuardRuleNode } from './guard-cache.service';

export interface GuardEvalContext {
  clientIp: string;
  routePath: string;
  userId?: string | null;
}

export interface GuardRejectInfo {
  guardName: string;
  ruleType: string;
  statusCode: number;
  message: string;
  headers?: Record<string, string>;
}

@Injectable()
export class GuardEvaluatorService {
  private readonly logger = new Logger(GuardEvaluatorService.name);

  constructor(private readonly rateLimitService: RateLimitService) {}

  async evaluateGuard(
    guard: GuardNode,
    evalCtx: GuardEvalContext,
  ): Promise<GuardRejectInfo | null> {
    return this.evaluateNode(guard, evalCtx, guard.name);
  }

  private readonly RULE_COST: Record<string, number> = {
    ip_whitelist: 0,
    ip_blacklist: 0,
    rate_limit_by_ip: 1,
    rate_limit_by_user: 1,
    rate_limit_by_route: 1,
  };

  private async evaluateNode(
    node: GuardNode,
    evalCtx: GuardEvalContext,
    rootName: string,
  ): Promise<GuardRejectInfo | null> {
    const items: Array<() => Promise<GuardRejectInfo | null>> = [];

    const sortedRules = [...node.rules].sort(
      (a, b) => (this.RULE_COST[a.type] ?? 0) - (this.RULE_COST[b.type] ?? 0),
    );

    for (const rule of sortedRules) {
      items.push(() => this.evaluateRule(rule, evalCtx, rootName));
    }

    for (const child of node.children) {
      if (!child.isEnabled) continue;
      items.push(() => this.evaluateNode(child, evalCtx, rootName));
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
  ): Promise<GuardRejectInfo | null> {
    if (rule.userIds.length > 0) {
      if (!evalCtx.userId || !rule.userIds.includes(evalCtx.userId)) {
        return null;
      }
    }

    switch (rule.type) {
      case 'rate_limit_by_ip':
        return this.evalRateLimit(
          `ip:${evalCtx.clientIp}:${evalCtx.routePath}`,
          rule,
          guardName,
        );
      case 'rate_limit_by_user':
        return this.evalRateLimit(
          `user:${evalCtx.userId || 'anonymous'}:${evalCtx.routePath}`,
          rule,
          guardName,
        );
      case 'rate_limit_by_route':
        return this.evalRateLimit(
          `route:${evalCtx.routePath}`,
          rule,
          guardName,
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
  ): Promise<GuardRejectInfo | null> {
    const { maxRequests, perSeconds } = rule.config || {};
    if (!maxRequests || !perSeconds) return null;

    const result: RateLimitResult = await this.rateLimitService.check(key, {
      maxRequests,
      perSeconds,
    });

    if (result.allowed) return null;

    return {
      guardName,
      ruleType: rule.type,
      statusCode: 429,
      message: 'Too Many Requests',
      headers: {
        'Retry-After': String(result.retryAfter),
        'X-RateLimit-Limit': String(result.limit),
        'X-RateLimit-Remaining': '0',
        'X-RateLimit-Reset': String(result.resetAt),
      },
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

    return {
      guardName,
      ruleType: rule.type,
      statusCode: 403,
      message: 'Forbidden',
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

    return {
      guardName,
      ruleType: rule.type,
      statusCode: 403,
      message: 'Forbidden',
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
