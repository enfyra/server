import {
  FieldPermissionCacheService,
  TFieldPermissionAction,
  TFieldPermissionRule,
} from '../../infrastructure/cache/services/field-permission-cache.service';
import { matchFieldPermissionCondition } from './field-permission-condition.util';

export type TFieldPermissionDecision = {
  allowed: boolean;
  reason?: string;
};

function toIdString(v: any): string | null {
  if (v === undefined || v === null) return null;
  return String(v?._id ?? v?.id ?? v);
}

function getUserId(user: any): string | null {
  if (!user || user.isAnonymous) return null;
  return toIdString(user);
}

function getRoleId(user: any): string | null {
  if (!user || user.isAnonymous) return null;
  return toIdString(user?.role);
}

type TFieldPermissionSubject = 'column' | 'relation';

export type TFieldPermissionContext = {
  user: any;
  tableName: string;
  action: TFieldPermissionAction;
  subjectType: TFieldPermissionSubject;
  subjectName: string;
  record?: any | null;
};

function bucketPriority(
  rule: TFieldPermissionRule,
  userId: string | null,
): 1 | 2 | 3 | 4 {
  const isUserSpecific = userId ? rule.allowedUserIds?.includes(userId) : false;
  const hasCondition = rule.condition != null;
  if (isUserSpecific && hasCondition) return 1;
  if (!isUserSpecific && hasCondition) return 2;
  if (isUserSpecific && !hasCondition) return 3;
  return 4;
}

function isRuleForSubject(
  rule: TFieldPermissionRule,
  ctx: TFieldPermissionContext,
): boolean {
  if (rule.tableName !== ctx.tableName) return false;
  if (rule.action !== ctx.action) return false;
  if (rule.isEnabled !== true) return false;

  if (ctx.subjectType === 'column') {
    return rule.columnName === ctx.subjectName;
  }
  return rule.relationPropertyName === ctx.subjectName;
}

function ruleAppliesToUser(rule: TFieldPermissionRule, user: any): boolean {
  const userId = getUserId(user);
  const roleId = getRoleId(user);

  const isUserSpecific =
    userId != null && rule.allowedUserIds?.includes(userId);
  const isRoleMatch =
    rule.roleId != null && roleId != null && rule.roleId === roleId;

  if (rule.allowedUserIds && rule.allowedUserIds.length > 0) {
    return isUserSpecific;
  }
  if (rule.roleId == null) {
    return true;
  }
  return isRoleMatch;
}

export async function decideFieldPermission(
  cache: FieldPermissionCacheService,
  ctx: TFieldPermissionContext,
  opt?: { defaultAllowed?: boolean },
): Promise<TFieldPermissionDecision> {
  const userId = getUserId(ctx.user);
  const defaultAllowed = opt?.defaultAllowed ?? true;

  const policies = await cache.getPoliciesFor(
    ctx.user,
    ctx.tableName,
    ctx.action,
  );
  if (policies.length === 0) return { allowed: defaultAllowed };

  const rules: TFieldPermissionRule[] = [];
  for (const p of policies) {
    for (const r of p.rules) {
      if (!isRuleForSubject(r, ctx)) continue;
      if (!ruleAppliesToUser(r, ctx.user)) continue;
      rules.push(r);
    }
  }
  if (rules.length === 0) return { allowed: defaultAllowed };

  const record = ctx.record ?? null;

  const byTier = new Map<number, TFieldPermissionRule[]>();
  for (const r of rules) {
    if (
      r.condition != null &&
      !matchFieldPermissionCondition(r.condition, record, ctx.user)
    ) {
      continue;
    }
    const tier = bucketPriority(r, userId);
    if (!byTier.has(tier)) byTier.set(tier, []);
    byTier.get(tier)!.push(r);
  }

  const tiers = Array.from(byTier.keys()).sort((a, b) => a - b);
  for (const tier of tiers) {
    const tierRules = byTier.get(tier)!;
    if (tierRules.some((r) => r.effect === 'deny')) {
      return {
        allowed: false,
        reason: `Denied by rule (tier ${tier})`,
      };
    }
    if (tierRules.some((r) => r.effect === 'allow')) {
      return { allowed: true };
    }
  }

  return { allowed: defaultAllowed };
}

export function formatFieldPermissionErrorMessage(opts: {
  action: 'read' | 'create' | 'update' | 'filter' | 'sort' | 'aggregate';
  tableName: string;
  fields: Array<{ type: 'column' | 'relation'; name: string }>;
}): string {
  const parts = opts.fields.map((f) =>
    f.type === 'column' ? `column '${f.name}'` : `relation '${f.name}'`,
  );
  const joined = parts.join(', ');
  return `You do not have permission to ${opts.action} ${joined} on table '${opts.tableName}'.`;
}
