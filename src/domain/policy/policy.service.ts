import { ObjectId } from 'mongodb';
import type {
  TPolicyDecision,
  TPolicyMutationContext,
  TPolicyRequestContext,
  TPolicyRequestDecision,
  TPolicySchemaMigrationContext,
} from './policy.types';
import type { RequestPolicyPermission } from './types/request-policy.types';
import type { TUserRoleReference } from '../../shared/types/authenticated-user.types';
import { SystemSafetyAuditorService } from './services/system-safety-auditor.service';
import { SchemaMigrationValidatorService } from './services/schema-migration-validator.service';
import { userHasRole } from '../../shared/utils/user-role.util';

export class PolicyService {
  private readonly systemSafetyAuditorService: SystemSafetyAuditorService;
  private readonly schemaMigrationValidatorService: SchemaMigrationValidatorService;

  constructor(deps: {
    systemSafetyAuditorService: SystemSafetyAuditorService;
    schemaMigrationValidatorService: SchemaMigrationValidatorService;
  }) {
    this.systemSafetyAuditorService = deps.systemSafetyAuditorService;
    this.schemaMigrationValidatorService = deps.schemaMigrationValidatorService;
  }

  checkRequestAccess(ctx: TPolicyRequestContext): TPolicyRequestDecision {
    if (ctx.routeData?.routeMethodConfig?.isPublic === true) return { allow: true };

    if (!ctx.user) {
      return {
        allow: false,
        statusCode: 401,
        code: 'UNAUTHORIZED',
        message: 'Unauthorized',
      };
    }

    if (ctx.routeData?.routeMethodConfig?.skipRoleGuard === true) {
      return { allow: true };
    }

    if (ctx.user.isRootAdmin) return { allow: true };

    const permissions = ctx.routeData?.routePermissions;
    if (!Array.isArray(permissions)) {
      return {
        allow: false,
        statusCode: 403,
        code: 'FORBIDDEN',
        message: 'Forbidden',
      };
    }

    const userId = String(ctx.user._id || ctx.user.id);
    const canPass = permissions.find((permission: unknown) => {
      if (!isPermission(permission) || permission.isEnabled === false) return false;
      if (
        Array.isArray(permission.allowedUsers) &&
        permission.allowedUsers.some(
          (user: unknown) => getPermissionUserId(user) === userId,
        )
      ) {
        return true;
      }
      return isRoleReference(permission.role) && userHasRole(ctx.user, permission.role);
    });

    if (canPass) return { allow: true };

    return {
      allow: false,
      statusCode: 403,
      code: 'FORBIDDEN',
      message: 'Forbidden',
    };
  }

  async checkMutationSafety(
    ctx: TPolicyMutationContext,
  ): Promise<TPolicyDecision> {
    try {
      await this.systemSafetyAuditorService.assertSystemSafe(ctx);
      return { allow: true };
    } catch (error: any) {
      return {
        allow: false,
        statusCode: 403,
        code: 'SYSTEM_PROTECTION',
        message: error?.message || 'Forbidden',
      };
    }
  }

  async checkSchemaMigration(
    ctx: TPolicySchemaMigrationContext,
  ): Promise<TPolicyDecision> {
    return this.schemaMigrationValidatorService.checkSchemaMigration(ctx);
  }
}

function isPermission(value: unknown): value is RequestPolicyPermission {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function isIdentity(value: unknown): boolean {
  return typeof value === 'string' || typeof value === 'number' || value instanceof ObjectId;
}

function isRoleReference(value: unknown): value is TUserRoleReference {
  if (typeof value === 'string' || typeof value === 'number') return true;
  if (!value || typeof value !== 'object' || Array.isArray(value)) return false;
  const id = '_id' in value ? value._id : undefined;
  const fallbackId = 'id' in value ? value.id : undefined;
  return (id == null || isIdentity(id)) && (fallbackId == null || isIdentity(fallbackId));
}

function getPermissionUserId(value: unknown): string | null {
  if (!isRoleReference(value) || typeof value !== 'object') return null;
  const id = value._id || value.id;
  return isIdentity(id) ? String(id) : null;
}
