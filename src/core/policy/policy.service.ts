import {
  TPolicyDecision,
  TPolicyMutationContext,
  TPolicyRequestContext,
  TPolicySchemaMigrationContext,
} from './policy.types';
import { SystemSafetyAuditorService } from './services/system-safety-auditor.service';
import { SchemaMigrationValidatorService } from './services/schema-migration-validator.service';

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

  checkRequestAccess(ctx: TPolicyRequestContext): TPolicyDecision {
    const isPublished = ctx.routeData?.publishedMethods?.some(
      (m: any) => m.method === ctx.method,
    );

    if (isPublished) return { allow: true };

    if (!ctx.user) {
      return {
        allow: false,
        statusCode: 401,
        code: 'UNAUTHORIZED',
        message: 'Unauthorized',
      };
    }

    const skipRoleGuard = ctx.routeData?.skipRoleGuardMethods?.some(
      (m: any) => m.method === ctx.method,
    );

    if (skipRoleGuard) return { allow: true };

    if (ctx.user.isRootAdmin) return { allow: true };

    if (!ctx.routeData?.routePermissions) {
      return {
        allow: false,
        statusCode: 403,
        code: 'FORBIDDEN',
        message: 'Forbidden',
      };
    }

    const userId = String(ctx.user._id || ctx.user.id);
    const userRoleId = ctx.user.role
      ? String(ctx.user.role._id || ctx.user.role.id)
      : null;

    const canPass = ctx.routeData.routePermissions.find((permission: any) => {
      const hasMethodAccess = permission.methods.some(
        (item: any) => item.method === ctx.method,
      );
      if (!hasMethodAccess) return false;
      if (
        permission?.allowedUsers?.some(
          (user: any) => String(user?._id || user?.id) === userId,
        )
      ) {
        return true;
      }
      if (!userRoleId) return false;
      const permRoleId = String(permission?.role?._id || permission?.role?.id);
      return permRoleId === userRoleId;
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
