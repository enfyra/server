import { Response, NextFunction } from 'express';
import { PolicyService, isPolicyDeny } from '../../domain/policy';
import { UnauthorizedException } from '../../domain/exceptions';
import { Logger } from '../../shared/logger';

const logger = new Logger('RoleGuard');

export function roleGuardMiddleware(policyService: PolicyService) {
  return async (req: any, res: Response, next: NextFunction) => {
    try {
      if (!req.routeData) {
        return next();
      }
      const decision = policyService.checkRequestAccess({
        method: req.method,
        routeData: req.routeData,
        user: req.user,
      });

      if (decision.allow) {
        return next();
      }

      if (isPolicyDeny(decision) && decision.statusCode === 401) {
        throw new UnauthorizedException();
      }

      const user = req.user;
      logger.warn({
        message: 'Route access denied',
        statusCode: 403,
        method: req.method,
        url: req.originalUrl || req.url,
        routeId: req.routeData?.id,
        routePath: req.routeData?.path,
        userId: user?.id,
        userEmail: user?.email,
        roleIds: Array.isArray(user?.roles)
          ? user.roles.map((role: any) => role?.id ?? role?._id).filter(Boolean)
          : [],
        decision,
      });

      res.status(403).json({
        statusCode: 403,
        message: (decision as any).message || 'Forbidden',
      });
    } catch (error) {
      next(error);
    }
  };
}
