import type { Response, NextFunction } from 'express';
import type { PolicyService } from '../../domain/policy';
import type { RuntimeRequest } from '../types/request-context.types';
import { UnauthorizedException } from '../../domain/exceptions';
import { Logger } from '../../shared/logger';

const logger = new Logger('RoleGuard');

export function roleGuardMiddleware(policyService: PolicyService) {
  return async (req: RuntimeRequest, res: Response, next: NextFunction) => {
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

      if (decision.statusCode === 401) {
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
          ? user.roles.map((role) =>
              typeof role === 'object' ? role?.id ?? role?._id : undefined,
            ).filter(Boolean)
          : [],
        decision,
      });

      res.status(403).json({
        statusCode: 403,
        message: decision.message || 'Forbidden',
      });
    } catch (error) {
      next(error);
    }
  };
}
