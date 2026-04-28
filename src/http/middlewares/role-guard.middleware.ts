import { Response, NextFunction } from 'express';
import { PolicyService, isPolicyDeny } from '../../domain/policy';
import { UnauthorizedException } from '../../domain/exceptions';

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

      res.status(403).json({
        statusCode: 403,
        message: (decision as any).message || 'Forbidden',
      });
    } catch (error) {
      next(error);
    }
  };
}
