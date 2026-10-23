import type { NextFunction, Response } from 'express';
import type {
  AuthenticationService,
  AuthenticatedRequest,
} from '../../domain/auth';
import type { RuntimeRequest } from '../types/request-context.types';

function isPublicRequest(req: RuntimeRequest): boolean {
  return req.routeData?.routeMethodConfig?.isPublic === true;
}

function setAnonymousUser(req: RuntimeRequest): void {
  req.user = null;
  req.auth = null;
  const routeData = req.routeData;
  if (routeData) {
    routeData.context.$user = null;
    routeData.context.$authSource = undefined;
  }
}

export function authMiddleware(
  authenticationService: Pick<AuthenticationService, 'authenticate'>,
) {
  return async (req: RuntimeRequest, _res: Response, next: NextFunction) => {
    try {
      const authenticated = await authenticationService.authenticate({
        headers: req.headers,
        allowAnonymous: isPublicRequest(req),
      });

      if (!authenticated) {
        setAnonymousUser(req);
        next();
        return;
      }

      applyAuthenticatedUser(req, authenticated);
      next();
    } catch (error) {
      next(error);
    }
  };
}

function applyAuthenticatedUser(
  req: RuntimeRequest,
  authenticated: AuthenticatedRequest,
): void {
  req.user = authenticated.user;
  req.auth = authenticated;
  const routeData = req.routeData;
  if (routeData) {
    routeData.context.$user = authenticated.user;
    routeData.context.$authSource = authenticated.source;
  }
}
