import type { NextFunction, Request, Response } from 'express';
import {
  type AuthenticationService,
  type AuthenticatedRequest,
} from '../../domain/auth';

function isPublicRequest(req: Request): boolean {
  if ((req as any).routeData?.isPublic === true) return true;
  return (
    (req as any).routeData?.publicMethods?.some(
      (method: any) => method?.name === req.method || method === req.method,
    ) === true
  );
}

function setAnonymousUser(req: Request): void {
  (req as any).user = null;
  (req as any).auth = null;
  const routeData = (req as any).routeData;
  if (routeData) routeData.context.$user = null;
}

export function authMiddleware(
  authenticationService: Pick<AuthenticationService, 'authenticate'>,
) {
  return async (req: Request, _res: Response, next: NextFunction) => {
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
  req: Request,
  authenticated: AuthenticatedRequest,
): void {
  (req as any).user = authenticated.user;
  (req as any).auth = authenticated;
  const routeData = (req as any).routeData;
  if (routeData) routeData.context.$user = authenticated.user;
}
