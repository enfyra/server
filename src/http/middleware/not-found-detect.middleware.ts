import { Request, Response, NextFunction } from 'express';

export function notFoundDetectMiddleware(req: any, res: Response, next: NextFunction) {
  if (!req.routeData) {
    req.routeNotFound = true;
  }
  next();
}
