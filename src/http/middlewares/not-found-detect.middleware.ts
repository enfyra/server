import type { NextFunction, Response } from 'express';
import { NotFoundException } from '../../domain/exceptions';

export function notFoundDetectMiddleware(
  req: any,
  _res: Response,
  next: NextFunction,
) {
  if (!req.routeData) {
    req.routeNotFound = true;
    if (!/^\/graphql\/?$/i.test(req.path)) {
      return next(new NotFoundException());
    }
  }
  next();
}
