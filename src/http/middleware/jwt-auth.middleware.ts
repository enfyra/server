import { Response, NextFunction } from 'express';
import * as jwt from 'jsonwebtoken';
import { TokenExpiredException, InvalidTokenException } from '../../core/exceptions/custom-exceptions';
import { QueryBuilderService } from '../../infrastructure/query-builder/query-builder.service';
import { CacheService } from '../../infrastructure/cache/services/cache.service';
import {
  loadUserWithRole,
  userCacheKey,
  USER_CACHE_TTL_MS,
} from '../../shared/utils/load-user-with-role.util';

export function jwtAuthMiddleware(
  queryBuilderService: QueryBuilderService,
  cacheService: CacheService,
  secretKey: string,
) {
  return async (req: any, res: Response, next: NextFunction) => {
    try {
      const authHeader = req.headers.authorization;
      if (!authHeader || !authHeader.startsWith('Bearer ')) {
        req.user = null;
        if (req.routeData) {
          req.routeData.context.$user = null;
        }
        return next();
      }

      const token = authHeader.substring(7);
      let payload: any;

      try {
        payload = jwt.verify(token, secretKey);
      } catch (err: any) {
        if (err.name === 'TokenExpiredError') {
          throw new TokenExpiredException();
        }
        throw new InvalidTokenException();
      }

      if (!payload || !payload.id) {
        req.user = null;
        if (req.routeData) {
          req.routeData.context.$user = null;
        }
        return next();
      }

      const { id, loginProvider } = payload;
      const cacheKey = userCacheKey(id);

      let user = await cacheService.get<any>(cacheKey);

      if (!user) {
        user = await loadUserWithRole(queryBuilderService, id);
        if (user) {
          await cacheService.set(cacheKey, user, USER_CACHE_TTL_MS);
        }
      }

      if (!user) {
        req.user = null;
        if (req.routeData) {
          req.routeData.context.$user = null;
        }
        return next();
      }

      Object.assign(user, {
        loginProvider: loginProvider ?? null,
      });

      req.user = user;
      if (req.routeData) {
        req.routeData.context.$user = user;
      }

      next();
    } catch (error) {
      next(error);
    }
  };
}
