import { Response, NextFunction } from 'express';
import { jwtVerify } from 'jose';
import {
  TokenExpiredException,
  InvalidTokenException,
} from '../../domain/exceptions';
import { QueryBuilderService } from '@enfyra/kernel';
import { CacheService } from '../../engines/cache';
import {
  loadUserWithRole,
  userCacheKey,
  USER_CACHE_TTL_MS,
} from '../../shared/utils/load-user-with-role.util';

function isPublishedRequest(req: any): boolean {
  if (req.routeData?.isPublished === true) return true;
  return (
    req.routeData?.publishedMethods?.some(
      (method: any) => method?.method === req.method || method === req.method,
    ) === true
  );
}

function setAnonymousUser(req: any): void {
  req.user = null;
  if (req.routeData) {
    req.routeData.context.$user = null;
  }
}

export function jwtAuthMiddleware(
  queryBuilderService: QueryBuilderService,
  cacheService: CacheService,
  secretKey: string,
) {
  const key = new TextEncoder().encode(secretKey);

  return async (req: any, res: Response, next: NextFunction) => {
    try {
      const authHeader = req.headers.authorization;
      if (!authHeader || !authHeader.startsWith('Bearer ')) {
        setAnonymousUser(req);
        return next();
      }

      const token = authHeader.substring(7);
      let payload: any;

      try {
        const { payload: decoded } = await jwtVerify(token, key);
        payload = decoded;
      } catch (err: any) {
        if (isPublishedRequest(req)) {
          setAnonymousUser(req);
          return next();
        }
        if (err.code === 'ERR_JWT_EXPIRED') {
          throw new TokenExpiredException();
        }
        throw new InvalidTokenException();
      }

      if (!payload || !payload.id) {
        setAnonymousUser(req);
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
        setAnonymousUser(req);
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
