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
import type { ApiTokenService } from '../../domain/auth';

function isPublicRequest(req: any): boolean {
  if (req.routeData?.isPublic === true) return true;
  return (
    req.routeData?.publicMethods?.some(
      (method: any) => method?.name === req.method || method === req.method,
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
  apiTokenService?: ApiTokenService,
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
        if (isPublicRequest(req)) {
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

      if (
        payload.tokenType === 'api_token' &&
        !(await apiTokenService?.validateAccessPayload(payload))
      ) {
        throw new InvalidTokenException();
      }

      const { id, loginProvider, tokenType, tokenId } = payload;
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
        tokenType: tokenType ?? null,
        apiTokenId: tokenId ?? null,
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
