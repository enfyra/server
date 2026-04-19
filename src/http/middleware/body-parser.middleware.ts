import { Request, Response, NextFunction } from 'express';
import * as express from 'express';
import { SettingCacheService } from '../../infrastructure/cache/services/setting-cache.service';

export function bodyParserMiddleware(settingCacheService: SettingCacheService) {
  return (req: Request, res: Response, next: NextFunction) => {
    const contentType = req.headers['content-type'] || '';
    if (contentType.includes('multipart/')) return next();

    const limit = settingCacheService.getMaxRequestBodySizeBytes();
    const limitStr = `${limit}`;

    if (contentType.includes('json')) {
      return express.json({ limit: limitStr })(req, res, next);
    }
    if (contentType.includes('urlencoded')) {
      return express.urlencoded({ limit: limitStr, extended: true })(
        req,
        res,
        next,
      );
    }
    next();
  };
}
