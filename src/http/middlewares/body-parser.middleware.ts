import express, {
  type Request,
  type Response,
  type NextFunction,
} from 'express';
import { SettingCacheService } from '../../engines/cache';
import { captureRawBody } from '../utils/raw-body-capture.util';

export function bodyParserMiddleware(settingCacheService: SettingCacheService) {
  return async (req: Request, res: Response, next: NextFunction) => {
    const contentType = req.headers['content-type'] || '';
    if (contentType.includes('multipart/')) return next();

    const limit = await settingCacheService.getMaxRequestBodySizeBytes();
    const limitStr = `${limit}`;

    if (contentType.includes('json')) {
      return express.json({ limit: limitStr, verify: captureRawBody })(
        req,
        res,
        next,
      );
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
