import express, {
  type Request,
  type Response,
  type NextFunction,
} from 'express';
import type { RuntimeRegistryService } from '../../engines/cache/services/runtime-registry.service';
import { captureRawBody } from '../utils/raw-body-capture.util';

export function bodyParserMiddleware(
  runtimeRegistryService: RuntimeRegistryService,
) {
  return async (req: Request, res: Response, next: NextFunction) => {
    const contentType = req.headers['content-type'] || '';
    if (contentType.includes('multipart/')) return next();

    const limit = runtimeRegistryService.getMaxRequestBodySizeBytes();
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
