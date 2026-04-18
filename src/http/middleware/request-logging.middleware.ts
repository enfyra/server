import { Request, Response, NextFunction } from 'express';
import { winstonLogger } from '../../shared/utils/winston-logger';

const SLOW_REQUEST_THRESHOLD_MS = 2000;

interface RequestWithStartTime extends Request {
  startTime?: number;
  correlationId?: string;
}

function getCorrelationId(req: Request): string {
  const provided = req.headers['x-correlation-id'] as string;
  if (provided && provided.length <= 128) {
    return provided.replace(/[^\w\-.:]/g, '');
  }

  return `req_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
}

export function requestLoggingBegin(req: RequestWithStartTime, res: Response, next: NextFunction) {
  req.startTime = Date.now();
  const correlationId = getCorrelationId(req);
  req.correlationId = correlationId;

  res.setHeader('X-Correlation-ID', correlationId);

  next();
}

export function requestLoggingEnd(req: RequestWithStartTime, res: Response, next: NextFunction) {
  const responseTime = req.startTime ? Date.now() - req.startTime : 0;
  const statusCode = res.statusCode;

  if (statusCode >= 400 || responseTime > SLOW_REQUEST_THRESHOLD_MS) {
    const logData: any = {
      method: req.method,
      url: req.url,
      statusCode,
      responseTime: `${responseTime}ms`,
      userId: (req as any).user?.id,
    };

    if (Object.keys(req.query).length > 0) {
      logData.query = req.query;
    }

    winstonLogger.info('API Response', logData);
  }

  next();
}
