import { Request, Response, NextFunction } from 'express';
import { Logger } from '../../shared/logger';
import { logStore, setCorrelationId } from '../../shared/log-store';

const httpLogger = new Logger('HTTP');

interface RequestWithStartTime extends Request {
  startTime?: number;
  correlationId?: string;
}

function resolveCorrelationId(req: Request): string {
  const provided = req.headers['x-correlation-id'] as string;
  if (provided && provided.length <= 128) {
    return provided.replace(/[^\w\-.:]/g, '');
  }
  return `req_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
}

export function requestLoggingBegin(
  req: RequestWithStartTime,
  res: Response,
  next: NextFunction,
) {
  req.startTime = Date.now();
  const correlationId = resolveCorrelationId(req);
  req.correlationId = correlationId;
  res.setHeader('X-Correlation-ID', correlationId);

  logStore.run({ correlationId, context: {} }, () => {
    setCorrelationId(correlationId);
    next();
  });
}

export function requestLoggingEnd(
  req: RequestWithStartTime,
  res: Response,
  next: NextFunction,
) {
  const responseTime = req.startTime ? Date.now() - req.startTime : 0;
  const statusCode = res.statusCode;

  if (statusCode >= 400) {
    const data: Record<string, any> = {
      message: 'API Response',
      method: req.method,
      url: req.url,
      statusCode,
      responseTime: `${responseTime}ms`,
      userId: (req as any).user?.id,
    };
    if (Object.keys(req.query).length > 0) data.query = req.query;
    if (statusCode >= 500) {
      httpLogger.error(data);
    } else if (statusCode >= 400) {
      httpLogger.warn(data);
    }
  }

  next();
}
