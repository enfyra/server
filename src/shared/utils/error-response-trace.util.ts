import type { Request, Response } from 'express';

function generateCorrelationId(): string {
  return `req_${Date.now()}_${Math.random().toString(36).slice(2, 11)}`;
}

function getResponseCorrelationId(res?: Pick<Response, 'getHeader'> | null): string | undefined {
  if (!res || typeof res.getHeader !== 'function') return undefined;
  const value = res.getHeader('X-Correlation-ID');
  if (Array.isArray(value)) return value[0] ? String(value[0]) : undefined;
  return value === undefined ? undefined : String(value);
}

export function buildErrorResponseTrace(
  req?: Request,
  res?: Pick<Response, 'getHeader'> | null,
) {
  const request = req as (Request & {
    correlationId?: string;
    routeData?: {
      context?: {
        $api?: {
          request?: {
            method?: string;
            url?: string;
            correlationId?: string;
          };
        };
      };
    };
  }) | undefined;
  const apiRequest = request?.routeData?.context?.$api?.request;
  const rawPath = request?.originalUrl || request?.url || apiRequest?.url || '';
  const correlationId = request?.correlationId
    || getResponseCorrelationId(res)
    || apiRequest?.correlationId
    || generateCorrelationId();

  return {
    timestamp: new Date().toISOString(),
    path: rawPath.split(/[?#]/u, 1)[0] || '/',
    method: request?.method || apiRequest?.method || 'UNKNOWN',
    correlationId,
  };
}
