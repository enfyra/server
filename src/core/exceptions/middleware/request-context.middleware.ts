import { Injectable, NestMiddleware } from '@nestjs/common';
import { Request, Response, NextFunction } from 'express';
import { LoggingService } from '../services/logging.service';

@Injectable()
export class RequestContextMiddleware implements NestMiddleware {
  constructor(private readonly loggingService: LoggingService) {}

  use(req: Request, res: Response, next: NextFunction): void {
    const startTime = Date.now();

    // Generate or extract correlation ID
    const correlationId = this.getCorrelationId(req);

    // Set up logging context
    this.loggingService.setCorrelationId(correlationId);
    this.loggingService.setContext({
      method: req.method,
      url: req.url,
      userAgent: req.headers['user-agent'],
      ip: req.ip || req.connection.remoteAddress,
      userId: (req as any).user?.id,
    });

    // Log incoming request
    this.loggingService.logRequest(req.method, req.url, (req as any).user?.id, {
      query: req.query,
      body: this.sanitizeBody(req.body),
      headers: this.sanitizeHeaders(req.headers),
    });

    // Override response.end to log response
    const originalEnd = res.end;
    const self = this;
    res.end = function (chunk?: any, encoding?: any, cb?: any) {
      const responseTime = Date.now() - startTime;

      // Log response
      self.loggingService.logResponse(
        req.method,
        req.url,
        res.statusCode,
        responseTime,
        (req as any).user?.id,
      );

      // Clear context after response
      self.loggingService.clearContext();

      // Call original end method
      return originalEnd.call(this, chunk, encoding, cb);
    };

    // Add correlation ID to response headers
    res.setHeader('X-Correlation-ID', correlationId);

    // Add correlation ID to request for internal use
    (req as any).correlationId = correlationId;

    next();
  }

  /**
   * Get correlation ID from headers or generate new one
   */
  private getCorrelationId(req: Request): string {
    // Check if correlation ID is provided in headers
    const providedCorrelationId = req.headers['x-correlation-id'] as string;
    if (providedCorrelationId) {
      return providedCorrelationId;
    }

    // Generate new correlation ID
    return `req_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  }

  /**
   * Sanitize request body for logging (remove sensitive data)
   */
  private sanitizeBody(body: any): any {
    if (!body) return body;

    const sanitized = { ...body };
    const sensitiveFields = [
      'password',
      'token',
      'secret',
      'key',
      'authorization',
    ];

    sensitiveFields.forEach((field) => {
      if (sanitized[field]) {
        sanitized[field] = '[REDACTED]';
      }
    });

    return sanitized;
  }

  /**
   * Sanitize headers for logging (remove sensitive data)
   */
  private sanitizeHeaders(headers: any): any {
    if (!headers) return headers;

    const sanitized = { ...headers };
    const sensitiveHeaders = ['authorization', 'cookie', 'x-api-key'];

    sensitiveHeaders.forEach((header) => {
      if (sanitized[header]) {
        sanitized[header] = '[REDACTED]';
      }
    });

    return sanitized;
  }
}
