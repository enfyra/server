import {
  Injectable,
  NestInterceptor,
  ExecutionContext,
  CallHandler,
} from '@nestjs/common';
import { Observable } from 'rxjs';
import { tap } from 'rxjs/operators';
import { Request, Response } from 'express';
import { LoggingService } from '../services/logging.service';

@Injectable()
export class RequestLoggingInterceptor implements NestInterceptor {
  constructor(private readonly loggingService: LoggingService) {}

  intercept(context: ExecutionContext, next: CallHandler): Observable<any> {
    const startTime = Date.now();
    const request = context.switchToHttp().getRequest<Request>();
    const response = context.switchToHttp().getResponse<Response>();

    const correlationId = this.getCorrelationId(request);

    this.loggingService.setCorrelationId(correlationId);
    this.loggingService.setContext({
      method: request.method,
      url: request.url,
      userAgent: request.headers['user-agent'],
      ip: request.ip || request.connection.remoteAddress,
      userId: (request as any).user?.id,
    });

    response.setHeader('X-Correlation-ID', correlationId);

    return next.handle().pipe(
      tap({
        next: () => {
          const responseTime = Date.now() - startTime;
          this.loggingService.logResponse(
            request.method,
            request.url,
            response.statusCode,
            responseTime,
            (request as any).user?.id,
            {
              query: request.query,
              body: this.sanitizeBody(request.body),
              headers: this.sanitizeHeaders(request.headers),
            },
          );
          this.loggingService.clearContext();
        },
        error: () => {
          this.loggingService.clearContext();
        },
      }),
    );
  }

  private getCorrelationId(req: Request): string {
    const providedCorrelationId = req.headers['x-correlation-id'] as string;
    if (providedCorrelationId) {
      return providedCorrelationId;
    }

    return `req_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  }

  private sanitizeBody(body: any): any {
    if (!body || typeof body !== 'object') {
      return body;
    }

    const sanitized = { ...body };
    const sensitiveFields = ['password', 'token', 'secret', 'key', 'authorization'];

    for (const field of sensitiveFields) {
      if (sanitized[field]) {
        sanitized[field] = '[REDACTED]';
      }
    }

    return sanitized;
  }

  private sanitizeHeaders(headers: any): any {
    if (!headers || typeof headers !== 'object') {
      return headers;
    }

    const sanitized = { ...headers };
    const sensitiveHeaders = ['authorization', 'cookie', 'x-api-key'];

    for (const header of sensitiveHeaders) {
      if (sanitized[header]) {
        sanitized[header] = '[REDACTED]';
      }
    }

    return sanitized;
  }
}
