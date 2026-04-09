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

const SLOW_REQUEST_THRESHOLD_MS = 2000;

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
      userId: (request as any).user?.id,
    });

    response.setHeader('X-Correlation-ID', correlationId);

    return next.handle().pipe(
      tap({
        next: () => {
          const responseTime = Date.now() - startTime;
          const statusCode = response.statusCode;

          if (statusCode >= 400 || responseTime > SLOW_REQUEST_THRESHOLD_MS) {
            this.loggingService.logResponse(
              request.method,
              request.url,
              statusCode,
              responseTime,
              (request as any).user?.id,
              {
                query:
                  Object.keys(request.query).length > 0
                    ? request.query
                    : undefined,
              },
            );
          }

          this.loggingService.clearContext();
        },
        error: () => {
          this.loggingService.clearContext();
        },
      }),
    );
  }

  private getCorrelationId(req: Request): string {
    const provided = req.headers['x-correlation-id'] as string;
    if (provided && provided.length <= 128) {
      return provided.replace(/[^\w\-.:]/g, '');
    }

    return `req_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  }
}
