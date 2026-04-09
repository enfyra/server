import { Injectable, LoggerService } from '@nestjs/common';
import { AsyncLocalStorage } from 'async_hooks';
import { winstonLogger } from '../../../shared/utils/winston-logger';

export interface LogContext {
  correlationId?: string;
  userId?: string;
  method?: string;
  url?: string;
  userAgent?: string;
  ip?: string;
  [key: string]: any;
}

export interface LogLevel {
  ERROR: 'error';
  WARN: 'warn';
  INFO: 'log';
  DEBUG: 'debug';
  VERBOSE: 'verbose';
}

@Injectable()
export class LoggingService implements LoggerService {
  private readonly als = new AsyncLocalStorage<{
    correlationId: string | null;
    context: LogContext;
  }>();

  setCorrelationId(correlationId: string): void {
    const store = this.als.getStore();
    if (store) {
      store.correlationId = correlationId;
      store.context.correlationId = correlationId;
    }
  }

  setContext(context: Partial<LogContext>): void {
    const store = this.als.getStore();
    if (store) {
      store.context = { ...store.context, ...context };
    }
  }

  clearContext(): void {
    const store = this.als.getStore();
    if (store) {
      store.correlationId = null;
      store.context = {};
    }
  }

  run<T>(fn: () => T): T {
    return this.als.run({ correlationId: null, context: {} }, fn);
  }

  private createLogMeta(data?: any): any {
    const store = this.als.getStore();
    const meta: any = { ...(store?.context || {}) };

    if (data) {
      meta.data = data;
    }

    return meta;
  }

  error(message: string, data?: any): void {
    winstonLogger.error(message, this.createLogMeta(data));
  }

  warn(message: string, data?: any): void {
    winstonLogger.warn(message, this.createLogMeta(data));
  }

  log(message: string, data?: any): void {
    winstonLogger.info(message, this.createLogMeta(data));
  }

  debug(message: string, data?: any): void {
    winstonLogger.debug(message, this.createLogMeta(data));
  }

  verbose(message: string, data?: any): void {
    winstonLogger.verbose(message, this.createLogMeta(data));
  }

  logResponse(
    method: string,
    url: string,
    statusCode: number,
    responseTime: number,
    userId?: string,
    requestData?: any,
  ): void {
    this.log('API Response', {
      method,
      url,
      statusCode,
      responseTime: `${responseTime}ms`,
      userId,
      ...requestData,
    });
  }

  logDatabaseOperation(
    operation: string,
    table: string,
    duration: number,
    success: boolean,
    error?: any,
  ): void {
    const logData: any = {
      operation,
      table,
      duration: `${duration}ms`,
      success,
    };

    if (error) {
      logData.error = error;
    }

    if (success) {
      this.log('Database Operation', logData);
    } else {
      this.error('Database Operation Failed', logData);
    }
  }

  logExternalServiceCall(
    service: string,
    endpoint: string,
    method: string,
    duration: number,
    success: boolean,
    error?: any,
  ): void {
    const logData: any = {
      service,
      endpoint,
      method,
      duration: `${duration}ms`,
      success,
    };

    if (error) {
      logData.error = error;
    }

    if (success) {
      this.log('External Service Call', logData);
    } else {
      this.error('External Service Call Failed', logData);
    }
  }

  logScriptExecution(
    scriptId: string,
    duration: number,
    success: boolean,
    error?: any,
  ): void {
    const logData: any = {
      scriptId,
      duration: `${duration}ms`,
      success,
    };

    if (error) {
      logData.error = error;
    }

    if (success) {
      this.log('Script Execution', logData);
    } else {
      this.error('Script Execution Failed', logData);
    }
  }

  logPerformance(operation: string, duration: number, metadata?: any): void {
    this.log('Performance Metric', {
      operation,
      duration: `${duration}ms`,
      ...metadata,
    });
  }

  logSecurityEvent(
    event: string,
    userId?: string,
    ip?: string,
    details?: any,
  ): void {
    this.warn('Security Event', {
      event,
      userId,
      ip,
      ...details,
    });
  }

  logBusinessEvent(
    event: string,
    userId?: string,
    entity?: string,
    entityId?: string,
    details?: any,
  ): void {
    this.log('Business Event', {
      event,
      userId,
      entity,
      entityId,
      ...details,
    });
  }
}
