import {
  logStore,
  mergeLogContext,
  setCorrelationId as setStoreCorrelationId,
  clearLogContext,
} from '../../../shared/log-store';
import { Logger } from '../../../shared/logger';

export interface LogContext {
  correlationId?: string;
  userId?: string;
  method?: string;
  url?: string;
  userAgent?: string;
  ip?: string;
  [key: string]: any;
}

export class LoggingService {
  private readonly logger = new Logger('HTTP');

  setCorrelationId(correlationId: string): void {
    setStoreCorrelationId(correlationId);
  }

  setContext(context: Partial<LogContext>): void {
    const { correlationId, ...rest } = context;
    if (correlationId) setStoreCorrelationId(correlationId);
    if (Object.keys(rest).length > 0) mergeLogContext(rest);
  }

  clearContext(): void {
    clearLogContext();
  }

  run<T>(fn: () => T): T {
    return logStore.run({ correlationId: undefined, context: {} }, fn);
  }

  error(message: string, data?: any): void {
    this.logger.error(data ? { message, data } : message);
  }

  warn(message: string, data?: any): void {
    this.logger.warn(data ? { message, data } : message);
  }

  log(message: string, data?: any): void {
    this.logger.log(data ? { message, data } : message);
  }

  debug(message: string, data?: any): void {
    this.logger.debug(data ? { message, data } : message);
  }

  verbose(message: string, data?: any): void {
    this.logger.verbose(data ? { message, data } : message);
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
    const data: any = {
      operation,
      table,
      duration: `${duration}ms`,
      success,
    };
    if (error) data.error = error;
    if (success) this.log('Database Operation', data);
    else this.error('Database Operation Failed', data);
  }

  logExternalServiceCall(
    service: string,
    endpoint: string,
    method: string,
    duration: number,
    success: boolean,
    error?: any,
  ): void {
    const data: any = {
      service,
      endpoint,
      method,
      duration: `${duration}ms`,
      success,
    };
    if (error) data.error = error;
    if (success) this.log('External Service Call', data);
    else this.error('External Service Call Failed', data);
  }

  logScriptExecution(
    scriptId: string,
    duration: number,
    success: boolean,
    error?: any,
  ): void {
    const data: any = {
      scriptId,
      duration: `${duration}ms`,
      success,
    };
    if (error) data.error = error;
    if (success) this.log('Script Execution', data);
    else this.error('Script Execution Failed', data);
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
    this.warn('Security Event', { event, userId, ip, ...details });
  }

  logBusinessEvent(
    event: string,
    userId?: string,
    entity?: string,
    entityId?: string,
    details?: any,
  ): void {
    this.log('Business Event', { event, userId, entity, entityId, ...details });
  }
}
