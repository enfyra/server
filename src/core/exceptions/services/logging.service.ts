import { Injectable, Logger, LoggerService } from '@nestjs/common';

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
  private readonly logger = new Logger(LoggingService.name);
  private correlationId: string | null = null;
  private context: LogContext = {};

  setCorrelationId(correlationId: string): void {
    this.correlationId = correlationId;
    this.context.correlationId = correlationId;
  }

  setContext(context: Partial<LogContext>): void {
    this.context = { ...this.context, ...context };
  }

  clearContext(): void {
    this.correlationId = null;
    this.context = {};
  }

  private createLogData(message: string, data?: any): any {
    const logData: any = {
      message,
      timestamp: new Date().toISOString(),
      ...this.context,
    };

    if (data) {
      logData.data = data;
    }

    return logData;
  }

  error(message: string, data?: any): void {
    const logData = this.createLogData(message, data);
    this.logger.error(logData);
  }

  warn(message: string, data?: any): void {
    const logData = this.createLogData(message, data);
    this.logger.warn(logData);
  }

  log(message: string, data?: any): void {
    const logData = this.createLogData(message, data);
    this.logger.log(logData);
  }

  debug(message: string, data?: any): void {
    const logData = this.createLogData(message, data);
    this.logger.debug(logData);
  }

  verbose(message: string, data?: any): void {
    const logData = this.createLogData(message, data);
    this.logger.verbose(logData);
  }

  logResponse(
    method: string,
    url: string,
    statusCode: number,
    responseTime: number,
    userId?: string,
    requestData?: any,
  ): void {
    const { userId: _, ...cleanRequestData } = requestData || {};

    this.log('API Response', {
      method,
      url,
      statusCode,
      responseTime: `${responseTime}ms`,
      userId,
      ...cleanRequestData,
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
