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

  /**
   * Set correlation ID for the current request
   */
  setCorrelationId(correlationId: string): void {
    this.correlationId = correlationId;
    this.context.correlationId = correlationId;
  }

  /**
   * Set context for the current request
   */
  setContext(context: Partial<LogContext>): void {
    this.context = { ...this.context, ...context };
  }

  /**
   * Clear context and correlation ID
   */
  clearContext(): void {
    this.correlationId = null;
    this.context = {};
  }

  /**
   * Create structured log data with context
   */
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

  /**
   * Log error with structured format
   */
  error(message: string, data?: any): void {
    const logData = this.createLogData(message, data);
    this.logger.error(logData);
  }

  /**
   * Log warning with structured format
   */
  warn(message: string, data?: any): void {
    const logData = this.createLogData(message, data);
    this.logger.warn(logData);
  }

  /**
   * Log info with structured format
   */
  log(message: string, data?: any): void {
    const logData = this.createLogData(message, data);
    this.logger.log(logData);
  }

  /**
   * Log debug with structured format
   */
  debug(message: string, data?: any): void {
    const logData = this.createLogData(message, data);
    this.logger.debug(logData);
  }

  /**
   * Log verbose with structured format
   */
  verbose(message: string, data?: any): void {
    const logData = this.createLogData(message, data);
    this.logger.verbose(logData);
  }

  /**
   * Log API request
   */
  logRequest(
    method: string,
    url: string,
    userId?: string,
    additionalData?: any,
  ): void {
    // Remove userId from additionalData to avoid duplication
    const { userId: _, ...cleanAdditionalData } = additionalData || {};
    
    this.log('API Request', {
      method,
      url,
      userId,
      ...cleanAdditionalData,
    });
  }

  /**
   * Log API response
   */
  logResponse(
    method: string,
    url: string,
    statusCode: number,
    responseTime: number,
    userId?: string,
  ): void {
    this.log('API Response', {
      method,
      url,
      statusCode,
      responseTime: `${responseTime}ms`,
      userId,
    });
  }

  /**
   * Log database operation
   */
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

  /**
   * Log external service call
   */
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

  /**
   * Log script execution
   */
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

  /**
   * Log performance metrics
   */
  logPerformance(operation: string, duration: number, metadata?: any): void {
    this.log('Performance Metric', {
      operation,
      duration: `${duration}ms`,
      ...metadata,
    });
  }

  /**
   * Log security events
   */
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

  /**
   * Log business events
   */
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
