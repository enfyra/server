import { LoggerService, Injectable } from '@nestjs/common';
import { winstonLogger, shouldLog } from './winston-logger';

@Injectable()
export class AppLogger implements LoggerService {
  log(message: any, context?: string) {
    if (!shouldLog(context)) return;

    if (typeof message === 'object' && message !== null) {
      const msg = (message as Record<string, any>).message || 'Log';
      const meta = { context, ...message };
      winstonLogger.info(msg, meta);
    } else {
      winstonLogger.info(String(message), { context });
    }
  }

  error(message: any, trace?: string, context?: string) {
    if (!shouldLog(context)) return;

    if (typeof message === 'object' && message !== null) {
      const msg = (message as Record<string, any>).message || 'Error';
      const meta = { context, ...message };
      winstonLogger.error(msg, meta);
    } else if (trace && typeof trace === 'object') {
      const meta = { context, ...(trace as Record<string, any>) };
      winstonLogger.error(String(message), meta);
    } else {
      winstonLogger.error(String(message), { context, trace });
    }
  }

  warn(message: any, context?: string) {
    if (!shouldLog(context)) return;

    if (typeof message === 'object' && message !== null) {
      const msg = (message as Record<string, any>).message || 'Warning';
      const meta = { context, ...message };
      winstonLogger.warn(msg, meta);
    } else {
      winstonLogger.warn(String(message), { context });
    }
  }

  debug(message: any, context?: string) {
    if (!shouldLog(context)) return;

    if (typeof message === 'object' && message !== null) {
      const msg = (message as Record<string, any>).message || 'Debug';
      const meta = { context, ...message };
      winstonLogger.debug(msg, meta);
    } else {
      winstonLogger.debug(String(message), { context });
    }
  }

  verbose(message: any, context?: string) {
    if (!shouldLog(context)) return;

    if (typeof message === 'object' && message !== null) {
      const msg = (message as Record<string, any>).message || 'Verbose';
      const meta = { context, ...message };
      winstonLogger.verbose(msg, meta);
    } else {
      winstonLogger.verbose(String(message), { context });
    }
  }

  fatal(message: any, trace?: string, context?: string) {
    if (!shouldLog(context)) return;

    if (typeof message === 'object' && message !== null) {
      const msg = (message as Record<string, any>).message || 'Fatal';
      const meta = { context, fatal: true, ...message };
      winstonLogger.error(msg, meta);
    } else if (trace && typeof trace === 'object') {
      const meta = { context, fatal: true, ...(trace as Record<string, any>) };
      winstonLogger.error(String(message), meta);
    } else {
      winstonLogger.error(String(message), { context, trace, fatal: true });
    }
  }
}