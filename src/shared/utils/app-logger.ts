import { LoggerService, Injectable } from '@nestjs/common';
import { winstonLogger, shouldLog } from './winston-logger';

@Injectable()
export class AppLogger implements LoggerService {
  private extractObjectMessage(
    message: Record<string, any>,
    fallback: string,
  ): { msg: string; meta: Record<string, any> } {
    const { message: msg, ...rest } = message;
    return { msg: msg || fallback, meta: rest };
  }

  log(message: any, context?: string) {
    if (!shouldLog(context)) return;

    if (typeof message === 'object' && message !== null) {
      const { msg, meta } = this.extractObjectMessage(message, 'Log');
      winstonLogger.info(msg, { context, ...meta });
    } else {
      winstonLogger.info(String(message), { context });
    }
  }

  error(message: any, trace?: string, context?: string) {
    if (!shouldLog(context)) return;

    if (typeof message === 'object' && message !== null) {
      const { msg, meta } = this.extractObjectMessage(message, 'Error');
      winstonLogger.error(msg, { context, ...meta });
    } else if (trace && typeof trace === 'object') {
      const { message: _msg, ...rest } = trace as Record<string, any>;
      winstonLogger.error(String(message), { context, ...rest });
    } else {
      winstonLogger.error(String(message), { context, trace });
    }
  }

  warn(message: any, context?: string) {
    if (!shouldLog(context)) return;

    if (typeof message === 'object' && message !== null) {
      const { msg, meta } = this.extractObjectMessage(message, 'Warning');
      winstonLogger.warn(msg, { context, ...meta });
    } else {
      winstonLogger.warn(String(message), { context });
    }
  }

  debug(message: any, context?: string) {
    if (!shouldLog(context)) return;

    if (typeof message === 'object' && message !== null) {
      const { msg, meta } = this.extractObjectMessage(message, 'Debug');
      winstonLogger.debug(msg, { context, ...meta });
    } else {
      winstonLogger.debug(String(message), { context });
    }
  }

  verbose(message: any, context?: string) {
    if (!shouldLog(context)) return;

    if (typeof message === 'object' && message !== null) {
      const { msg, meta } = this.extractObjectMessage(message, 'Verbose');
      winstonLogger.verbose(msg, { context, ...meta });
    } else {
      winstonLogger.verbose(String(message), { context });
    }
  }

  fatal(message: any, trace?: string, context?: string) {
    if (!shouldLog(context)) return;

    if (typeof message === 'object' && message !== null) {
      const { msg, meta } = this.extractObjectMessage(message, 'Fatal');
      winstonLogger.error(msg, { context, fatal: true, ...meta });
    } else if (trace && typeof trace === 'object') {
      const { message: _msg, ...rest } = trace as Record<string, any>;
      winstonLogger.error(String(message), { context, fatal: true, ...rest });
    } else {
      winstonLogger.error(String(message), { context, trace, fatal: true });
    }
  }
}
