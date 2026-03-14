import { LoggerService, Injectable, ConsoleLogger } from '@nestjs/common';

const EXCLUDED_CONTEXTS = [
  'InstanceLoader',
  'RouterExplorer',
  'RoutesResolver',
  'NestFactory',
  'NestApplication',
];

@Injectable()
export class AppLogger extends ConsoleLogger implements LoggerService {
  private shouldLog(context?: string): boolean {
    if (!context) return true;
    return !EXCLUDED_CONTEXTS.includes(context);
  }

  log(message: any, context?: string) {
    if (this.shouldLog(context)) {
      super.log(message, context);
    }
  }

  error(message: any, trace?: string, context?: string) {
    if (this.shouldLog(context)) {
      super.error(message, trace, context);
    }
  }

  warn(message: any, context?: string) {
    if (this.shouldLog(context)) {
      super.warn(message, context);
    }
  }

  debug(message: any, context?: string) {
    if (this.shouldLog(context)) {
      super.debug(message, context);
    }
  }

  verbose(message: any, context?: string) {
    if (this.shouldLog(context)) {
      super.verbose(message, context);
    }
  }
}
