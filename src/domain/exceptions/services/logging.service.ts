import { Logger } from '../../../shared/logger';

export class LoggingService {
  private readonly logger = new Logger('HTTP');

  error(message: string, data?: any): void {
    this.logger.error(data ? { message, data } : message);
  }
}
