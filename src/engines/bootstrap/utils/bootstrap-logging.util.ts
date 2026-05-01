import type { Logger } from '../../../shared/logger';

export function isBootstrapVerbose(): boolean {
  return process.env.BOOTSTRAP_VERBOSE === '1';
}

export function bootstrapVerboseLog(logger: Logger, message: string): void {
  if (isBootstrapVerbose()) {
    logger.log(message);
  }
}
