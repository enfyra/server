import { Logger } from '@nestjs/common';
import {
  ScriptTimeoutException,
  ScriptExecutionException,
  AuthenticationException,
  AuthorizationException,
  BusinessLogicException,
} from '../../../core/exceptions/custom-exceptions';

export class ErrorHandler {
  private static readonly logger = new Logger(ErrorHandler.name);

  static createException(
    errorPath?: string,
    statusCode?: number,
    message?: string,
    code?: string,
    details?: any,
  ): any {
    if (errorPath?.includes('$throw')) {
      switch (errorPath) {
        case '$throw.400':
          return new BusinessLogicException(message || 'Bad request');
        case '$throw.401':
          return new AuthenticationException(
            message || 'Authentication required',
          );
        case '$throw.403':
          return new AuthorizationException(
            message || 'Insufficient permissions',
          );
        default:
          return new ScriptExecutionException(message || 'Unknown error', code);
      }
    }

    if (statusCode) {
      switch (statusCode) {
        case 400:
          return new BusinessLogicException(message || 'Bad request');
        case 401:
          return new AuthenticationException(
            message || 'Authentication required',
          );
        case 403:
          return new AuthorizationException(
            message || 'Insufficient permissions',
          );
        default:
          return new ScriptExecutionException(
            message || 'Unknown error',
            code,
            details,
          );
      }
    }

    return new ScriptExecutionException(
      message || 'Unknown error',
      code,
      details,
    );
  }

  static logError(
    errorType: string,
    message: string,
    code: string,
    additionalData?: any,
  ): void {
    this.logger.error(`Handler Executor ${errorType}`, {
      message,
      code: code.substring(0, 100),
      ...additionalData,
    });
  }

  static handleChildError(
    isDoneRef: { value: boolean },
    child: any,
    timeout: NodeJS.Timeout,
    pool: any,
    error: any,
    errorType: string,
    message: string,
    code: string,
    reject: (error: any) => void,
    additionalData?: any,
  ): boolean {
    if (isDoneRef.value) return true;

    isDoneRef.value = true;
    child.removeAllListeners();
    clearTimeout(timeout);

    pool.destroy(child).catch((err: any) => {
      this.logger.warn('Failed to destroy dead child process', err);
    });

    this.logError(errorType, message, code, additionalData);
    reject(error);
    return true;
  }
}
