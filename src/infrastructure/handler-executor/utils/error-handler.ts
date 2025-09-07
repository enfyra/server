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
    // Handle $errors calls from child process
    if (errorPath?.includes('$errors')) {
      switch (errorPath) {
        case '$errors.throw400':
          return new BusinessLogicException(message || 'Bad request');
        case '$errors.throw401':
          return new AuthenticationException(
            message || 'Authentication required',
          );
        case '$errors.throw403':
          return new AuthorizationException(
            message || 'Insufficient permissions',
          );
        default:
          return new ScriptExecutionException(message || 'Unknown error', code);
      }
    }

    // Handle status code based errors
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

    // Default fallback
    return new ScriptExecutionException(
      message || 'Unknown error',
      code,
      details,
    );
  }

  /**
   * Log error with consistent format
   */
  static logError(
    errorType: string,
    message: string,
    code: string,
    additionalData?: any,
  ): void {
    this.logger.error(`Handler Executor ${errorType}`, {
      message,
      code: code.substring(0, 100), // Log first 100 chars of script
      ...additionalData,
    });
  }

  /**
   * Handle child process error with cleanup
   */
  static handleChildError(
    isDone: boolean,
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
    if (isDone) return true;

    child.removeAllListeners();
    clearTimeout(timeout);
    pool.release(child);

    this.logError(errorType, message, code, additionalData);
    reject(error);
    return true;
  }
}
