import { Logger } from '@nestjs/common';

export interface RetryOptions {
  maxRetries?: number;
  baseDelayMs?: number;
  maxDelayMs?: number;
  onRetry?: (attempt: number, error: any) => void;
}

export interface RetryableError {
  isRetriable: boolean;
  reason?: string;
}

const logger = new Logger('RetryHelper');

/**
 * Determines if an error is retriable based on error type and message
 */
export function isRetriableError(error: any): RetryableError {
  const message = error?.message?.toLowerCase() || '';
  const status = error?.status || error?.statusCode || error?.response?.status;
  const errorCode = error?.code || error?.errorCode;

  // Network errors - retriable
  if (
    message.includes('timeout') ||
    message.includes('timed out') ||
    message.includes('econnrefused') ||
    message.includes('enotfound') ||
    message.includes('enetunreach') ||
    message.includes('network') ||
    errorCode === 'ECONNRESET' ||
    errorCode === 'ETIMEDOUT' ||
    errorCode === 'ECONNREFUSED'
  ) {
    return { isRetriable: true, reason: 'network_error' };
  }

  // Rate limit errors - retriable with backoff
  if (
    status === 429 ||
    message.includes('rate limit') ||
    message.includes('too many requests') ||
    errorCode === 'rate_limit_exceeded'
  ) {
    return { isRetriable: true, reason: 'rate_limit' };
  }

  // Service unavailable - retriable
  if (
    status === 503 ||
    status === 504 ||
    message.includes('service unavailable') ||
    message.includes('gateway timeout')
  ) {
    return { isRetriable: true, reason: 'service_unavailable' };
  }

  // Server errors (5xx) - retriable
  if (status >= 500 && status < 600) {
    return { isRetriable: true, reason: 'server_error' };
  }

  // Overloaded errors - retriable
  if (
    message.includes('overloaded') ||
    message.includes('capacity') ||
    errorCode === 'overloaded_error'
  ) {
    return { isRetriable: true, reason: 'overloaded' };
  }

  // Business logic errors - NOT retriable
  if (
    message.includes('not found') ||
    message.includes('does not exist') ||
    message.includes('already exists') ||
    message.includes('duplicate') ||
    message.includes('permission denied') ||
    message.includes('unauthorized') ||
    message.includes('forbidden') ||
    message.includes('invalid') ||
    status === 400 ||
    status === 401 ||
    status === 403 ||
    status === 404 ||
    status === 409
  ) {
    return { isRetriable: false, reason: 'business_logic_error' };
  }

  // Default: retriable for unknown errors (fail-safe approach)
  return { isRetriable: true, reason: 'unknown_error' };
}

/**
 * Calculates delay for retry with exponential backoff
 */
export function calculateBackoffDelay(
  attempt: number,
  baseDelayMs: number = 1000,
  maxDelayMs: number = 10000,
): number {
  // Exponential backoff: 1s, 2s, 4s, 8s, ...
  const exponentialDelay = baseDelayMs * Math.pow(2, attempt - 1);
  // Add jitter (±20%) to prevent thundering herd
  const jitter = exponentialDelay * (0.8 + Math.random() * 0.4);
  // Cap at max delay
  return Math.min(Math.round(jitter), maxDelayMs);
}

/**
 * Sleeps for specified milliseconds
 */
export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Retries an async operation with exponential backoff
 *
 * @param operation - The async function to retry
 * @param options - Retry configuration
 * @returns The result of the operation
 * @throws The last error if all retries fail
 */
export async function retryWithBackoff<T>(
  operation: () => Promise<T>,
  options: RetryOptions = {},
): Promise<T> {
  const {
    maxRetries = 3,
    baseDelayMs = 1000,
    maxDelayMs = 10000,
    onRetry,
  } = options;

  let lastError: any;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await operation();
    } catch (error: any) {
      lastError = error;

      // Check if error is retriable
      const { isRetriable, reason } = isRetriableError(error);

      if (!isRetriable) {
        logger.debug(
          `Non-retriable error (${reason}), not retrying: ${error.message}`,
        );
        throw error;
      }

      // Last attempt - don't wait, just throw
      if (attempt === maxRetries) {
        logger.warn(
          `Max retries (${maxRetries}) reached, operation failed: ${error.message}`,
        );
        throw error;
      }

      // Calculate backoff delay
      const delayMs = calculateBackoffDelay(attempt, baseDelayMs, maxDelayMs);

      logger.warn(
        `Retriable error (${reason}) on attempt ${attempt}/${maxRetries}: ${error.message}. Retrying in ${delayMs}ms...`,
      );

      // Call retry callback if provided
      if (onRetry) {
        try {
          onRetry(attempt, error);
        } catch (callbackError) {
          logger.error('Error in onRetry callback:', callbackError);
        }
      }

      // Wait before retry
      await sleep(delayMs);
    }
  }

  // Should never reach here, but throw last error just in case
  throw lastError;
}

/**
 * Retry operation specifically for tool execution
 */
export async function retryToolExecution<T>(
  toolName: string,
  operation: () => Promise<T>,
  options: RetryOptions = {},
): Promise<T> {
  const toolLogger = new Logger(`ToolRetry:${toolName}`);

  return retryWithBackoff(operation, {
    ...options,
    onRetry: (attempt, error) => {
      toolLogger.warn(
        `Tool "${toolName}" failed (attempt ${attempt}/${options.maxRetries || 3}): ${error.message}`,
      );
      if (options.onRetry) {
        options.onRetry(attempt, error);
      }
    },
  });
}
