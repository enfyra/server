import type { Response } from 'express';
import type { DynamicHttpFailure } from '../types/dynamic-http-failure.types';
import { buildErrorResponseTrace } from '../../../shared/utils/error-response-trace.util';

const failures = new WeakMap<object, DynamicHttpFailure>();
const controllers = new WeakMap<object, AbortController>();

export function bindDynamicHttpAbort(
  response: object,
  controller: AbortController,
): () => void {
  controllers.set(response, controller);
  return () => {
    if (controllers.get(response) === controller) controllers.delete(response);
  };
}

export function finishDynamicHttpFailure(
  response: Response,
  failure: DynamicHttpFailure,
): void {
  if (
    failure.origin === 'client' ||
    response.destroyed ||
    response.writableEnded
  )
    return;
  if (response.headersSent) {
    response.destroy();
    return;
  }
  const trace = buildErrorResponseTrace(response.req, response);
  response.setHeader('X-Correlation-ID', trace.correlationId);
  response.setHeader('Content-Type', 'application/json; charset=utf-8');
  response.setHeader('x-should-retry', 'true');
  response.setHeader('Retry-After', '5');
  response.status(failure.statusCode).json({
    success: false,
    statusCode: failure.statusCode,
    message: failure.message,
    error: {
      code: failure.code,
      message: failure.message,
      should_retry: true,
      retry_after_seconds: 5,
      ...trace,
    },
  });
}

export function getDynamicHttpFailure(
  response: object | undefined,
): DynamicHttpFailure | undefined {
  return response ? failures.get(response) : undefined;
}

export function recordDynamicHttpFailure(
  response: object,
  failure: DynamicHttpFailure,
): DynamicHttpFailure {
  const existing = failures.get(response);
  if (existing) return existing;
  failures.set(response, failure);
  return failure;
}

export function recordDynamicClientDisconnect(
  response: object,
): DynamicHttpFailure {
  return recordDynamicHttpFailure(response, {
    origin: 'client',
    statusCode: 499,
    code: 'client_closed_request',
    message: 'The client closed the request before completion.',
  });
}

export function recordDynamicStreamFailure(
  response: object,
  error: Error & { code?: string },
): DynamicHttpFailure {
  const executionTimeout = error.code === 'ERR_SCRIPT_EXECUTION_TIMEOUT';
  const streamTimeout =
    error.code === 'ERR_PACKAGE_STREAM_TIMEOUT' ||
    /^Package stream timed out after \d+ms$/.test(error.message);
  const failure = recordDynamicHttpFailure(response, {
    origin: executionTimeout ? 'execution' : 'stream',
    statusCode: executionTimeout || streamTimeout ? 504 : 502,
    code: executionTimeout
      ? 'execution_timeout'
      : streamTimeout
        ? 'response_stream_timeout'
        : 'response_stream_error',
    message:
      executionTimeout || streamTimeout
        ? 'The request did not complete before its timeout.'
        : 'The response stream could not be completed.',
  });
  controllers.get(response)?.abort();
  return failure;
}

export function isDynamicExecutionTimeout(error: unknown): boolean {
  const failure = error as {
    code?: string;
    errorCode?: string;
    isTimeout?: boolean;
  } | null;
  return (
    failure?.code === 'ERR_SCRIPT_EXECUTION_TIMEOUT' ||
    failure?.errorCode === 'SCRIPT_TIMEOUT' ||
    failure?.isTimeout === true
  );
}

export function recordDynamicExecutionFailure(
  response: object,
  error: unknown,
): DynamicHttpFailure {
  const timeout = isDynamicExecutionTimeout(error);
  return recordDynamicHttpFailure(response, {
    origin: 'execution',
    statusCode: timeout ? 504 : 500,
    code: timeout ? 'execution_timeout' : 'execution_failed',
    message: timeout
      ? 'The request did not complete before its timeout.'
      : 'The request could not be completed.',
  });
}
