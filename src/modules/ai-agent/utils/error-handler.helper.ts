import { HttpStatus } from '@nestjs/common';

export function mapStatusCodeToHttpStatus(statusCode: number): HttpStatus {
  const statusMap: Record<number, HttpStatus> = {
    400: HttpStatus.BAD_REQUEST,
    401: HttpStatus.UNAUTHORIZED,
    403: HttpStatus.FORBIDDEN,
    404: HttpStatus.NOT_FOUND,
    429: HttpStatus.TOO_MANY_REQUESTS,
    500: HttpStatus.INTERNAL_SERVER_ERROR,
    502: HttpStatus.BAD_GATEWAY,
    503: HttpStatus.SERVICE_UNAVAILABLE,
    504: HttpStatus.GATEWAY_TIMEOUT,
  };
  return statusMap[statusCode] || HttpStatus.INTERNAL_SERVER_ERROR;
}

export function getErrorCodeFromStatus(httpStatus: HttpStatus): string {
  const errorCodeMap: Record<number, string> = {
    [HttpStatus.BAD_REQUEST]: 'BAD_REQUEST',
    [HttpStatus.UNAUTHORIZED]: 'UNAUTHORIZED',
    [HttpStatus.FORBIDDEN]: 'FORBIDDEN',
    [HttpStatus.NOT_FOUND]: 'NOT_FOUND',
    [HttpStatus.TOO_MANY_REQUESTS]: 'TOO_MANY_REQUESTS',
    [HttpStatus.INTERNAL_SERVER_ERROR]: 'INTERNAL_SERVER_ERROR',
    [HttpStatus.BAD_GATEWAY]: 'BAD_GATEWAY',
    [HttpStatus.SERVICE_UNAVAILABLE]: 'SERVICE_UNAVAILABLE',
    [HttpStatus.GATEWAY_TIMEOUT]: 'GATEWAY_TIMEOUT',
  };
  return errorCodeMap[httpStatus] || 'UNKNOWN_ERROR';
}

