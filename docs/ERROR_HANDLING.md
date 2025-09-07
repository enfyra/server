# Error Handling Documentation

## Overview

Enfyra Backend implements a comprehensive error handling system that provides consistent, structured error responses across all API endpoints. The system includes custom exception classes, centralized error handling, and detailed logging for debugging.

## Error Response Format

All errors follow a standardized format:

```json
{
  "success": false,
  "message": "Error message",
  "statusCode": 400,
  "error": {
    "code": "ERROR_CODE",
    "message": "Error message",
    "details": null,
    "timestamp": "2025-08-05T03:54:42.610Z",
    "path": "/api/endpoint",
    "method": "GET",
    "correlationId": "req_1754366082608_f1ts2w7za"
  }
}
```

### Response Fields

| Field                 | Type    | Description                                 |
| --------------------- | ------- | ------------------------------------------- |
| `success`             | boolean | Always `false` for errors                   |
| `message`             | string  | Human-readable error message                |
| `statusCode`          | number  | HTTP status code                            |
| `error.code`          | string  | Machine-readable error code                 |
| `error.message`       | string  | Detailed error message                      |
| `error.details`       | any     | Additional error details (development only) |
| `error.timestamp`     | string  | ISO timestamp of error                      |
| `error.path`          | string  | Request path that caused error              |
| `error.method`        | string  | HTTP method that caused error               |
| `error.correlationId` | string  | Unique ID for tracing                       |

## Custom Exception Classes

### Exception Hierarchy

```typescript
CustomException (base)
├── AuthenticationException
├── AuthorizationException
├── BusinessLogicException
├── ResourceNotFoundException
├── ScriptExecutionException
├── ScriptTimeoutException
└── ValidationException
```

### Base Exception (`src/exceptions/custom-exceptions.ts`)

```typescript
export abstract class CustomException extends Error {
  abstract readonly errorCode: string;
  abstract readonly statusCode: number;
  readonly details?: any;

  constructor(message: string, details?: any) {
    super(message);
    this.details = details;
  }

  abstract getStatus(): number;
}
```

### Authentication Exception

```typescript
export class AuthenticationException extends CustomException {
  readonly errorCode = 'UNAUTHORIZED';
  readonly statusCode = 401;

  constructor(message: string = 'Authentication required') {
    super(message);
  }

  getStatus(): number {
    return this.statusCode;
  }
}
```

### Authorization Exception

```typescript
export class AuthorizationException extends CustomException {
  readonly errorCode = 'FORBIDDEN';
  readonly statusCode = 403;

  constructor(message: string = 'Insufficient permissions') {
    super(message);
  }

  getStatus(): number {
    return this.statusCode;
  }
}
```

### Business Logic Exception

```typescript
export class BusinessLogicException extends CustomException {
  readonly errorCode = 'BUSINESS_LOGIC_ERROR';
  readonly statusCode = 400;

  constructor(message: string, details?: any) {
    super(message, details);
  }

  getStatus(): number {
    return this.statusCode;
  }
}
```

### Resource Not Found Exception

```typescript
export class ResourceNotFoundException extends CustomException {
  readonly errorCode = 'NOT_FOUND';
  readonly statusCode = 404;

  constructor(resource: string, id?: string) {
    const message = id
      ? `${resource} with id ${id} not found`
      : `${resource} not found`;
    super(message);
  }

  getStatus(): number {
    return this.statusCode;
  }
}
```

### Script Execution Exception

```typescript
export class ScriptExecutionException extends CustomException {
  readonly errorCode = 'SCRIPT_EXECUTION_ERROR';
  readonly statusCode = 500;

  constructor(message: string, details?: any) {
    super(message, details);
  }

  getStatus(): number {
    return this.statusCode;
  }
}
```

### Script Timeout Exception

```typescript
export class ScriptTimeoutException extends CustomException {
  readonly errorCode = 'SCRIPT_TIMEOUT_ERROR';
  readonly statusCode = 408;

  constructor(timeout: number) {
    super(`Script execution timed out after ${timeout}ms`);
  }

  getStatus(): number {
    return this.statusCode;
  }
}
```

### Validation Exception

```typescript
export class ValidationException extends CustomException {
  readonly errorCode = 'VALIDATION_ERROR';
  readonly statusCode = 400;

  constructor(message: string, details?: any) {
    super(message, details);
  }

  getStatus(): number {
    return this.statusCode;
  }
}
```

## Global Exception Filter

### Overview (`src/error-handling/filters/global-exception.filter.ts`)

The Global Exception Filter catches all exceptions thrown throughout the application and formats them into standardized error responses.

### Key Features

- **Centralized Error Handling**: All exceptions go through one filter
- **Structured Logging**: Detailed error logging with correlation IDs
- **Environment-aware**: Different detail levels for development vs production
- **Correlation IDs**: Track requests across services
- **GraphQL Support**: Handles GraphQL errors differently

### Implementation

```typescript
@Catch()
export class GlobalExceptionFilter implements ExceptionFilter {
  private readonly logger = new Logger(GlobalExceptionFilter.name);

  catch(exception: unknown, host: ArgumentsHost): void {
    const ctx = host.switchToHttp();
    const response = ctx.getResponse<Response>();
    const request = ctx.getRequest<Request>();

    // Extract correlation ID
    const correlationId = this.generateCorrelationId();

    // Get error details
    const { statusCode, errorCode, message, details } =
      this.getErrorDetails(exception);

    // Log error
    this.logError(exception, request, correlationId, statusCode);

    // Create response
    const errorResponse: ErrorResponse = {
      success: false,
      message,
      statusCode,
      error: {
        code: errorCode,
        message,
        details,
        timestamp: new Date().toISOString(),
        path: request.url,
        method: request.method,
        correlationId,
      },
    };

    // Send response
    response.status(statusCode).json(errorResponse);
  }
}
```

### Error Details Resolution

```typescript
private getErrorDetails(exception: unknown): {
  statusCode: number;
  errorCode: string;
  message: string;
  details?: any;
} {
  // Handle Custom Exceptions
  if (isCustomException(exception)) {
    return {
      statusCode: exception.getStatus(),
      errorCode: exception.errorCode,
      message: exception.message,
      details: exception.details,
    };
  }

  // Handle HttpException
  if (exception instanceof HttpException) {
    const status = exception.getStatus();
    const response = exception.getResponse() as any;

    return {
      statusCode: status,
      errorCode: getErrorCode(exception),
      message: response?.message || exception.message,
      details: response?.details || null,
    };
  }

  // Handle GraphQLError
  if (exception instanceof GraphQLError) {
    return {
      statusCode: HttpStatus.BAD_REQUEST,
      errorCode: 'GRAPHQL_ERROR',
      message: exception.message,
      details: exception.extensions,
    };
  }

  // Handle unknown errors
  if (exception instanceof Error) {
    return {
      statusCode: HttpStatus.INTERNAL_SERVER_ERROR,
      errorCode: 'INTERNAL_SERVER_ERROR',
      message: exception.message || 'An unexpected error occurred',
      details: process.env.NODE_ENV === 'development' ? exception.stack : null,
    };
  }

  // Handle other types
  return {
    statusCode: HttpStatus.INTERNAL_SERVER_ERROR,
    errorCode: 'UNKNOWN_ERROR',
    message: 'An unknown error occurred',
    details: exception,
  };
}
```

## Error Logging

### Structured Logging

```typescript
private logError(
  exception: unknown,
  request: Request,
  correlationId: string,
  statusCode: number,
): void {
  const logData = {
    correlationId,
    method: request.method,
    url: request.url,
    statusCode,
    userAgent: request.headers['user-agent'],
    ip: request.ip,
    userId: (request as any).user?.id,
    error: exception instanceof Error
      ? {
          name: exception.name,
          message: exception.message,
          stack: exception.stack,
        }
      : exception,
  };

  if (statusCode >= 500) {
    this.logger.error('Server Error', logData);
  } else if (statusCode >= 400) {
    this.logger.warn('Client Error', logData);
  } else {
    this.logger.log('Other Error', logData);
  }
}
```

### Log Levels

- **Error (500+)**: Server errors, logged as ERROR
- **Warning (400-499)**: Client errors, logged as WARN
- **Info (Other)**: Other errors, logged as INFO

## Error Codes Reference

| Code                     | Status | Description              | Usage                                |
| ------------------------ | ------ | ------------------------ | ------------------------------------ |
| `UNAUTHORIZED`           | 401    | Authentication required  | Missing or invalid JWT token         |
| `FORBIDDEN`              | 403    | Insufficient permissions | User lacks required role/permission  |
| `NOT_FOUND`              | 404    | Resource not found       | Table, record, or endpoint not found |
| `VALIDATION_ERROR`       | 400    | Invalid input data       | Request validation failed            |
| `BUSINESS_LOGIC_ERROR`   | 400    | Business rule violation  | Custom business logic error          |
| `SCRIPT_EXECUTION_ERROR` | 500    | Handler script error     | JavaScript handler execution failed  |
| `SCRIPT_TIMEOUT_ERROR`   | 408    | Handler script timeout   | Script exceeded timeout limit        |
| `GRAPHQL_ERROR`          | 400    | GraphQL specific error   | GraphQL parsing or validation error  |
| `INTERNAL_SERVER_ERROR`  | 500    | Unexpected server error  | Unhandled exception                  |
| `UNKNOWN_ERROR`          | 500    | Unknown error type       | Non-Error exception                  |

## Error Handling Best Practices

### 1. Use Custom Exceptions

```typescript
// Good
if (!user) {
  throw new ResourceNotFoundException('User', userId);
}

// Bad
if (!user) {
  throw new Error('User not found');
}
```

### 2. Provide Meaningful Messages

```typescript
// Good
throw new BusinessLogicException('Cannot delete user with active posts', {
  userId,
  activePostCount: 5,
});

// Bad
throw new BusinessLogicException('Error');
```

### 3. Include Relevant Details

```typescript
// Good
throw new ValidationException('Invalid email format', {
  field: 'email',
  value: email,
  expected: 'valid email format',
});
```

### 4. Handle Async Errors

```typescript
try {
  await someAsyncOperation();
} catch (error) {
  if (error instanceof CustomException) {
    throw error;
  }
  throw new ScriptExecutionException('Failed to execute operation', {
    originalError: error.message,
  });
}
```

## Testing Error Handling

### Unit Tests

```typescript
describe('CustomException', () => {
  it('should create BusinessLogicException with correct properties', () => {
    const exception = new BusinessLogicException('Test error', {
      detail: 'test',
    });

    expect(exception.errorCode).toBe('BUSINESS_LOGIC_ERROR');
    expect(exception.statusCode).toBe(400);
    expect(exception.message).toBe('Test error');
    expect(exception.details).toEqual({ detail: 'test' });
  });
});
```

### Integration Tests

```typescript
describe('Error Handling', () => {
  it('should return 404 for non-existent resource', async () => {
    const response = await request(app.getHttpServer())
      .get('/non-existent-table')
      .expect(404);

    expect(response.body).toMatchObject({
      success: false,
      statusCode: 404,
      error: {
        code: 'NOT_FOUND',
        message: expect.stringContaining('not found'),
      },
    });
  });
});
```

## Monitoring and Alerting

### Error Metrics

- Error rate by endpoint
- Error rate by error code
- Response time for error cases
- Correlation ID tracking

### Alerting Rules

- Error rate > 5% for any endpoint
- 500 errors > 1% of total requests
- Script timeout rate > 10%
- Authentication failures > 20%

## Debugging Errors

### Using Correlation IDs

1. Find correlation ID in error response
2. Search logs for correlation ID
3. Trace request flow through all services
4. Identify root cause

### Development Mode

In development mode, error responses include:

- Full stack traces
- Detailed error information
- Internal error details

### Production Mode

In production mode, error responses:

- Hide internal details
- Provide generic messages for 500 errors
- Log full details for debugging
