export class AppError extends Error {
  constructor(
    public readonly statusCode: number,
    message: string,
    public readonly code?: string,
    public readonly details?: any,
  ) {
    super(message);
    this.name = this.constructor.name;
    Error.captureStackTrace(this, this.constructor);
  }

  toJSON() {
    return {
      success: false,
      statusCode: this.statusCode,
      message: this.message,
      error: {
        code: this.code || this.name,
        message: this.message,
        details: this.details ?? null,
      },
    };
  }
}

export class BadRequestException extends AppError {
  constructor(message = 'Bad Request', details?: any) {
    super(400, message, 'BAD_REQUEST', details);
  }
}

export class UnauthorizedException extends AppError {
  constructor(message = 'Unauthorized', details?: any) {
    super(401, message, 'UNAUTHORIZED', details);
  }
}

export class ForbiddenException extends AppError {
  constructor(message = 'Forbidden', details?: any) {
    super(403, message, 'FORBIDDEN', details);
  }
}

export class NotFoundException extends AppError {
  constructor(message = 'Not Found', details?: any) {
    super(404, message, 'NOT_FOUND', details);
  }
}

export class ConflictException extends AppError {
  constructor(message = 'Conflict', details?: any) {
    super(409, message, 'CONFLICT', details);
  }
}

export class UnprocessableEntityException extends AppError {
  constructor(message = 'Unprocessable Entity', details?: any) {
    super(422, message, 'UNPROCESSABLE_ENTITY', details);
  }
}

export class InternalServerErrorException extends AppError {
  constructor(message = 'Internal Server Error', details?: any) {
    super(500, message, 'INTERNAL_SERVER_ERROR', details);
  }
}

export class ServiceUnavailableException extends AppError {
  constructor(message = 'Service Unavailable', details?: any) {
    super(503, message, 'SERVICE_UNAVAILABLE', details);
  }
}

export class DatabaseException extends AppError {
  constructor(message = 'Database Error', details?: any) {
    super(500, message, 'DATABASE_ERROR', details);
  }
}
