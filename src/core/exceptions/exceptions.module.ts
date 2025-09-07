import { Global, Module } from '@nestjs/common';
import { APP_FILTER } from '@nestjs/core';
import { LoggingService } from './services/logging.service';
import { GlobalExceptionFilter } from './filters/global-exception.filter';
import { RequestContextMiddleware } from './middleware/request-context.middleware';

@Global()
@Module({
  providers: [
    LoggingService,
    RequestContextMiddleware,
    {
      provide: APP_FILTER,
      useClass: GlobalExceptionFilter,
    },
  ],
  exports: [LoggingService, RequestContextMiddleware],
})
export class ExceptionsModule {}
