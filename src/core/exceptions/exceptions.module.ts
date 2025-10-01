import { Global, Module } from '@nestjs/common';
import { APP_FILTER } from '@nestjs/core';
import { LoggingService } from './services/logging.service';
import { GlobalExceptionFilter } from './filters/global-exception.filter';

@Global()
@Module({
  providers: [
    LoggingService,
    {
      provide: APP_FILTER,
      useClass: GlobalExceptionFilter,
    },
  ],
  exports: [LoggingService],
})
export class ExceptionsModule {}
