import { Module } from '@nestjs/common';
import { AdminController } from './admin.controller';
import { LogController } from './log.controller';
import { LogReaderService } from './services/log-reader.service';

@Module({
  controllers: [AdminController, LogController],
  providers: [LogReaderService],
})
export class AdminModule {}