import { Module } from '@nestjs/common';
import { AdminController } from './admin.controller';
import { MetadataController } from './metadata.controller';
import { LogController } from './log.controller';
import { LogReaderService } from './services/log-reader.service';
import { AdminSocketHandler } from './services/admin-socket.handler';

@Module({
  controllers: [AdminController, MetadataController, LogController],
  providers: [LogReaderService, AdminSocketHandler],
})
export class AdminModule {}