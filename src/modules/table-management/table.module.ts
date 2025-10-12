import { Global, Module } from '@nestjs/common';
import { TableHandlerService } from './services/table-handler.service';
import { SqlTableHandlerService } from './services/sql-table-handler.service';
import { MongoTableHandlerService } from './services/mongo-table-handler.service';

@Global()
@Module({
  imports: [],
  providers: [TableHandlerService, SqlTableHandlerService, MongoTableHandlerService],
  exports: [TableHandlerService],
})
export class TableModule {}
