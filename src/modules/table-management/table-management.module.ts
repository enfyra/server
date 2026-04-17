import { Global, Module } from '@nestjs/common';
import { TableHandlerService } from './services/table-handler.service';
import { SqlTableHandlerService } from './services/sql-table-handler.service';
import { MongoTableHandlerService } from './services/mongo-table-handler.service';
import { TableValidationService } from './services/table-validation.service';
import { MongoMetadataSnapshotService } from './services/mongo-metadata-snapshot.service';
import { SqlTableMetadataBuilderService } from './services/sql-table-metadata-builder.service';
import { SqlTableMetadataWriterService } from './services/sql-table-metadata-writer.service';

@Global()
@Module({
  imports: [],
  providers: [
    TableHandlerService,
    SqlTableHandlerService,
    MongoTableHandlerService,
    TableValidationService,
    MongoMetadataSnapshotService,
    SqlTableMetadataBuilderService,
    SqlTableMetadataWriterService,
  ],
  exports: [TableHandlerService],
})
export class TableManagementModule {}
