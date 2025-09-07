import { Global, Module } from '@nestjs/common';
import { TableHandlerService } from './services/table-handler.service';

@Global()
@Module({
  imports: [],
  providers: [TableHandlerService],
  exports: [TableHandlerService],
})
export class TableModule {}
