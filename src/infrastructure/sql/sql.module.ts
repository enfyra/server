import { Module } from '@nestjs/common';
import { SqlFunctionService } from './services/sql-function.service';

@Module({
  providers: [SqlFunctionService],
  exports: [SqlFunctionService],
})
export class SqlModule {}
