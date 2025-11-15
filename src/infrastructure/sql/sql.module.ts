import { Global, Module } from '@nestjs/common';
import { SqlFunctionService } from './services/sql-function.service';

@Global()
@Module({
  providers: [SqlFunctionService],
  exports: [SqlFunctionService],
})
export class SqlModule {}
