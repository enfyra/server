import { Global, Module } from '@nestjs/common';
import { QueryEngine } from './services/query-engine.service';

@Global()
@Module({
  providers: [QueryEngine],
  exports: [QueryEngine],
})
export class QueryEngineModule {}
