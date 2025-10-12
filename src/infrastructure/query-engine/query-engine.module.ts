import { Global, Module } from '@nestjs/common';
import { QueryEngine } from './services/query-engine.service';
import { SqlQueryEngine } from './services/sql-query-engine.service';
import { MongoQueryEngine } from './services/mongo-query-engine.service';

@Global()
@Module({
  providers: [QueryEngine, SqlQueryEngine, MongoQueryEngine],
  exports: [QueryEngine],
})
export class QueryEngineModule {}
