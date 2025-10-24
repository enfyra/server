import { forwardRef, Inject, Injectable } from '@nestjs/common';
import { SqlQueryEngine } from './sql-query-engine.service';
import { MongoQueryEngine } from './mongo-query-engine.service';

@Injectable()
export class QueryEngine {

  constructor(
    @Inject(forwardRef(() => SqlQueryEngine))
    private sqlQueryEngine: SqlQueryEngine,
    @Inject(forwardRef(() => MongoQueryEngine))
    private mongoQueryEngine: MongoQueryEngine,
  ) {}

  private getEngine() {
    const dbType = process.env.DB_TYPE || 'mysql';
    
    if (dbType === 'mongodb') {
      return this.mongoQueryEngine;
    }
    
    return this.sqlQueryEngine;
  }

  async find(params: any) {
    return this.getEngine().find(params);
  }
}
