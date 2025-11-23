import { forwardRef, Inject, Injectable } from '@nestjs/common';
import { SqlQueryEngine } from './sql-query-engine.service';
import { MongoQueryEngine } from './mongo-query-engine.service';
import { QueryBuilderService } from '../../query-builder/query-builder.service';

@Injectable()
export class QueryEngine {
  private cachedEngine: SqlQueryEngine | MongoQueryEngine | null = null;

  constructor(
    @Inject(forwardRef(() => SqlQueryEngine))
    private sqlQueryEngine: SqlQueryEngine,
    @Inject(forwardRef(() => MongoQueryEngine))
    private mongoQueryEngine: MongoQueryEngine,
    @Inject(forwardRef(() => QueryBuilderService))
    private queryBuilder: QueryBuilderService,
  ) {}

  private getEngine() {
    if (this.cachedEngine) {
      return this.cachedEngine;
    }

    if (this.queryBuilder.isMongoDb()) {
      this.cachedEngine = this.mongoQueryEngine;
    } else {
      this.cachedEngine = this.sqlQueryEngine;
    }

    return this.cachedEngine;
  }

  async find(params: any) {
    return this.getEngine().find(params);
  }
}
