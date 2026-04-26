import { SqlQueryEngine } from './sql-query-engine.service';
import { MongoQueryEngine } from './mongo-query-engine.service';
import { QueryBuilderService } from '../../query-builder/query-builder.service';

export class QueryEngine {
  private readonly sqlQueryEngine: SqlQueryEngine;
  private readonly mongoQueryEngine: MongoQueryEngine;
  private readonly queryBuilderService: QueryBuilderService;
  private cachedEngine: SqlQueryEngine | MongoQueryEngine | null = null;

  constructor(deps: {
    sqlQueryEngine: SqlQueryEngine;
    mongoQueryEngine: MongoQueryEngine;
    queryBuilderService: QueryBuilderService;
  }) {
    this.sqlQueryEngine = deps.sqlQueryEngine;
    this.mongoQueryEngine = deps.mongoQueryEngine;
    this.queryBuilderService = deps.queryBuilderService;
  }

  private getEngine() {
    if (this.cachedEngine) {
      return this.cachedEngine;
    }

    if (this.queryBuilderService.isMongoDb()) {
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
