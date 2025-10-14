import { forwardRef, Inject, Injectable, Logger } from '@nestjs/common';
import { SqlQueryEngine } from './sql-query-engine.service';
import { MongoQueryEngine } from './mongo-query-engine.service';

/**
 * QueryEngine - Router for database-specific query engines
 * Routes to SqlQueryEngine or MongoQueryEngine based on DB_TYPE
 */
@Injectable()
export class QueryEngine {
  private logger = new Logger(QueryEngine.name);

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

  /**
   * Reload query engine (for metadata changes)
   */
  async reload(): Promise<void> {
    this.logger.log('🔄 Reloading QueryEngine...');
    // QueryEngine is stateless, no need to reload
    // Metadata changes are handled by MetadataCacheService
    this.logger.log('✅ QueryEngine reloaded');
  }
}
