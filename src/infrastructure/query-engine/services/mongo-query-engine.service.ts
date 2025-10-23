// @nestjs packages
import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';

// Internal imports
import { QueryBuilderService } from '../../query-builder/query-builder.service';
import { MetadataCacheService } from '../../cache/services/metadata-cache.service';
import { LoggingService } from '../../../core/exceptions/services/logging.service';

/**
 * MongoQueryEngine - Delegates to QueryBuilderService for MongoDB queries
 * Maintains API compatibility with SqlQueryEngine
 */
@Injectable()
export class MongoQueryEngine {
  constructor(
    private queryBuilder: QueryBuilderService,
    private metadataCacheService: MetadataCacheService,
    private loggingService: LoggingService,
    private configService: ConfigService,
  ) {}

  /**
   * Find records using QueryBuilderService
   * Delegates all logic to QueryBuilderService.select() which has proper MongoDB support
   */
  async find(options: {
    tableName: string;
    fields?: string | string[];
    filter?: any;
    sort?: string | string[];
    page?: number;
    limit?: number;
    meta?: string;
    aggregate?: any;
    deep?: Record<string, any>;
    debugMode?: boolean;
  }): Promise<any> {
    const {
      tableName,
      fields,
      filter,
      sort,
      page,
      limit,
      meta,
      debugMode = false,
    } = options;

    // Default to '*' if no fields specified
    const normalizedFields = fields || '*';

    // Initialize debug log array
    const debugLog: any[] = [];

    // Delegate to QueryBuilderService.mongoExecutor() which handles:
    // - expandFieldsMongo() for proper field expansion
    // - Aggregation pipeline building with $lookup
    // - Nested relation population
    const result = await this.queryBuilder.mongoExecutor({
      tableName,
      fields: normalizedFields,
      filter,
      sort,
      page,
      limit,
      meta,
      debugLog,
    });

    // Attach debug log to result if debugMode is enabled
    if (debugMode) {
      return {
        ...result,
        debug: debugLog,
      };
    }

    return result;
  }
}
