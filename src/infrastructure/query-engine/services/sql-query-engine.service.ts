import { QueryBuilderService } from '../../query-builder/query-builder.service';
import { LoggingService } from '../../../core/exceptions/services/logging.service';
import {
  DatabaseQueryException,
  ResourceNotFoundException,
} from '../../../core/exceptions/custom-exceptions';

export class SqlQueryEngine {
  private readonly queryBuilderService: QueryBuilderService;
  private readonly loggingService: LoggingService;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    loggingService: LoggingService;
  }) {
    this.queryBuilderService = deps.queryBuilderService;
    this.loggingService = deps.loggingService;
  }

  async find(options: {
    table: string;
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
    try {
      const fields = options.fields || '*';

      const result = await this.queryBuilderService.find({
        table: options.table,
        fields: fields,
        filter: options.filter,
        sort: options.sort,
        page: options.page,
        limit: options.limit,
        meta: options.meta,
        deep: options.deep,
        debugMode: options.debugMode,
        debugTrace: (options as any).debugTrace,
        maxQueryDepth: (options as any).maxQueryDepth,
      });

      return result;
    } catch (error) {
      this.loggingService.error('Query execution failed', {
        context: 'find',
        error: error.message,
        stack: error.stack,
        table: options.table,
        fields: options.fields,
        filterPresent: !!options.filter,
        sortPresent: !!options.sort,
        page: options.page,
        limit: options.limit,
        hasDeepRelations: options.deep && Object.keys(options.deep).length > 0,
      });

      if (
        error.message?.includes('relation') &&
        error.message?.includes('does not exist')
      ) {
        throw new ResourceNotFoundException('Table or Relation', options.table);
      }

      if (
        error.message?.includes('column') &&
        error.message?.includes('does not exist')
      ) {
        throw new DatabaseQueryException(
          `Invalid column in query: ${error.message}`,
          {
            table: options.table,
            fields: options.fields,
            operation: 'query',
          },
        );
      }

      throw new DatabaseQueryException(`Query failed: ${error.message}`, {
        table: options.table,
        operation: 'find',
        originalError: error.message,
      });
    }
  }
}
