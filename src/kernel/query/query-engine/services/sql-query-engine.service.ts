import { QueryBuilderService } from '../../query-builder/query-builder.service';
import {
  LoggingService,
  DatabaseQueryException,
  ResourceNotFoundException,
} from '../../../../domain/exceptions';
import {
  getErrorMessage,
  getErrorStack,
} from '../../../../shared/utils/error.util';

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
      const errMsg = getErrorMessage(error);
      const errStack = getErrorStack(error);
      this.loggingService.error('Query execution failed', {
        context: 'find',
        error: errMsg,
        stack: errStack,
        table: options.table,
        fields: options.fields,
        filterPresent: !!options.filter,
        sortPresent: !!options.sort,
        page: options.page,
        limit: options.limit,
        hasDeepRelations: options.deep && Object.keys(options.deep).length > 0,
      });

      if (errMsg.includes('relation') && errMsg.includes('does not exist')) {
        throw new ResourceNotFoundException('Table or Relation', options.table);
      }

      if (errMsg.includes('column') && errMsg.includes('does not exist')) {
        throw new DatabaseQueryException(`Invalid column in query: ${errMsg}`, {
          table: options.table,
          fields: options.fields,
          operation: 'query',
        });
      }

      throw new DatabaseQueryException(`Query failed: ${errMsg}`, {
        table: options.table,
        operation: 'find',
        originalError: errMsg,
      });
    }
  }
}
