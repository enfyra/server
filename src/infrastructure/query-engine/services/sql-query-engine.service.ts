// @nestjs packages
import { Injectable } from '@nestjs/common';

// Internal imports
import { QueryBuilderService } from '../../query-builder/query-builder.service';
import { LoggingService } from '../../../core/exceptions/services/logging.service';
import {
  DatabaseQueryException,
  ResourceNotFoundException,
} from '../../../core/exceptions/custom-exceptions';

@Injectable()
export class SqlQueryEngine {
  constructor(
    private queryBuilder: QueryBuilderService,
    private loggingService: LoggingService,
  ) {}

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
    try {
      // Default fields to '*' if not provided (trigger auto-join)
      const fields = options.fields || '*';

      return await this.queryBuilder.select({
        tableName: options.tableName,
        fields: fields,
        filter: options.filter,
        sort: options.sort,
        page: options.page,
        limit: options.limit,
        meta: options.meta,
        deep: options.deep,
        debugMode: options.debugMode,
      });
    } catch (error) {
      this.loggingService.error('Query execution failed', {
        context: 'find',
        error: error.message,
        stack: error.stack,
        tableName: options.tableName,
        fields: options.fields,
        filterPresent: !!options.filter,
        sortPresent: !!options.sort,
        page: options.page,
        limit: options.limit,
        hasDeepRelations: options.deep && Object.keys(options.deep).length > 0,
      });

      // Handle specific database errors
      if (
        error.message?.includes('relation') &&
        error.message?.includes('does not exist')
      ) {
        throw new ResourceNotFoundException(
          'Table or Relation',
          options.tableName,
        );
      }

      if (
        error.message?.includes('column') &&
        error.message?.includes('does not exist')
      ) {
        throw new DatabaseQueryException(
          `Invalid column in query: ${error.message}`,
          {
            tableName: options.tableName,
            fields: options.fields,
            operation: 'query',
          },
        );
      }

      throw new DatabaseQueryException(`Query failed: ${error.message}`, {
        tableName: options.tableName,
        operation: 'find',
        originalError: error.message,
      });
    }
  }
}
