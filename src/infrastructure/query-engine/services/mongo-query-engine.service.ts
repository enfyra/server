import { Injectable } from '@nestjs/common';
import { QueryBuilderService } from '../../query-builder/query-builder.service';


@Injectable()
export class MongoQueryEngine {
  constructor(
    private queryBuilder: QueryBuilderService,
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

    const normalizedFields = fields || '*';
    const debugLog: any[] = [];

    const result = await this.queryBuilder.select({
      tableName,
      fields: normalizedFields,
      filter,
      sort,
      page,
      limit,
      meta,
      debugLog,
    });

    if (debugMode) {
      return {
        ...result,
        debug: debugLog,
      };
    }

    return result;
  }
}
