import { Injectable } from '@nestjs/common';
import { QueryBuilderService } from '../../query-builder/query-builder.service';

@Injectable()
export class MongoQueryEngine {
  constructor(private queryBuilder: QueryBuilderService) {}

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
    const {
      table: tableName,
      fields,
      filter,
      sort,
      page,
      limit,
      meta,
      debugMode = false,
    } = options;

    const normalizedFields = fields || '*';

    const result = await this.queryBuilder.find({
      table: tableName,
      fields: normalizedFields,
      filter,
      sort,
      page,
      limit,
      meta,
      debugMode,
    });

    return result;
  }
}
