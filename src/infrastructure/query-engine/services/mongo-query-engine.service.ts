import { QueryBuilderService } from '../../query-builder/query-builder.service';

export class MongoQueryEngine {
  private readonly queryBuilderService: QueryBuilderService;

  constructor(deps: { queryBuilderService: QueryBuilderService }) {
    this.queryBuilderService = deps.queryBuilderService;
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

    const result = await this.queryBuilderService.find({
      table: tableName,
      fields: normalizedFields,
      filter,
      sort,
      page,
      limit,
      meta,
      debugMode,
      debugTrace: (options as any).debugTrace,
    });

    return result;
  }
}
