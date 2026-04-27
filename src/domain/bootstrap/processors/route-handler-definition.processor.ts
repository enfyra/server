import { BaseTableProcessor } from './base-table-processor';
import { IQueryBuilder } from '../../shared/interfaces/query-builder.interface';
import { DatabaseConfigService } from '../../../shared/services';
import { normalizeScriptRecord } from '../../../kernel/execution';

export class RouteHandlerDefinitionProcessor extends BaseTableProcessor {
  private readonly queryBuilderService: IQueryBuilder;
  constructor(deps: { queryBuilderService: IQueryBuilder }) {
    super();
    this.queryBuilderService = deps.queryBuilderService;
  }
  async transformRecords(records: any[], _context?: any): Promise<any[]> {
    const isMongoDB = DatabaseConfigService.instanceIsMongoDb();
    const transformedRecords = await Promise.all(
      records.map(async (handler) => {
        if (handler.route && typeof handler.route === 'string') {
          const route = await this.queryBuilderService.findOne({
            table: 'route_definition',
            where: {
              path: handler.route,
            },
          });
          if (!route) {
            this.logger.warn(
              `Route '${handler.route}' not found for handler ${handler.name}, skipping.`,
            );
            return null;
          }
          if (isMongoDB) {
            handler.route = route._id;
          } else {
            handler.routeId = route.id;
            delete handler.route;
          }
        }
        if (handler.method && typeof handler.method === 'string') {
          const method = await this.queryBuilderService.findOne({
            table: 'method_definition',
            where: {
              method: handler.method,
            },
          });
          if (!method) {
            this.logger.warn(
              `Method '${handler.method}' not found for handler ${handler.name}, skipping.`,
            );
            return null;
          }
          if (isMongoDB) {
            handler.method = method._id;
          } else {
            handler.methodId = method.id;
            delete handler.method;
          }
        }
        if (handler.isEnabled === undefined) handler.isEnabled = true;
        if (isMongoDB) {
          const now = new Date();
          if (!handler.createdAt) handler.createdAt = now;
          if (!handler.updatedAt) handler.updatedAt = now;
        }
        return normalizeScriptRecord('route_handler_definition', handler);
      }),
    );
    return transformedRecords.filter(Boolean);
  }
  getUniqueIdentifier(record: any): object {
    return {
      route: record.route || record.routeId,
      method: record.method || record.methodId,
    };
  }
  protected getCompareFields(): string[] {
    return ['name', 'sourceCode', 'scriptLanguage', 'compiledCode', 'timeout', 'isEnabled'];
  }
  protected getRecordIdentifier(record: any): string {
    return `[RouteHandler] ${record.name}`;
  }
}
