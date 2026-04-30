import { BaseTableProcessor } from './base-table-processor';
import { IQueryBuilder } from '../../shared/interfaces/query-builder.interface';
import { DatabaseConfigService } from '../../../shared/services';

export class RoutePermissionDefinitionProcessor extends BaseTableProcessor {
  private readonly queryBuilderService: IQueryBuilder;
  constructor(deps: { queryBuilderService: IQueryBuilder }) {
    super();
    this.queryBuilderService = deps.queryBuilderService;
  }

  async transformRecords(records: any[], _context?: any): Promise<any[]> {
    const isMongoDB = DatabaseConfigService.instanceIsMongoDb();
    const transformedRecords = await Promise.all(
      records.map(async (record) => {
        const transformed = { ...record };
        if (transformed.isEnabled === undefined) transformed.isEnabled = true;
        if (isMongoDB) {
          const now = new Date();
          if (!transformed.createdAt) transformed.createdAt = now;
          if (!transformed.updatedAt) transformed.updatedAt = now;
          if (!transformed.allowedUsers) transformed.allowedUsers = [];
        }

        const result = await this.autoTransformFkFields(
          transformed,
          'route_permission_definition',
          this.queryBuilderService,
        );
        if (!result.route && !result.routeId) return null;
        if (!result.methods || result.methods.length === 0) return null;
        return result;
      }),
    );
    return transformedRecords.filter(Boolean);
  }

  getUniqueIdentifier(record: any): object {
    return this.autoGetUniqueIdentifier(record, 'route_permission_definition');
  }

  protected getCompareFields(): string[] {
    return ['isEnabled', 'isSystem'];
  }
}
