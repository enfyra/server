import { BaseTableProcessor } from './base-table-processor';
import { IQueryBuilder } from '../../shared/interfaces/query-builder.interface';
import { ObjectId } from 'mongodb';
import { DatabaseConfigService } from '../../../shared/services/database-config.service';

export class GraphQLDefinitionProcessor extends BaseTableProcessor {
  private readonly queryBuilderService: IQueryBuilder;
  constructor(deps: { queryBuilderService: IQueryBuilder }) {
    super();
    this.queryBuilderService = deps.queryBuilderService;
  }

  async transformRecords(records: any[], _context?: any): Promise<any[]> {
    const isMongoDB = DatabaseConfigService.instanceIsMongoDb();

    const transformedRecords = await Promise.all(
      records.map(async (record) => {
        const transformedRecord = { ...record };

        if (transformedRecord.description === undefined)
          transformedRecord.description = null;
        if (transformedRecord.isSystem === undefined)
          transformedRecord.isSystem = false;
        if (transformedRecord.isEnabled === undefined)
          transformedRecord.isEnabled = true;

        if (isMongoDB) {
          const now = new Date();
          if (!transformedRecord.createdAt) transformedRecord.createdAt = now;
          if (!transformedRecord.updatedAt) transformedRecord.updatedAt = now;
        }

        if (record.table) {
          if (isMongoDB) {
            const table = await this.queryBuilderService.findOne({
              table: 'table_definition',
              where: { name: record.table },
            });
            if (!table) {
              this.logger.warn(
                `Table '${record.table}' not found for GraphQL definition, skipping.`,
              );
              return null;
            }
            transformedRecord.table =
              typeof table._id === 'string'
                ? new ObjectId(table._id)
                : table._id;
          } else {
            const table = await this.queryBuilderService.findOne({
              table: 'table_definition',
              where: { name: record.table },
            });
            if (!table) {
              this.logger.warn(
                `Table '${record.table}' not found for GraphQL definition, skipping.`,
              );
              return null;
            }
            transformedRecord.tableId = table.id;
            delete transformedRecord.table;
          }
        }

        return transformedRecord;
      }),
    );

    return transformedRecords.filter(Boolean);
  }

  getUniqueIdentifier(record: any): object {
    return this.autoGetUniqueIdentifier(record, 'gql_definition');
  }

  protected getCompareFields(): string[] {
    return ['table', 'isEnabled', 'description', 'metadata'];
  }

  protected getRecordIdentifier(record: any): string {
    return `[GqlDefinition] ${record.table || record.tableId}`;
  }
}
