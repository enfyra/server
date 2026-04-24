import { BadRequestException } from '../../../domain/exceptions/custom-exceptions';
import { QueryBuilderService } from '../../../engine/query-builder/query-builder.service';

export class DynamicApiTableValidationService {
  private readonly queryBuilderService: QueryBuilderService;

  constructor(deps: { queryBuilderService: QueryBuilderService }) {
    this.queryBuilderService = deps.queryBuilderService;
  }

  async assertTableValid({
    operation,
    tableName,
    tableMetadata,
  }: {
    operation: 'create' | 'update' | 'delete';
    tableName: string;
    tableMetadata: any;
  }) {
    if (!tableMetadata) {
      return;
    }

    if (tableMetadata.isSingleRecord) {
      await this.assertSingleRecordRule(operation, tableName, tableMetadata);
    }
  }

  private async assertSingleRecordRule(
    operation: 'create' | 'update' | 'delete',
    tableName: string,
    _tableMetadata: any,
  ) {
    const { data: existingResult } = await this.queryBuilderService.find({
      table: tableName,
    });
    const existingRecords = existingResult || [];
    const hasExistingRecord = existingRecords.length > 0;

    if (operation === 'create') {
      if (hasExistingRecord) {
        throw new BadRequestException(
          `Cannot create new record: table '${tableName}' is a single-record table and already has a record. Use update instead.`,
        );
      }
    }

    if (operation === 'delete') {
      throw new BadRequestException(
        `Cannot delete record: table '${tableName}' is a single-record table. Use update instead.`,
      );
    }
  }
}
