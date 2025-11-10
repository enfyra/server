import { Injectable, BadRequestException } from '@nestjs/common';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';

@Injectable()
export class TableValidationService {
  constructor(
    private readonly queryBuilder: QueryBuilderService,
  ) {}

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
    tableMetadata: any,
  ) {
    const idField = this.queryBuilder.isMongoDb() ? '_id' : 'id';
    const existingResult = await this.queryBuilder.findWhere(tableName, {});
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

