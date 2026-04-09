import { Injectable } from '@nestjs/common';
import { BaseTableProcessor } from './base-table-processor';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';

@Injectable()
export class WebsocketDefinitionProcessor extends BaseTableProcessor {
  constructor(private readonly queryBuilder: QueryBuilderService) {
    super();
  }

  async transformRecords(records: any[], context?: any): Promise<any[]> {
    const isMongoDB = process.env.DB_TYPE === 'mongodb';

    const transformedRecords = await Promise.all(
      records.map(async (record) => {
        const transformedRecord = { ...record };

        if (transformedRecord.description === undefined)
          transformedRecord.description = null;
        if (transformedRecord.isSystem === undefined)
          transformedRecord.isSystem = false;
        if (transformedRecord.isEnabled === undefined)
          transformedRecord.isEnabled = true;
        if (transformedRecord.requireAuth === undefined)
          transformedRecord.requireAuth = true;
        if (transformedRecord.connectionHandlerScript === undefined)
          transformedRecord.connectionHandlerScript = null;
        if (transformedRecord.connectionHandlerTimeout === undefined)
          transformedRecord.connectionHandlerTimeout = 5000;

        if (isMongoDB) {
          const now = new Date();
          if (!transformedRecord.createdAt) transformedRecord.createdAt = now;
          if (!transformedRecord.updatedAt) transformedRecord.updatedAt = now;
        }

        return transformedRecord;
      }),
    );

    return transformedRecords.filter(Boolean);
  }

  getUniqueIdentifier(record: any): object {
    return { path: record.path };
  }

  protected getCompareFields(): string[] {
    return [
      'path',
      'isEnabled',
      'isSystem',
      'description',
      'requireAuth',
      'connectionHandlerScript',
      'connectionHandlerTimeout',
    ];
  }

  protected getRecordIdentifier(record: any): string {
    return `[WebSocket Gateway] ${record.path}`;
  }
}
