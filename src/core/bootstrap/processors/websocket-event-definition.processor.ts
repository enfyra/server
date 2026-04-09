import { Injectable } from '@nestjs/common';
import { BaseTableProcessor } from './base-table-processor';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { ObjectId } from 'mongodb';

@Injectable()
export class WebsocketEventDefinitionProcessor extends BaseTableProcessor {
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
        if (transformedRecord.handlerScript === undefined)
          transformedRecord.handlerScript = null;
        if (transformedRecord.timeout === undefined)
          transformedRecord.timeout = 5000;

        if (isMongoDB) {
          const now = new Date();
          if (!transformedRecord.createdAt) transformedRecord.createdAt = now;
          if (!transformedRecord.updatedAt) transformedRecord.updatedAt = now;
        }

        if (record.gateway) {
          if (isMongoDB) {
            const gateway = await this.queryBuilder.findOneWhere(
              'websocket_definition',
              {
                path: record.gateway,
              },
            );

            if (!gateway) {
              this.logger.warn(
                `WebSocket gateway '${record.gateway}' not found for event ${record.eventName}, skipping.`,
              );
              return null;
            }
            transformedRecord.gatewayId =
              typeof gateway._id === 'string'
                ? new ObjectId(gateway._id)
                : gateway._id;
            delete transformedRecord.gateway;
          } else {
            const gateway = await this.queryBuilder.findOneWhere(
              'websocket_definition',
              {
                path: record.gateway,
              },
            );

            if (!gateway) {
              this.logger.warn(
                `WebSocket gateway '${record.gateway}' not found for event ${record.eventName}, skipping.`,
              );
              return null;
            }

            transformedRecord.gatewayId = gateway.id;
            delete transformedRecord.gateway;
          }
        }

        return transformedRecord;
      }),
    );

    return transformedRecords.filter(Boolean);
  }

  getUniqueIdentifier(record: any): object {
    return { eventName: record.eventName, gatewayId: record.gatewayId };
  }

  protected getCompareFields(): string[] {
    return [
      'eventName',
      'isEnabled',
      'isSystem',
      'description',
      'handlerScript',
      'timeout',
    ];
  }

  protected getRecordIdentifier(record: any): string {
    return `[WebSocket Event] ${record.eventName}`;
  }
}
