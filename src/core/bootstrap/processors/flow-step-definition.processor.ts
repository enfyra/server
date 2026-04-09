import { Injectable } from '@nestjs/common';
import { BaseTableProcessor } from './base-table-processor';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';

@Injectable()
export class FlowStepDefinitionProcessor extends BaseTableProcessor {
  constructor(private readonly queryBuilder: QueryBuilderService) {
    super();
  }

  async transformRecords(records: any[], context?: any): Promise<any[]> {
    const isMongoDB = process.env.DB_TYPE === 'mongodb';

    return Promise.all(
      records.map(async (record) => {
        const transformed = { ...record };
        if (transformed.isEnabled === undefined) transformed.isEnabled = true;
        if (transformed.stepOrder === undefined) transformed.stepOrder = 0;
        if (transformed.onError === undefined) transformed.onError = 'stop';
        if (transformed.retryAttempts === undefined)
          transformed.retryAttempts = 0;
        if (transformed.timeout === undefined) transformed.timeout = 5000;

        if (transformed.config && typeof transformed.config === 'object') {
          transformed.config = JSON.stringify(transformed.config);
        }

        if (record.flow && typeof record.flow === 'string') {
          const flow = await this.queryBuilder.findOneWhere('flow_definition', {
            name: record.flow,
          });
          if (flow) {
            if (isMongoDB) {
              transformed.flow = flow._id;
            } else {
              transformed.flowId = flow.id;
              delete transformed.flow;
            }
          }
        }

        return transformed;
      }),
    );
  }

  getUniqueIdentifier(record: any): object {
    return { key: record.key, flowId: record.flowId };
  }

  protected getCompareFields(): string[] {
    return [
      'key',
      'stepOrder',
      'type',
      'config',
      'timeout',
      'onError',
      'retryAttempts',
      'isEnabled',
    ];
  }

  protected getRecordIdentifier(record: any): string {
    return `[FlowStep] ${record.key}`;
  }
}
