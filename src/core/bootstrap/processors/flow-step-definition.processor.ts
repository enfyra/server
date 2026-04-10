import { Injectable } from '@nestjs/common';
import { BaseTableProcessor } from './base-table-processor';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';

@Injectable()
export class FlowStepDefinitionProcessor extends BaseTableProcessor {
  constructor(private readonly queryBuilder: QueryBuilderService) {
    super();
  }

  async transformRecords(records: any[], context?: any): Promise<any[]> {
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

        return this.autoTransformFkFields(
          transformed,
          'flow_step_definition',
          this.queryBuilder,
        );
      }),
    );
  }

  getUniqueIdentifier(record: any): object {
    return this.autoGetUniqueIdentifier(record, 'flow_step_definition');
  }

  protected getCompareFields(): string[] {
    return this.autoGetCompareFields('flow_step_definition');
  }

  protected getRecordIdentifier(record: any): string {
    return `[FlowStep] ${record.key}`;
  }
}
