import { BaseTableProcessor } from './base-table-processor';
import { IQueryBuilder } from '../../shared/interfaces/query-builder.interface';

export class FlowStepDefinitionProcessor extends BaseTableProcessor {
  private readonly queryBuilderService: IQueryBuilder;
  constructor(deps: { queryBuilderService: IQueryBuilder }) {
    super();
    this.queryBuilderService = deps.queryBuilderService;
  }

  async transformRecords(records: any[], _context?: any): Promise<any[]> {
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
          this.queryBuilderService,
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
