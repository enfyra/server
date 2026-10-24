import { BaseTableProcessor } from './base-table-processor';
import { IQueryBuilder } from '../../shared/interfaces/query-builder.interface';
import { mapSequentially } from '../utils/map-sequentially.util';

export class FlowStepDefinitionProcessor extends BaseTableProcessor {
  private readonly queryBuilderService: IQueryBuilder;
  constructor(deps: { queryBuilderService: IQueryBuilder }) {
    super();
    this.queryBuilderService = deps.queryBuilderService;
  }

  override async transformRecords(records: any[], _context?: any): Promise<any[]> {
    return mapSequentially(records, async (record) => {
      const transformed = { ...record };
      if (transformed.isEnabled === undefined) transformed.isEnabled = true;
      if (transformed.stepOrder === undefined) transformed.stepOrder = 0;
      if (transformed.onError === undefined) transformed.onError = 'stop';
      if (transformed.retryAttempts === undefined)
        transformed.retryAttempts = 0;
      if (transformed.timeout === undefined) transformed.timeout = 5000;

      return this.autoTransformFkFields(
        transformed,
        'enfyra_flow_step',
        this.queryBuilderService,
      );
    });
  }

  getUniqueIdentifier(record: any): object {
    return this.autoGetUniqueIdentifier(record, 'enfyra_flow_step');
  }

  protected override getCompareFields(): string[] {
    return this.autoGetCompareFields('enfyra_flow_step');
  }

  protected override getRecordIdentifier(record: any): string {
    return `[FlowStep] ${record.key}`;
  }
}
