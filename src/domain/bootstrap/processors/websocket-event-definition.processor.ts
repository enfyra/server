import { BaseTableProcessor } from './base-table-processor';
import { IQueryBuilder } from '../../shared/interfaces/query-builder.interface';
import { DatabaseConfigService } from '../../../shared/services';
import { normalizeScriptRecord } from '../../../kernel/execution';

export class WebsocketEventDefinitionProcessor extends BaseTableProcessor {
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

        if (transformed.description === undefined)
          transformed.description = null;
        if (transformed.isSystem === undefined) transformed.isSystem = false;
        if (transformed.isEnabled === undefined) transformed.isEnabled = true;
        if (transformed.handlerScript === undefined)
          transformed.handlerScript = null;
        if (transformed.sourceCode === undefined)
          transformed.sourceCode = transformed.handlerScript;
        if (transformed.timeout === undefined) transformed.timeout = 5000;

        if (isMongoDB) {
          const now = new Date();
          if (!transformed.createdAt) transformed.createdAt = now;
          if (!transformed.updatedAt) transformed.updatedAt = now;
        }

        const result = await this.autoTransformFkFields(
          transformed,
          'websocket_event_definition',
          this.queryBuilderService,
        );
        if (!result.gateway && !result.gatewayId) return null;
        return normalizeScriptRecord('websocket_event_definition', result);
      }),
    );

    return transformedRecords.filter(Boolean);
  }

  getUniqueIdentifier(record: any): object {
    return this.autoGetUniqueIdentifier(record, 'websocket_event_definition');
  }

  protected getCompareFields(): string[] {
    return this.autoGetCompareFields('websocket_event_definition');
  }

  protected getRecordIdentifier(record: any): string {
    return `[WebSocket Event] ${record.eventName}`;
  }
}
