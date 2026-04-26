import { BaseTableProcessor } from './base-table-processor';
import { IQueryBuilder } from '../../shared/interfaces/query-builder.interface';
import { DatabaseConfigService } from '../../../shared/services/database-config.service';
import { normalizeScriptRecord } from '../../shared/script-code.util';

export class WebsocketDefinitionProcessor extends BaseTableProcessor {
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
        if (transformedRecord.requireAuth === undefined)
          transformedRecord.requireAuth = true;
        if (transformedRecord.connectionHandlerScript === undefined)
          transformedRecord.connectionHandlerScript = null;
        if (transformedRecord.sourceCode === undefined)
          transformedRecord.sourceCode =
            transformedRecord.connectionHandlerScript;
        if (transformedRecord.connectionHandlerTimeout === undefined)
          transformedRecord.connectionHandlerTimeout = 5000;

        if (isMongoDB) {
          const now = new Date();
          if (!transformedRecord.createdAt) transformedRecord.createdAt = now;
          if (!transformedRecord.updatedAt) transformedRecord.updatedAt = now;
        }

        return normalizeScriptRecord('websocket_definition', transformedRecord);
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
      'sourceCode',
      'scriptLanguage',
      'compiledCode',
      'connectionHandlerTimeout',
    ];
  }

  protected getRecordIdentifier(record: any): string {
    return `[WebSocket Gateway] ${record.path}`;
  }
}
