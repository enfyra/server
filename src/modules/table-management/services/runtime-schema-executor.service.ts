import { Logger } from '../../../shared/logger';
import type { TableHandlerService } from './table-handler.service';
import type { RuntimeSchemaUnitOfWorkService } from './runtime-schema-unit-of-work.service';
import type { DatabaseConfigService } from '../../../shared/services';
import type { TCreateTableBody } from '../types/table-handler.types';
import type { TDynamicContext } from '../../../shared/types';
import type {
  RuntimeSchemaMutationContract,
} from '../types/runtime-schema-mutation.types';
import type {
  RuntimeSchemaExecutionResult,
  RuntimeSchemaJournalStage,
} from '../types/runtime-schema-executor.types';

export class RuntimeSchemaExecutorService {
  private readonly logger = new Logger(RuntimeSchemaExecutorService.name);
  private readonly tableHandlerService: TableHandlerService;
  private readonly unitOfWork: RuntimeSchemaUnitOfWorkService;
  private readonly databaseConfigService: DatabaseConfigService;

  constructor(deps: {
    tableHandlerService: TableHandlerService;
    runtimeSchemaUnitOfWorkService: RuntimeSchemaUnitOfWorkService;
    databaseConfigService: DatabaseConfigService;
  }) {
    this.tableHandlerService = deps.tableHandlerService;
    this.unitOfWork = deps.runtimeSchemaUnitOfWorkService;
    this.databaseConfigService = deps.databaseConfigService;
  }

  async execute(input: {
    contract: RuntimeSchemaMutationContract;
    ownerTableId: string | number;
    body: TCreateTableBody;
    context?: TDynamicContext;
    onStage?: (stage: RuntimeSchemaJournalStage) => void;
  }): Promise<RuntimeSchemaExecutionResult> {
    const { contract, ownerTableId, body, context, onStage } = input;
    const mutationId = contract.mutationId;
    const stage = (s: RuntimeSchemaJournalStage) => {
      this.logger.log(`[${mutationId}] stage: ${s}`);
      onStage?.(s);
    };

    stage('captured');

    const result = await this.unitOfWork.run(async () => {
      stage('executing');

      const tableResult: any = await this.tableHandlerService.updateTable(
        ownerTableId,
        body,
        context,
      );

      if (tableResult?._preview) {
        return { preview: tableResult };
      }

      stage('target_attested');
      stage('db_committed');

      return {
        affectedTables: tableResult?.affectedTables as string[] | undefined,
        tableRenames: tableResult?.tableRenames,
      };
    });

    if ('preview' in result && result.preview) {
      return {
        mutationId,
        contractHash: contract.contractHash,
        outputs: new Map(),
        affectedTables: [],
      };
    }

    const affectedTables = (result as any).affectedTables ?? [];
    stage('completed');

    return {
      mutationId,
      contractHash: contract.contractHash,
      outputs: new Map(),
      affectedTables,
    };
  }
}
