import { Logger } from '../../../shared/logger';
import type { TableHandlerService } from './table-handler.service';
import type { RuntimeSchemaUnitOfWorkService } from './runtime-schema-unit-of-work.service';
import type { RuntimeSchemaJournalService } from './runtime-schema-journal.service';
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
  private readonly journal: RuntimeSchemaJournalService;
  private readonly databaseConfigService: DatabaseConfigService;

  constructor(deps: {
    tableHandlerService: TableHandlerService;
    runtimeSchemaUnitOfWorkService: RuntimeSchemaUnitOfWorkService;
    runtimeSchemaJournalService: RuntimeSchemaJournalService;
    databaseConfigService: DatabaseConfigService;
  }) {
    this.tableHandlerService = deps.tableHandlerService;
    this.unitOfWork = deps.runtimeSchemaUnitOfWorkService;
    this.journal = deps.runtimeSchemaJournalService;
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

    await this.journal.create({
      mutationId,
      contractHash: contract.contractHash,
      backend: contract.backend,
    });
    stage('captured');

    let result: any;
    try {
      result = await this.unitOfWork.run(async () => {
        stage('executing');
        await this.journal.advanceStage(mutationId, 'executing');

        const tableResult: any = await this.tableHandlerService.updateTable(
          ownerTableId,
          body,
          context,
        );

        if (tableResult?._preview) {
          return { preview: tableResult };
        }

        stage('target_attested');
        await this.journal.advanceStage(mutationId, 'target_attested');
        stage('db_committed');
        await this.journal.advanceStage(mutationId, 'db_committed');

        return {
          affectedTables: tableResult?.affectedTables as string[] | undefined,
          tableRenames: tableResult?.tableRenames,
        };
      });
    } catch (error: any) {
      await this.journal.markFailed(mutationId, error.message);
      throw error;
    }

    if ('preview' in result && result.preview) {
      await this.journal.markCompleted(mutationId);
      return {
        mutationId,
        contractHash: contract.contractHash,
        outputs: new Map(),
        affectedTables: [],
      };
    }

    const affectedTables = (result as any).affectedTables ?? [];
    stage('completed');
    await this.journal.markCompleted(mutationId);

    return {
      mutationId,
      contractHash: contract.contractHash,
      outputs: new Map(),
      affectedTables,
    };
  }
}
