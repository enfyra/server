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
import { verifySchemaMutationContractHash } from '../../../shared/utils/schema-mutation-contract.util';
import { normalizeRuntimeTableSchema } from '../utils/runtime-schema-normalization.util';
import { hashCanonical } from '../../../shared/utils/schema-mutation-contract.util';
import type { RuntimeRegistryService } from '../../../engines/cache';

export class RuntimeSchemaExecutorService {
  private readonly logger = new Logger(RuntimeSchemaExecutorService.name);
  private readonly tableHandlerService: TableHandlerService;
  private readonly unitOfWork: RuntimeSchemaUnitOfWorkService;
  private readonly journal: RuntimeSchemaJournalService;
  private readonly databaseConfigService: DatabaseConfigService;
  private readonly runtimeRegistryService: RuntimeRegistryService;

  constructor(deps: {
    tableHandlerService: TableHandlerService;
    runtimeSchemaUnitOfWorkService: RuntimeSchemaUnitOfWorkService;
    runtimeSchemaJournalService: RuntimeSchemaJournalService;
    databaseConfigService: DatabaseConfigService;
    runtimeRegistryService: RuntimeRegistryService;
  }) {
    this.tableHandlerService = deps.tableHandlerService;
    this.unitOfWork = deps.runtimeSchemaUnitOfWorkService;
    this.journal = deps.runtimeSchemaJournalService;
    this.databaseConfigService = deps.databaseConfigService;
    this.runtimeRegistryService = deps.runtimeRegistryService;
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

    this.verifyContractIntegrity(contract, ownerTableId, body);
    this.attestSourceRevision(contract);

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

        const executionContext: any = {
          ...(context ?? {}),
          $schemaContract: {
            contract,
            requiredConfirmHash: contract.context.confirmationDigest,
          },
        };

        const tableResult: any = await this.tableHandlerService.updateTable(
          ownerTableId,
          body,
          executionContext,
        );

        if (tableResult?._preview) {
          return { preview: tableResult };
        }

        stage('target_attested');
        await this.journal.advanceStage(mutationId, 'target_attested');

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
      await this.journal.markFailed(mutationId, 'preview_returned_no_write');
      return {
        mutationId,
        contractHash: contract.contractHash,
        outputs: new Map(),
        affectedTables: [],
        preview: result.preview,
      } as any;
    }

    stage('db_committed');
    await this.journal.advanceStage(mutationId, 'db_committed');

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

  private verifyContractIntegrity(
    contract: RuntimeSchemaMutationContract,
    ownerTableId: string | number,
    body: TCreateTableBody,
  ): void {
    if (!verifySchemaMutationContractHash(contract)) {
      throw new Error(
        `Schema mutation contract hash integrity check failed for ${contract.mutationId}`,
      );
    }
    const contractTableId = contract.context.tableId;
    if (contractTableId != null && String(contractTableId) !== String(ownerTableId)) {
      throw new Error(
        `Schema mutation contract tableId mismatch: contract=${contractTableId}, executor=${ownerTableId}`,
      );
    }
    const contractTableName = contract.context.tableName;
    if (contractTableName && body.name && body.name !== contractTableName) {
      throw new Error(
        `Schema mutation contract tableName mismatch: contract=${contractTableName}, body=${body.name}`,
      );
    }
  }

  private attestSourceRevision(contract: RuntimeSchemaMutationContract): void {
    const expectedRevision = contract.context.sourceRevision;
    if (!expectedRevision) return;
    const tableName = contract.context.tableName;
    try {
      const metadata = this.runtimeRegistryService.getMetadata();
      if (!metadata) return;
      const tableMeta = metadata.tables.get(tableName);
      if (!tableMeta) return;
      const normalized = normalizeRuntimeTableSchema(tableMeta);
      if (!normalized) return;
      const currentRevision = hashCanonical(normalized.contract);
      if (currentRevision !== expectedRevision) {
        throw new Error(
          `Schema mutation source revision stale: expected=${expectedRevision}, current=${currentRevision}. Re-compile the contract.`,
        );
      }
    } catch (error: any) {
      if (error.message?.includes('source revision stale')) throw error;
      this.logger.warn(
        `[${contract.mutationId}] source attestation skipped: ${error.message}`,
      );
    }
  }
}
