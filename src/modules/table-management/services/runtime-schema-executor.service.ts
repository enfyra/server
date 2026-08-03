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
  RuntimeSchemaCommandAdapter,
  RuntimeSchemaCommandAdapterContext,
} from '../types/runtime-schema-executor.types';
import { verifySchemaMutationContractHash } from '../../../shared/utils/schema-mutation-contract.util';
import { normalizeRuntimeTableSchema } from '../utils/runtime-schema-normalization.util';
import { hashCanonical } from '../../../shared/utils/schema-mutation-contract.util';
import type { QueryBuilderService } from '@enfyra/kernel';
import type { RuntimeSchemaTargetAttestorService } from './runtime-schema-target-attestor.service';
import { RuntimeSchemaDagExecutor } from './runtime-schema-dag-executor.service';

export class RuntimeSchemaExecutorService {
  private readonly logger = new Logger(RuntimeSchemaExecutorService.name);
  private readonly tableHandlerService: TableHandlerService;
  private readonly unitOfWork: RuntimeSchemaUnitOfWorkService;
  private readonly journal: RuntimeSchemaJournalService;
  private readonly databaseConfigService: DatabaseConfigService;
  private readonly queryBuilderService: QueryBuilderService;
  private readonly targetAttestor: RuntimeSchemaTargetAttestorService;

  constructor(deps: {
    tableHandlerService: TableHandlerService;
    runtimeSchemaUnitOfWorkService: RuntimeSchemaUnitOfWorkService;
    runtimeSchemaJournalService: RuntimeSchemaJournalService;
    databaseConfigService: DatabaseConfigService;
    queryBuilderService: QueryBuilderService;
    runtimeSchemaTargetAttestorService: RuntimeSchemaTargetAttestorService;
  }) {
    this.tableHandlerService = deps.tableHandlerService;
    this.unitOfWork = deps.runtimeSchemaUnitOfWorkService;
    this.journal = deps.runtimeSchemaJournalService;
    this.databaseConfigService = deps.databaseConfigService;
    this.queryBuilderService = deps.queryBuilderService;
    this.targetAttestor = deps.runtimeSchemaTargetAttestorService;
  }

  async execute(input: {
    contract: RuntimeSchemaMutationContract;
    ownerTableId?: string | number;
    body?: TCreateTableBody;
    tableId?: string | number;
    context?: TDynamicContext;
    onStage?: (stage: RuntimeSchemaJournalStage) => void;
  }): Promise<RuntimeSchemaExecutionResult> {
    const { contract, body, context, onStage } = input;
    const ownerTableId = input.ownerTableId ?? input.tableId;
    const mutationId = contract.mutationId;
    const stage = (s: RuntimeSchemaJournalStage) => {
      this.logger.log(`[${mutationId}] stage: ${s}`);
      onStage?.(s);
    };

    this.verifyContractIntegrity(contract, ownerTableId, body);

    await this.journal.create({
      mutationId,
      contractHash: contract.contractHash,
      backend: contract.backend,
    });
    stage('captured');

    const dag = this.buildDagExecutor(contract, ownerTableId, body, context);

    let result: any;
    try {
      result = await this.unitOfWork.run(async (unitOfWorkContext = {}) => {
        stage('executing');
        await this.journal.advanceStage(mutationId, 'executing', {
          sagaSessionId: unitOfWorkContext.sagaSessionId,
        });

        const dagResult = await dag.executePhases(contract, contract.phases, {
          stopBeforeNodeKind: 'commit-database',
          onNodeCompleted: async (nodeId, completedIds) => {
            await this.journal.advanceStage(mutationId, 'executing', {
              completedNodeIds: [...completedIds],
              sagaSessionId: unitOfWorkContext.sagaSessionId,
            });
          },
        });

        const handlerOutput = this.findHandlerOutput(dagResult.outputs);
        if (handlerOutput?.preview) {
          return { preview: handlerOutput.preview };
        }

        stage('target_attested');
        await this.journal.advanceStage(mutationId, 'target_attested', {
          completedNodeIds: dagResult.completedNodeIds,
        });

        return {
          recordId: handlerOutput?.recordId ?? ownerTableId,
          affectedTables: handlerOutput?.affectedTables as string[] | undefined,
          tableRenames: handlerOutput?.tableRenames,
        };
      }, contract);
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
      };
    }

    stage('db_committed');
    await this.journal.advanceStage(mutationId, 'db_committed', {
      completedNodeIds: [...(result as any).completedNodeIds ?? []],
    });

    const affectedTables = (result as any).affectedTables ?? [];
    stage('activation_pending');
    await this.journal.advanceStage(mutationId, 'activation_pending');

    return {
      mutationId,
      contractHash: contract.contractHash,
      outputs: new Map(),
      affectedTables,
      recordId: (result as any).recordId,
      tableRenames: (result as any).tableRenames,
    };
  }

  async markActivated(mutationId: string): Promise<void> {
    await this.journal.markCompleted(mutationId);
  }

  private verifyContractIntegrity(
    contract: RuntimeSchemaMutationContract,
    ownerTableId?: string | number,
    body?: TCreateTableBody,
  ): void {
    if (!verifySchemaMutationContractHash(contract)) {
      throw new Error(
        `Schema mutation contract hash integrity check failed for ${contract.mutationId}`,
      );
    }
    const activeBackend = this.databaseConfigService.isMongoDb()
      ? 'mongodb'
      : this.databaseConfigService.getDbType();
    const normalizeBackend = (b: string) =>
      b === 'postgres' ? 'postgresql' : b;
    if (contract.backend && normalizeBackend(contract.backend) !== normalizeBackend(activeBackend)) {
      throw new Error(
        `Schema mutation contract backend mismatch: contract=${contract.backend}, active=${activeBackend}`,
      );
    }
    const contractTableId = contract.context.tableId;
    if (contractTableId != null && ownerTableId != null && String(contractTableId) !== String(ownerTableId)) {
      throw new Error(
        `Schema mutation contract tableId mismatch: contract=${contractTableId}, executor=${ownerTableId}`,
      );
    }
    const expectedBodyRevision =
      contract.context.executionBodyRevision ?? contract.context.targetRevision;
    if (expectedBodyRevision && body) {
      const normalizedBody = normalizeRuntimeTableSchema(body, {
        backend: contract.backend,
        mode: 'intent',
      });
      const bodyRevision = normalizedBody
        ? hashCanonical(normalizedBody.contract)
        : null;
      if (bodyRevision !== expectedBodyRevision) {
        throw new Error(
          `Schema mutation execution body does not match target revision: expected=${expectedBodyRevision}, body=${bodyRevision ?? 'unavailable'}`,
        );
      }
    }
  }

  private async attestSourceRevision(contract: RuntimeSchemaMutationContract): Promise<void> {
    const expectedRevision = contract.context.sourceRevision;
    const tableName = contract.context.tableName;
    const pkField = this.databaseConfigService.isMongoDb() ? '_id' : 'id';
    const tableId = contract.context.tableId;
    const tableMeta = await this.queryBuilderService.findOne({
      table: 'enfyra_table',
      where: expectedRevision && tableId != null
        ? { [pkField]: tableId }
        : { name: tableName },
      fields: [
        '*',
        'columns.*',
        'relations.*',
        'relations.targetTable.name',
        'relations.mappedBy.id',
        'relations.mappedBy._id',
        'relations.mappedBy.propertyName',
      ],
    });
    if (!expectedRevision) {
      if (contract.context.operation === 'create' && tableMeta) {
        throw new Error(
          `[${contract.mutationId}] source attestation failed: table '${tableName}' already exists`,
        );
      }
      return;
    }
    if (!tableMeta) {
      throw new Error(
        `[${contract.mutationId}] source attestation failed: table '${tableName}' not found in database`,
      );
    }
    const normalized = normalizeRuntimeTableSchema(tableMeta, {
      backend: contract.backend,
      mode: 'persisted',
    });
    if (!normalized) {
      throw new Error(
        `[${contract.mutationId}] source attestation failed: could not normalize table '${tableName}'`,
      );
    }
    const currentRevision = hashCanonical(normalized.contract);
    if (currentRevision !== expectedRevision) {
      throw new Error(
        `Schema mutation source revision stale: expected=${expectedRevision}, current=${currentRevision}. Re-compile the contract.`,
      );
    }
  }

  private async attestTargetRevision(
    contract: RuntimeSchemaMutationContract,
    tableId?: string | number,
  ): Promise<void> {
    const expectedRevision = contract.context.targetRevision;
    const tableName = contract.context.target?.name ?? contract.context.tableName;
    const pkField = this.databaseConfigService.isMongoDb() ? '_id' : 'id';
    const tableMeta = await this.queryBuilderService.findOne({
      table: 'enfyra_table',
      where: tableId != null
        ? { [pkField]: tableId }
        : { name: tableName },
      fields: [
        '*',
        'columns.*',
        'relations.*',
        'relations.targetTable.name',
        'relations.mappedBy.id',
        'relations.mappedBy._id',
        'relations.mappedBy.propertyName',
      ],
    });

    if (!expectedRevision) {
      if (tableMeta) {
        throw new Error(
          `[${contract.mutationId}] target attestation failed: table '${tableName}' still exists`,
        );
      }
      return;
    }
    if (!tableMeta) {
      throw new Error(
        `[${contract.mutationId}] target attestation failed: table '${tableName}' not found in database`,
      );
    }
    const normalized = normalizeRuntimeTableSchema(tableMeta, {
      backend: contract.backend,
      mode: 'persisted',
    });
    if (!normalized) {
      throw new Error(
        `[${contract.mutationId}] target attestation failed: could not normalize table '${tableName}'`,
      );
    }
    const currentRevision = hashCanonical(normalized.contract);
    if (currentRevision !== expectedRevision) {
      throw new Error(
        `Schema mutation target revision mismatch: expected=${expectedRevision}, current=${currentRevision}. The database unit of work will be rolled back.`,
      );
    }
  }

  private findHandlerOutput(
    outputs: Map<string, Record<string, unknown>>,
  ): Record<string, unknown> | undefined {
    for (const output of outputs.values()) {
      if (output.recordId !== undefined || output.preview !== undefined) {
        return output;
      }
    }
    return undefined;
  }

  private buildDagExecutor(
    contract: RuntimeSchemaMutationContract,
    ownerTableId: string | number | undefined,
    body: TCreateTableBody | undefined,
    context: TDynamicContext | undefined,
  ): RuntimeSchemaDagExecutor {
    const dag = new RuntimeSchemaDagExecutor();
    let handlerRan = false;
    let handlerResult: any = null;

    const self = this;

    dag.registerAdapter('attest-source', {
      async execute(ctx: RuntimeSchemaCommandAdapterContext) {
        await self.attestSourceRevision(ctx.contract);
        await self.targetAttestor.assertSource(ctx.contract);
      },
    });

    dag.registerAdapter('capture-compensation', {
      async execute() {},
    });

    dag.registerAdapter('apply-physical-change', {
      async execute(ctx: RuntimeSchemaCommandAdapterContext) {
        if (handlerRan) return;
        handlerRan = true;

        const executionContext: any = {
          ...(context ?? {}),
          $schemaContract: {
            contract: ctx.contract,
            requiredConfirmHash: ctx.contract.context.confirmationDigest,
          },
          $onLockAcquired: async () => {},
        };

        const operation = ctx.contract.context.operation;
        if (operation === 'create') {
          handlerResult = await self.tableHandlerService.createTable(
            body!,
            executionContext,
          );
        } else if (operation === 'delete') {
          handlerResult = await self.tableHandlerService.delete(
            ownerTableId!,
            executionContext,
          );
        } else {
          handlerResult = await self.tableHandlerService.updateTable(
            ownerTableId!,
            body!,
            executionContext,
          );
        }

        if (handlerResult?._preview) {
          return { preview: handlerResult };
        }

        const recordId = handlerResult?._id ?? handlerResult?.id ?? ownerTableId;
        return {
          recordId,
          affectedTables: handlerResult?.affectedTables,
          tableRenames: handlerResult?.tableRenames,
        };
      },
    });

    dag.registerAdapter('apply-metadata-change', {
      async execute() {
        if (!handlerRan) {
          throw new Error('apply-metadata-change ran before apply-physical-change');
        }
      },
    });

    dag.registerAdapter('apply-artifacts', {
      async execute() {},
    });

    dag.registerAdapter('attest-target', {
      async execute(ctx: RuntimeSchemaCommandAdapterContext) {
        const recordId = handlerResult?._id ?? handlerResult?.id ?? ownerTableId;
        await self.attestTargetRevision(ctx.contract, recordId);
        await self.targetAttestor.assertTarget(ctx.contract);
      },
    });

    dag.registerAdapter('stage-cache', {
      async execute() {},
    });

    dag.registerAdapter('commit-database', {
      async execute() {},
    });

    dag.registerAdapter('activate-runtime', {
      async execute() {},
    });

    dag.registerAdapter('complete-change', {
      async execute() {},
    });

    return dag;
  }
}
