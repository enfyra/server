import { describe, it, expect, vi, beforeEach } from 'vitest';
import { RuntimeSchemaExecutorService } from '../../src/modules/table-management/services/runtime-schema-executor.service';
import { RuntimeSchemaContractCompilerService } from '../../src/modules/table-management/services/runtime-schema-contract-compiler.service';
import { RuntimeMetadataSchemaRouterService } from '../../src/modules/table-management/services/runtime-metadata-schema-router.service';
import { RuntimeSchemaJournalService } from '../../src/modules/table-management/services/runtime-schema-journal.service';
import { RuntimeSchemaUnitOfWorkService } from '../../src/modules/table-management/services/runtime-schema-unit-of-work.service';

function makeContract(overrides: Record<string, unknown> = {}) {
  return {
    contractVersion: 1,
    mutationId: 'test-mutation-1',
    idempotencyKey: 'test-idem-1',
    backend: 'postgresql',
    origin: 'runtime',
    contractHash: 'abc123',
    context: {
      operation: 'update',
      actorId: 'user-1',
      tableId: '42',
      tableName: 'post',
      sourceRevision: 'source-rev-1',
      targetRevision: 'target-rev-1',
      source: { name: 'post', columns: [], relations: [], uniques: [], indexes: [] },
      target: { name: 'post', columns: [{ name: 'title', type: 'varchar' }], relations: [], uniques: [], indexes: [] },
      diff: {
        tableName: 'post',
        operation: 'update',
        schemaChanged: true,
        isDestructive: false,
        removedColumns: [],
        addedColumns: ['title'],
        renamedColumns: [],
        changedColumns: [],
        removedRelations: [],
        addedRelations: [],
        removedUniques: [],
        addedUniques: [],
        removedIndexes: [],
        addedIndexes: [],
        owningSideInverseCascadeWarnings: [],
      },
      confirmationDigest: 'confirm-hash-1',
      affectedResources: { tables: ['post'], relationIds: [], cacheTables: [] },
    },
    changes: [{ id: 'runtime:add-column:post:title', kind: 'add-column', label: 'add column title' }],
    phases: [{ index: 0, nodes: [{ id: 'n1', changeId: 'c1', dependsOn: [], phase: 0, completesChange: true, command: { kind: 'apply-physical-change' } }] }],
    ...overrides,
  } as any;
}

describe('C2: Executor must verify contract inputs', () => {
  it('rejects mismatched ownerTableId', async () => {
    const tableHandlerService = {
      updateTable: vi.fn().mockResolvedValue({ id: 99, affectedTables: [] }),
    };
    const journal = {
      create: vi.fn().mockResolvedValue(undefined),
      advanceStage: vi.fn().mockResolvedValue(undefined),
      markCompleted: vi.fn().mockResolvedValue(undefined),
      markFailed: vi.fn().mockResolvedValue(undefined),
    };
    const unitOfWork = { run: vi.fn((cb: any) => cb()) };
    const executor = new RuntimeSchemaExecutorService({
      tableHandlerService: tableHandlerService as any,
      runtimeSchemaUnitOfWorkService: unitOfWork as any,
      runtimeSchemaJournalService: journal as any,
      databaseConfigService: { getDbType: () => 'postgres', isMongoDb: () => false } as any,
    });

    const contract = makeContract();
    // contract.context.tableId is '42', but we pass ownerTableId 999
    await expect(
      executor.execute({
        contract,
        ownerTableId: 999,
        body: { name: 'completely_different_table', columns: [] } as any,
      }),
    ).rejects.toThrow(/contract.*mismatch|tableId/i);
  });

  it('rejects tampered contract hash', async () => {
    const tableHandlerService = {
      updateTable: vi.fn().mockResolvedValue({ id: 42, affectedTables: [] }),
    };
    const journal = {
      create: vi.fn().mockResolvedValue(undefined),
      advanceStage: vi.fn().mockResolvedValue(undefined),
      markCompleted: vi.fn().mockResolvedValue(undefined),
      markFailed: vi.fn().mockResolvedValue(undefined),
    };
    const unitOfWork = { run: vi.fn((cb: any) => cb()) };
    const executor = new RuntimeSchemaExecutorService({
      tableHandlerService: tableHandlerService as any,
      runtimeSchemaUnitOfWorkService: unitOfWork as any,
      runtimeSchemaJournalService: journal as any,
      databaseConfigService: { getDbType: () => 'postgres', isMongoDb: () => false } as any,
    });

    const contract = makeContract({ contractHash: 'tampered-hash' });
    await expect(
      executor.execute({
        contract,
        ownerTableId: 42,
        body: { name: 'post', columns: [] } as any,
      }),
    ).rejects.toThrow(/hash.*mismatch|integrity|invalid/i);
  });
});

describe('C3: Source revision must be attested under lock', () => {
  it('rejects stale sourceRevision', async () => {
    const tableHandlerService = {
      updateTable: vi.fn().mockResolvedValue({ id: 42, affectedTables: [] }),
    };
    const journal = {
      create: vi.fn().mockResolvedValue(undefined),
      advanceStage: vi.fn().mockResolvedValue(undefined),
      markCompleted: vi.fn().mockResolvedValue(undefined),
      markFailed: vi.fn().mockResolvedValue(undefined),
    };
    const unitOfWork = { run: vi.fn((cb: any) => cb()) };
    const executor = new RuntimeSchemaExecutorService({
      tableHandlerService: tableHandlerService as any,
      runtimeSchemaUnitOfWorkService: unitOfWork as any,
      runtimeSchemaJournalService: journal as any,
      databaseConfigService: { getDbType: () => 'postgres', isMongoDb: () => false } as any,
    });

    // Contract compiled from source-rev-1, but actual DB is now at source-rev-2
    const contract = makeContract();
    contract.context.sourceRevision = 'stale-revision';

    // Executor should re-attest source under lock and reject on mismatch
    await expect(
      executor.execute({
        contract,
        ownerTableId: 42,
        body: { name: 'post', columns: [] } as any,
      }),
    ).rejects.toThrow(/source.*revision|stale|attestation/i);
  });
});

describe('C6: db_committed must only advance after UOW commits', () => {
  it('does not advance db_committed before UOW returns', async () => {
    const stageOrder: string[] = [];
    const tableHandlerService = {
      updateTable: vi.fn().mockResolvedValue({ id: 42, affectedTables: [] }),
    };
    const journal = {
      create: vi.fn().mockResolvedValue(undefined),
      advanceStage: vi.fn(async (_id: string, stage: string) => {
        stageOrder.push(stage);
      }),
      markCompleted: vi.fn().mockResolvedValue(undefined),
      markFailed: vi.fn().mockResolvedValue(undefined),
    };
    let uowCommitted = false;
    const unitOfWork = {
      run: vi.fn(async (cb: any) => {
        const result = await cb();
        uowCommitted = true;
        return result;
      }),
    };
    const executor = new RuntimeSchemaExecutorService({
      tableHandlerService: tableHandlerService as any,
      runtimeSchemaUnitOfWorkService: unitOfWork as any,
      runtimeSchemaJournalService: journal as any,
      databaseConfigService: { getDbType: () => 'postgres', isMongoDb: () => false } as any,
    });

    let dbCommittedBeforeUowDone = false;
    journal.advanceStage = vi.fn(async (_id: string, stage: string) => {
      stageOrder.push(stage);
      if (stage === 'db_committed' && !uowCommitted) {
        dbCommittedBeforeUowDone = true;
      }
    });

    await executor.execute({
      contract: makeContract(),
      ownerTableId: 42,
      body: { name: 'post', columns: [] } as any,
    });

    expect(dbCommittedBeforeUowDone).toBe(false);
  });
});

describe('H1: Additive mutations must not become hidden previews', () => {
  it('propagates preview to caller instead of marking completed', async () => {
    const tableHandlerService = {
      updateTable: vi.fn().mockResolvedValue({
        _preview: true,
        schemaChanged: true,
        requiredConfirmHash: 'confirm-hash-1',
      }),
    };
    const journal = {
      create: vi.fn().mockResolvedValue(undefined),
      advanceStage: vi.fn().mockResolvedValue(undefined),
      markCompleted: vi.fn().mockResolvedValue(undefined),
      markFailed: vi.fn().mockResolvedValue(undefined),
    };
    const unitOfWork = { run: vi.fn((cb: any) => cb()) };
    const executor = new RuntimeSchemaExecutorService({
      tableHandlerService: tableHandlerService as any,
      runtimeSchemaUnitOfWorkService: unitOfWork as any,
      runtimeSchemaJournalService: journal as any,
      databaseConfigService: { getDbType: () => 'postgres', isMongoDb: () => false } as any,
    });

    const result = await executor.execute({
      contract: makeContract(),
      ownerTableId: 42,
      body: { name: 'post', columns: [{ name: 'title', type: 'varchar' }] } as any,
    });

    // Should NOT mark completed when preview is returned
    expect(journal.markCompleted).not.toHaveBeenCalled();
    // Should propagate preview info
    expect((result as any).preview).toBeDefined();
  });
});

describe('Journal: must support retry after failure', () => {
  it('same source+target must produce different mutationIds for retry', async () => {
    const compiler = new RuntimeSchemaContractCompilerService({
      databaseConfigService: { getDbType: () => 'postgres' } as any,
      runtimeRegistryService: { getMetadata: () => ({ tables: new Map() }) } as any,
      runtimeSchemaPhysicalPlannerService: { plan: async () => null } as any,
    });

    const input = {
      operation: 'update' as const,
      tableName: 'post',
      tableId: '42',
      beforeMetadata: { name: 'post', columns: [{ id: 1, name: 'id', type: 'int', isPrimary: true, isGenerated: true, isNullable: false }], relations: [] },
      afterMetadata: { name: 'post', columns: [{ id: 1, name: 'id', type: 'int', isPrimary: true, isGenerated: true, isNullable: false }, { id: 2, name: 'title', type: 'varchar', isPrimary: false, isGenerated: false, isNullable: true }], relations: [] },
    };

    const first = await compiler.compile(input);
    const second = await compiler.compile(input);

    // Retrying the same logical mutation must not collide with a failed journal entry
    expect(first.contract.mutationId).not.toBe(second.contract.mutationId);
  });
});

describe('C4: Batch create must not bypass router', () => {
  it('DynamicRepository.createBatch checks handles() for schema tables', async () => {
    // This is a structural test: we verify the code path exists
    const fs = await import('node:fs');
    const source = fs.readFileSync(
      new URL('../../src/modules/dynamic-api/repositories/dynamic.repository.ts', import.meta.url),
      'utf-8',
    );

    // Find createBatch method
    const batchStart = source.indexOf('async createBatch(');
    expect(batchStart).toBeGreaterThan(0);

    // Find the next method boundary
    const batchSection = source.slice(batchStart, batchStart + 2000);

    // createBatch must reject or route all schema metadata tables
    expect(batchSection).toContain("this.tableName === 'enfyra_table'");
    expect(batchSection).toContain('runtimeMetadataSchemaRouterService.handles');
  });
});

describe('C5: Delete must require confirmation', () => {
  it('policy returns preview for delete without confirmation hash', async () => {
    const { SchemaMigrationValidatorService } = await import('../../src/domain/policy/services/schema-migration-validator.service');
    const compiler = new RuntimeSchemaContractCompilerService({
      databaseConfigService: { getDbType: () => 'postgres' } as any,
      runtimeRegistryService: { getMetadata: () => ({ tables: new Map() }) } as any,
      runtimeSchemaPhysicalPlannerService: { plan: async () => null } as any,
    });
    const validator = new SchemaMigrationValidatorService({
      runtimeRegistryService: { getMetadata: () => ({ tables: new Map() }), requireMetadata: () => ({ tables: new Map() }) } as any,
      runtimeSchemaContractCompilerService: compiler,
    });

    const result = await validator.checkSchemaMigration({
      operation: 'delete',
      tableName: 'post',
      beforeMetadata: { name: 'post', columns: [{ id: 1, name: 'id', type: 'int' }], relations: [] },
      requestContext: {},
    });

    // Delete without hash must return preview, not auto-approve
    expect(result.preview).toBe(true);
    expect(result.allow).toBe(false);
  });
});

describe('H2: Relation target identity must be consistent', () => {
  it('normalization produces same key for name and ID targets of same relation', async () => {
    const { normalizeRuntimeTableSchema } = await import(
      '../../src/modules/table-management/utils/runtime-schema-normalization.util'
    );

    // Before metadata: relation target is a name (as loaded from DB join)
    const before = normalizeRuntimeTableSchema({
      name: 'post',
      columns: [{ id: 1, name: 'id', type: 'int', isPrimary: true }],
      relations: [{
        id: 10,
        propertyName: 'author',
        type: 'many-to-one',
        targetTable: { name: 'user' },
        isNullable: true,
      }],
    });

    // After metadata: relation target is an ID (as constructed by router body)
    const after = normalizeRuntimeTableSchema({
      name: 'post',
      columns: [{ id: 1, name: 'id', type: 'int', isPrimary: true }],
      relations: [{
        id: 10,
        propertyName: 'author',
        type: 'many-to-one',
        targetTable: 5,
        isNullable: true,
      }],
    });

    // Same relation must produce the same normalized key regardless of
    // whether target is expressed as name or ID
    const beforeKey = before!.contract.relations[0].targetTableName;
    const afterKey = after!.contract.relations[0].targetTableName;
    expect(beforeKey).toBe(afterKey);
  });
});

describe('H7: onDelete change must be detected as schema change', () => {
  it('changing only onDelete produces schemaChanged=true', async () => {
    const compiler = new RuntimeSchemaContractCompilerService({
      databaseConfigService: { getDbType: () => 'postgres' } as any,
      runtimeRegistryService: { getMetadata: () => ({ tables: new Map() }) } as any,
      runtimeSchemaPhysicalPlannerService: { plan: async () => null } as any,
    });

    const before = {
      name: 'post',
      columns: [{ id: 1, name: 'id', type: 'int', isPrimary: true, isGenerated: true, isNullable: false }],
      relations: [{
        id: 10,
        propertyName: 'author',
        type: 'many-to-one',
        targetTableName: 'user',
        foreignKeyColumn: 'authorId',
        isNullable: true,
        onDelete: 'SET NULL',
      }],
    };
    const after = {
      name: 'post',
      columns: [{ id: 1, name: 'id', type: 'int', isPrimary: true, isGenerated: true, isNullable: false }],
      relations: [{
        id: 10,
        propertyName: 'author',
        type: 'many-to-one',
        targetTableName: 'user',
        foreignKeyColumn: 'authorId',
        isNullable: true,
        onDelete: 'CASCADE',
      }],
    };

    const { contract } = await compiler.compile({
      operation: 'update',
      tableName: 'post',
      tableId: '42',
      beforeMetadata: before,
      afterMetadata: after,
    });

    // onDelete changed from SET NULL to CASCADE — requires FK DDL
    expect(contract.context.diff.schemaChanged).toBe(true);
  });
});
