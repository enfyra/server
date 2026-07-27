import { describe, it, expect, vi } from 'vitest';
import { RuntimeSchemaExecutorService } from '../../src/modules/table-management/services/runtime-schema-executor.service';
import { RuntimeSchemaContractCompilerService } from '../../src/modules/table-management/services/runtime-schema-contract-compiler.service';
import { RuntimeSchemaJournalService } from '../../src/modules/table-management/services/runtime-schema-journal.service';
import { normalizeRuntimeTableSchema } from '../../src/modules/table-management/utils/runtime-schema-normalization.util';
import { hashCanonical } from '../../src/shared/utils/schema-mutation-contract.util';

const baseBefore = {
  name: 'post',
  columns: [
    { id: 1, name: 'id', type: 'int', isPrimary: true, isGenerated: true, isNullable: false },
  ],
  relations: [],
};
const baseAfter = {
  name: 'post',
  columns: [
    { id: 1, name: 'id', type: 'int', isPrimary: true, isGenerated: true, isNullable: false },
    { id: 2, name: 'title', type: 'varchar', isPrimary: false, isGenerated: false, isNullable: true },
  ],
  relations: [],
};

function makeCompiler() {
  return new RuntimeSchemaContractCompilerService({
    databaseConfigService: { getDbType: () => 'postgres' } as any,
    runtimeRegistryService: { getMetadata: () => ({ tables: new Map() }) } as any,
    runtimeSchemaPhysicalPlannerService: { plan: async () => null } as any,
  });
}

async function compileRealContract(overrides: Record<string, unknown> = {}) {
  const compiler = makeCompiler();
  const { contract } = await compiler.compile({
    operation: 'update',
    tableName: 'post',
    tableId: '42',
    beforeMetadata: baseBefore,
    afterMetadata: baseAfter,
    ...overrides,
  });
  return contract;
}

function makeExecutor(deps: Record<string, unknown> = {}) {
  const tableHandlerService = (deps.tableHandlerService ?? {
    updateTable: vi.fn(async (_id: any, _body: any, ctx: any) => {
      if (ctx?.$onLockAcquired) await ctx.$onLockAcquired();
      return { id: 42, affectedTables: ['post'] };
    }),
  }) as any;
  const journal = (deps.journal ?? {
    create: vi.fn().mockResolvedValue(undefined),
    advanceStage: vi.fn().mockResolvedValue(undefined),
    markCompleted: vi.fn().mockResolvedValue(undefined),
    markFailed: vi.fn().mockResolvedValue(undefined),
  }) as any;
  const unitOfWork = (deps.unitOfWork ?? {
    run: vi.fn((cb: any) => cb()),
  }) as any;
  const queryBuilderService = (deps.queryBuilderService ?? {
    findOne: vi.fn().mockResolvedValue(baseBefore),
  }) as any;
  return {
    executor: new RuntimeSchemaExecutorService({
      tableHandlerService,
      runtimeSchemaUnitOfWorkService: unitOfWork,
      runtimeSchemaJournalService: journal,
      databaseConfigService: { getDbType: () => 'postgres', isMongoDb: () => false } as any,
      queryBuilderService,
    }),
    tableHandlerService,
    journal,
    unitOfWork,
  };
}

describe('C2: Executor must verify contract inputs', () => {
  it('rejects mismatched ownerTableId', async () => {
    const contract = await compileRealContract();
    const { executor } = makeExecutor();
    await expect(
      executor.execute({
        contract,
        ownerTableId: 999,
        body: { name: 'post', columns: [] } as any,
      }),
    ).rejects.toThrow(/tableId mismatch/i);
  });

  it('rejects tampered contract hash', async () => {
    const contract = await compileRealContract();
    const tampered = JSON.parse(JSON.stringify(contract));
    tampered.contractHash = 'tampered';
    Object.setPrototypeOf(tampered, Object.prototype);
    const { executor } = makeExecutor();
    await expect(
      executor.execute({
        contract: tampered,
        ownerTableId: 42,
        body: { name: 'post', columns: [] } as any,
      }),
    ).rejects.toThrow(/hash integrity/i);
  });
});

describe('C3: Source revision must be attested under lock', () => {
  it('rejects stale sourceRevision when metadata has changed', async () => {
    const contract = await compileRealContract();
    const differentTable = {
      name: 'post',
      columns: [
        { id: 1, name: 'id', type: 'int', isPrimary: true, isGenerated: true, isNullable: false },
        { id: 99, name: 'extra', type: 'text', isPrimary: false, isGenerated: false, isNullable: true },
      ],
      relations: [],
    };
    const { executor } = makeExecutor({
      queryBuilderService: { findOne: vi.fn().mockResolvedValue(differentTable) },
    });
    await expect(
      executor.execute({
        contract,
        ownerTableId: 42,
        body: { name: 'post', columns: [] } as any,
      }),
    ).rejects.toThrow(/source revision stale/i);
  });

  it('passes when source revision matches current metadata', async () => {
    const contract = await compileRealContract();
    const { executor, tableHandlerService } = makeExecutor({
      queryBuilderService: { findOne: vi.fn().mockResolvedValue(baseBefore) },
    });
    const result = await executor.execute({
      contract,
      ownerTableId: 42,
      body: { name: 'post', columns: [] } as any,
    });
    expect(result.mutationId).toBe(contract.mutationId);
    expect(tableHandlerService.updateTable).toHaveBeenCalled();
  });
});

describe('C6: db_committed must only advance after UOW commits', () => {
  it('does not advance db_committed before UOW returns', async () => {
    const contract = await compileRealContract();
    let uowCommitted = false;
    let dbCommittedBeforeUowDone = false;
    const journal = {
      create: vi.fn().mockResolvedValue(undefined),
      advanceStage: vi.fn(async (_id: string, stage: string) => {
        if (stage === 'db_committed' && !uowCommitted) {
          dbCommittedBeforeUowDone = true;
        }
      }),
      markCompleted: vi.fn().mockResolvedValue(undefined),
      markFailed: vi.fn().mockResolvedValue(undefined),
    };
    const unitOfWork = {
      run: vi.fn(async (cb: any) => {
        const result = await cb();
        uowCommitted = true;
        return result;
      }),
    };
    const { executor } = makeExecutor({ journal, unitOfWork });
    await executor.execute({
      contract,
      ownerTableId: 42,
      body: { name: 'post', columns: [] } as any,
    });
    expect(dbCommittedBeforeUowDone).toBe(false);
  });
});

describe('H1: Additive mutations must not become hidden previews', () => {
  it('propagates preview to caller instead of marking completed', async () => {
    const contract = await compileRealContract();
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
    const { executor } = makeExecutor({ tableHandlerService, journal });
    const result = await executor.execute({
      contract,
      ownerTableId: 42,
      body: { name: 'post', columns: [{ name: 'title', type: 'varchar' }] } as any,
    });
    expect(journal.markCompleted).not.toHaveBeenCalled();
    expect((result as any).preview).toBeDefined();
  });
});

describe('Journal: must support retry after failure', () => {
  it('journal.create resets failed entry instead of throwing duplicate key', async () => {
    const { RuntimeSchemaJournalService } = await import(
      '../../src/modules/table-management/services/runtime-schema-journal.service'
    );
    const store = new Map<string, any>();
    const queryBuilderService = {
      isMongoDb: () => false,
      getKnex: () => {
        const knexFn: any = () => ({
          insert: async (doc: any) => {
            if (store.has(doc.mutationId)) throw new Error('duplicate key');
            store.set(doc.mutationId, doc);
          },
          where: () => ({
            first: async () => store.get('test-mutation') ?? null,
            update: async (fields: any) => {
              const existing = store.get('test-mutation');
              if (existing) Object.assign(existing, fields);
            },
          }),
        });
        knexFn.schema = {
          hasTable: async () => true,
          createTable: async () => {},
        };
        return knexFn;
      },
    };
    const journal = new RuntimeSchemaJournalService({
      queryBuilderService: queryBuilderService as any,
    });

    // First attempt fails
    await journal.create({ mutationId: 'test-mutation', contractHash: 'hash1', backend: 'postgresql' });
    await journal.markFailed('test-mutation', 'some error');

    // Retry with same mutationId must succeed (reset failed entry)
    await expect(
      journal.create({ mutationId: 'test-mutation', contractHash: 'hash1', backend: 'postgresql' }),
    ).resolves.toBeUndefined();
  });

  it('journal.create rejects in-progress mutation', async () => {
    const { RuntimeSchemaJournalService } = await import(
      '../../src/modules/table-management/services/runtime-schema-journal.service'
    );
    const store = new Map<string, any>();
    const queryBuilderService = {
      isMongoDb: () => false,
      getKnex: () => {
        const knexFn: any = () => ({
          insert: async (doc: any) => {
            if (store.has(doc.mutationId)) throw new Error('duplicate key');
            store.set(doc.mutationId, doc);
          },
          where: () => ({
            first: async () => store.get('test-mutation-2') ?? null,
            update: async (fields: any) => {
              const existing = store.get('test-mutation-2');
              if (existing) Object.assign(existing, fields);
            },
          }),
        });
        knexFn.schema = {
          hasTable: async () => true,
          createTable: async () => {},
        };
        return knexFn;
      },
    };
    const journal = new RuntimeSchemaJournalService({
      queryBuilderService: queryBuilderService as any,
    });

    await journal.create({ mutationId: 'test-mutation-2', contractHash: 'hash1', backend: 'postgresql' });
    await journal.advanceStage('test-mutation-2', 'executing');

    // In-progress mutation must be rejected
    await expect(
      journal.create({ mutationId: 'test-mutation-2', contractHash: 'hash1', backend: 'postgresql' }),
    ).rejects.toThrow(/already in progress/i);
  });
});

describe('C4: Batch create must not bypass router', () => {
  it('DynamicRepository.createBatch checks handles() for schema tables', async () => {
    const fs = await import('node:fs');
    const source = fs.readFileSync(
      new URL('../../src/modules/dynamic-api/repositories/dynamic.repository.ts', import.meta.url),
      'utf-8',
    );
    const batchStart = source.indexOf('async createBatch(');
    expect(batchStart).toBeGreaterThan(0);
    const batchSection = source.slice(batchStart, batchStart + 2000);
    expect(batchSection).toContain("this.tableName === 'enfyra_table'");
    expect(batchSection).toContain('runtimeMetadataSchemaRouterService.handles');
  });
});

describe('C5: Delete must require confirmation', () => {
  it('policy returns preview for delete without confirmation hash', async () => {
    const { SchemaMigrationValidatorService } = await import('../../src/domain/policy/services/schema-migration-validator.service');
    const compiler = makeCompiler();
    const validator = new SchemaMigrationValidatorService({
      runtimeRegistryService: { getMetadata: () => ({ tables: new Map() }), requireMetadata: () => ({ tables: new Map() }) } as any,
      runtimeSchemaContractCompilerService: compiler,
    });
    const result = await validator.checkSchemaMigration({
      operation: 'delete',
      tableName: 'post',
      beforeMetadata: baseBefore,
      requestContext: {},
    });
    expect(result.preview).toBe(true);
    expect(result.allow).toBe(false);
  });
});

describe('H2: Relation target identity must be consistent', () => {
  it('normalization produces same key when router preserves targetTableName alongside ID', () => {
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
    // After router fix: body preserves targetTableName alongside numeric ID
    const after = normalizeRuntimeTableSchema({
      name: 'post',
      columns: [{ id: 1, name: 'id', type: 'int', isPrimary: true }],
      relations: [{
        id: 10,
        propertyName: 'author',
        type: 'many-to-one',
        targetTable: 5,
        targetTableName: 'user',
        isNullable: true,
      }],
    });
    const beforeKey = before!.contract.relations[0].targetTableName;
    const afterKey = after!.contract.relations[0].targetTableName;
    expect(beforeKey).toBe(afterKey);
  });
});

describe('H7: onDelete change must be detected as schema change', () => {
  it('changing only onDelete produces schemaChanged=true', async () => {
    const compiler = makeCompiler();
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
    expect(contract.context.diff.schemaChanged).toBe(true);
  });
});
