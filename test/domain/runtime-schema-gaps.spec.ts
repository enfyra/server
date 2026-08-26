import { describe, it, expect, vi } from 'vitest';
import { RuntimeSchemaExecutorService } from '../../src/modules/table-management/services/runtime-schema-executor.service';
import { RuntimeSchemaContractCompilerService } from '../../src/modules/table-management/services/runtime-schema-contract-compiler.service';
import { RuntimeSchemaJournalService } from '../../src/modules/table-management/services/runtime-schema-journal.service';
import { normalizeRuntimeTableSchema } from '../../src/modules/table-management/utils/runtime-schema-normalization.util';
import { hashCanonical } from '../../src/shared/utils/schema-mutation-contract.util';
import { SqlSchemaDiffService } from '../../src/engines/knex/services/sql-schema-diff.service';
import { RuntimeSchemaTargetAttestorService } from '../../src/modules/table-management/services/runtime-schema-target-attestor.service';

const baseBefore = {
  name: 'post',
  columns: [
    {
      id: 1,
      name: 'id',
      type: 'int',
      isPrimary: true,
      isGenerated: true,
      isNullable: false,
    },
  ],
  relations: [],
};
const baseAfter = {
  name: 'post',
  columns: [
    {
      id: 1,
      name: 'id',
      type: 'int',
      isPrimary: true,
      isGenerated: true,
      isNullable: false,
    },
    {
      id: 2,
      name: 'title',
      type: 'varchar',
      isPrimary: false,
      isGenerated: false,
      isNullable: true,
    },
  ],
  relations: [],
};

function makeCompiler(dbType = 'postgres') {
  return new RuntimeSchemaContractCompilerService({
    databaseConfigService: { getDbType: () => dbType } as any,
    runtimeRegistryService: {
      getMetadata: () => ({ tables: new Map() }),
    } as any,
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
    findOne: vi
      .fn()
      .mockResolvedValueOnce(baseBefore)
      .mockResolvedValue(baseAfter),
  }) as any;
  return {
    executor: new RuntimeSchemaExecutorService({
      tableHandlerService,
      runtimeSchemaUnitOfWorkService: unitOfWork,
      runtimeSchemaJournalService: journal,
      databaseConfigService: {
        getDbType: () => 'postgres',
        isMongoDb: () => false,
      } as any,
      queryBuilderService,
      runtimeSchemaTargetAttestorService:
        (deps.runtimeSchemaTargetAttestorService ?? {
          assertSource: vi.fn().mockResolvedValue(undefined),
          assertTarget: vi.fn().mockResolvedValue(undefined),
        }) as any,
    }),
    tableHandlerService,
    journal,
    unitOfWork,
    queryBuilderService,
  };
}

describe('Runtime schema normalization', () => {
  it('canonicalizes column isUnique into the persisted table unique contract', () => {
    const intent = normalizeRuntimeTableSchema({
      name: 'gateway_models',
      columns: [
        { name: 'id', type: 'int', isPrimary: true, isGenerated: true },
        { name: 'as', type: 'varchar', isUnique: true },
      ],
    });
    const persisted = normalizeRuntimeTableSchema({
      name: 'gateway_models',
      columns: [
        { name: 'id', type: 'int', isPrimary: true, isGenerated: true },
        { name: 'as', type: 'varchar' },
      ],
      uniques: [['as']],
    });

    expect(intent?.contract.uniques).toEqual([['as']]);
    expect(
      persisted?.contract.columns.find((column) => column.name === 'as')
        ?.isUnique,
    ).toBe(true);
    expect(hashCanonical(intent!.contract)).toBe(
      hashCanonical(persisted!.contract),
    );
  });

  it('removes a persisted single-column unique when isUnique is explicitly false', () => {
    const target = normalizeRuntimeTableSchema({
      name: 'gateway_models',
      columns: [
        { name: 'id', type: 'int', isPrimary: true, isGenerated: true },
        { name: 'modelName', type: 'varchar', isUnique: false },
      ],
      uniques: [['modelName']],
    });

    expect(target?.contract.uniques).toEqual([]);
    expect(
      target?.contract.columns.find((column) => column.name === 'modelName')
        ?.isUnique,
    ).toBe(false);
  });

  it('treats omitted constraints and persisted empty constraints as the same contract', () => {
    const request = normalizeRuntimeTableSchema({
      name: 'course',
      columns: [
        { name: 'id', type: 'int', isPrimary: true, isGenerated: true },
        { name: 'title', type: 'varchar', isNullable: false },
      ],
    });
    const persisted = normalizeRuntimeTableSchema({
      name: 'course',
      columns: [
        { name: 'id', type: 'int', isPrimary: true, isGenerated: true },
        { name: 'title', type: 'varchar', isNullable: false },
      ],
      uniques: [],
      indexes: [],
    });

    expect(hashCanonical(request!.contract)).toBe(
      hashCanonical(persisted!.contract),
    );
  });

  it('canonicalizes hydrated relation references without object string coercion', () => {
    const hydrated = normalizeRuntimeTableSchema(
      {
        name: 'enfyra_user',
        relations: [
          {
            id: 14,
            propertyName: 'allowedRoutePermissions',
            type: 'many-to-many',
            targetTable: { id: 8, name: 'enfyra_route_permission' },
            mappedBy: { id: 13, propertyName: 'allowedUsers' },
            junctionTableName: 'j_9475348e0853',
          },
        ],
      },
      { backend: 'postgresql', mode: 'persisted' },
    );
    const scalar = normalizeRuntimeTableSchema(
      {
        name: 'enfyra_user',
        relations: [
          {
            id: 14,
            propertyName: 'allowedRoutePermissions',
            type: 'many-to-many',
            targetTableName: 'enfyra_route_permission',
            mappedBy: 'allowedUsers',
            junctionTableName: 'j_9475348e0853',
          },
        ],
      },
      { backend: 'postgresql', mode: 'persisted' },
    );

    expect(hydrated!.contract.relations[0]).toMatchObject({
      targetTableName: 'enfyra_route_permission',
      mappedBy: 'allowedUsers',
    });
    expect(hashCanonical(hydrated!.contract)).toBe(
      hashCanonical(scalar!.contract),
    );
  });

  it('ignores inverse-only physical mappings in the logical contract', () => {
    const intent = normalizeRuntimeTableSchema(
      {
        name: 'student',
        relations: [
          {
            propertyName: 'courses',
            type: 'many-to-many',
            targetTableName: 'course',
            mappedBy: 'students',
          },
        ],
      },
      { backend: 'mongodb', mode: 'persisted' },
    );
    const persisted = normalizeRuntimeTableSchema(
      {
        name: 'student',
        relations: [
          {
            propertyName: 'courses',
            type: 'many-to-many',
            targetTableName: 'course',
            mappedBy: { propertyName: 'students' },
            foreignKeyColumn: 'students',
            junctionTableName: 'j_legacy_mapping',
          },
        ],
      },
      { backend: 'mongodb', mode: 'persisted' },
    );

    expect(hashCanonical(intent!.contract)).toBe(
      hashCanonical(persisted!.contract),
    );
  });

  it('compiles SQL create against the effective metadata including generated indexes', async () => {
    const compiler = makeCompiler();
    const { contract } = await compiler.compile({
      operation: 'create',
      tableName: 'course',
      beforeMetadata: null,
      afterMetadata: {
        name: 'course',
        columns: [
          { name: 'id', type: 'int', isPrimary: true, isGenerated: true },
          { name: 'title', type: 'varchar', isNullable: false },
        ],
      },
    });

    expect(contract.context.target?.indexes).toEqual([
      ['createdAt'],
      ['updatedAt'],
    ]);
    expect(contract.context.targetRevision).toBe(
      hashCanonical(
        normalizeRuntimeTableSchema(
          {
            name: 'course',
            columns: [
              { name: 'id', type: 'int', isPrimary: true, isGenerated: true },
              { name: 'title', type: 'varchar', isNullable: false },
            ],
            indexes: [['createdAt'], ['updatedAt']],
          },
          { backend: 'postgresql' },
        )!.contract,
      ),
    );
  });

  it('canonicalizes unique scalar and owning one-to-one indexes before SQL snapshotting', () => {
    const normalized = normalizeRuntimeTableSchema(
      {
        name: 'payment_order',
        columns: [
          { name: 'id', type: 'int', isPrimary: true, isGenerated: true },
          { name: 'providerOrderId', type: 'varchar', isNullable: false },
        ],
        relations: [
          {
            propertyName: 'user',
            type: 'one-to-one',
            targetTableName: 'enfyra_user',
            foreignKeyColumn: 'userId',
          },
        ],
        uniques: [['providerOrderId']],
        indexes: [['providerOrderId'], ['user']],
      },
      { backend: 'postgresql', mode: 'persisted' },
    );

    expect(normalized?.contract.uniques).toEqual([
      ['providerOrderId'],
      ['user'],
    ]);
    expect(normalized?.contract.indexes).toEqual([
      ['createdAt'],
      ['updatedAt'],
    ]);
  });

  it('uses the same canonical unique and index snapshot for MySQL', () => {
    const metadata = {
      name: 'payment_order',
      columns: [
        { name: 'id', type: 'int', isPrimary: true, isGenerated: true },
        { name: 'providerOrderId', type: 'varchar', isNullable: false },
      ],
      uniques: [['providerOrderId']],
      indexes: [['providerOrderId']],
    };

    expect(
      normalizeRuntimeTableSchema(metadata, {
        backend: 'postgresql',
        mode: 'persisted',
      })?.contract,
    ).toEqual(
      normalizeRuntimeTableSchema(metadata, {
        backend: 'mysql',
        mode: 'persisted',
      })?.contract,
    );
  });

  it('compiles Mongo create against the effective metadata including generated indexes', async () => {
    const compiler = makeCompiler('mongodb');
    const { contract } = await compiler.compile({
      operation: 'create',
      tableName: 'course',
      beforeMetadata: null,
      afterMetadata: {
        name: 'course',
        columns: [
          { name: 'id', type: 'int', isPrimary: true, isGenerated: true },
          { name: 'title', type: 'varchar', isNullable: false },
        ],
      },
    });

    expect(contract.context.target?.indexes).toEqual([
      ['createdAt'],
      ['updatedAt'],
    ]);
    expect(contract.context.target?.columns[0]).toMatchObject({
      name: '_id',
      type: 'ObjectId',
      isPrimary: true,
    });
  });

  it('separates persisted M2M target state from immutable inverse creation intent', async () => {
    const compiler = makeCompiler();
    const { contract } = await compiler.compile({
      operation: 'update',
      tableName: 'course',
      tableId: 10,
      beforeMetadata: {
        name: 'course',
        columns: [{ id: 1, name: 'id', type: 'int', isPrimary: true }],
        indexes: [['createdAt'], ['updatedAt']],
        relations: [],
      },
      afterMetadata: {
        name: 'course',
        columns: [{ id: 1, name: 'id', type: 'int', isPrimary: true }],
        indexes: [['createdAt'], ['updatedAt']],
        relations: [
          {
            propertyName: 'students',
            type: 'many-to-many',
            targetTableName: 'student',
            inversePropertyName: 'courses',
          },
        ],
      },
    });

    expect(contract.context.target?.relations[0]).toMatchObject({
      inversePropertyName: '',
      junctionTableName: expect.stringMatching(/^j_/),
    });
    expect(contract.context.executionTarget?.relations[0]).toMatchObject({
      inversePropertyName: 'courses',
      junctionTableName: expect.stringMatching(/^j_/),
    });
    expect(contract.context.executionBodyRevision).toBe(
      hashCanonical(contract.context.executionTarget),
    );
  });
});

describe('SQL schema diff identity normalization', () => {
  it('does not delete and recreate a column when a numeric id arrives as a string', () => {
    const service = new SqlSchemaDiffService({
      knexService: {} as any,
      metadataCacheService: {} as any,
      queryBuilderService: {} as any,
    });
    const diff = {
      columns: { create: [], update: [], delete: [], rename: [] },
    };

    service.analyzeColumnChanges(
      [{ id: 319, name: 'title', type: 'varchar', isNullable: false }],
      [{ id: '319', name: 'title', type: 'varchar', isNullable: false }],
      diff,
    );

    expect(diff.columns).toEqual({
      create: [],
      update: [],
      delete: [],
      rename: [],
    });
  });

  it('emits a unique deletion when a table-level unique is removed', () => {
    const service = new SqlSchemaDiffService({
      knexService: {} as any,
      metadataCacheService: {} as any,
      queryBuilderService: {} as any,
    });
    const diff = {
      columns: { create: [], update: [], delete: [], rename: [] },
      constraints: {
        uniques: { create: [], update: [], delete: [] },
        indexes: { create: [], update: [], delete: [] },
      },
    };

    (service as any).analyzeConstraintChanges(
      {
        name: 'ai_gateway_models',
        columns: [],
        relations: [],
        uniques: [['modelName'], ['upstreamModel']],
        indexes: [],
      },
      {
        name: 'ai_gateway_models',
        columns: [],
        relations: [],
        uniques: [['modelName']],
        indexes: [],
      },
      diff,
    );

    expect(diff.constraints.uniques.delete).toEqual([['upstreamModel']]);
    expect(diff.constraints.uniques.create).toEqual([]);
  });
});

describe('Runtime inverse metadata attestation', () => {
  it('allows a delete to remediate source physical drift', async () => {
    const hasTable = vi.fn();
    const service = new RuntimeSchemaTargetAttestorService({
      queryBuilderService: {
        getKnex: () => ({ schema: { hasTable } }),
      } as any,
      databaseConfigService: { isMongoDb: () => false } as any,
    });

    await expect(
      service.assertSource({
        context: {
          operation: 'delete',
          tableName: 'legacy_landing_content',
          source: { name: 'legacy_landing_content', columns: [], relations: [] },
          target: null,
        },
      } as any),
    ).resolves.toBeUndefined();

    expect(hasTable).not.toHaveBeenCalled();
  });

  it('rejects a target when requested inverse metadata was not materialized', async () => {
    const rows = [
      { id: 10, name: 'course' },
      { id: 20, name: 'student' },
      {
        id: 30,
        sourceTableId: 10,
        targetTableId: 20,
        propertyName: 'students',
      },
      undefined,
    ];
    const knex = vi.fn(() => ({
      where: vi.fn().mockReturnThis(),
      first: vi.fn(async () => rows.shift()),
    }));
    const service = new RuntimeSchemaTargetAttestorService({
      queryBuilderService: { getKnex: () => knex } as any,
      databaseConfigService: { isMongoDb: () => false } as any,
    });
    (service as any).assertPresent = vi.fn().mockResolvedValue(undefined);
    (service as any).assertRemovedJunctions = vi
      .fn()
      .mockResolvedValue(undefined);

    await expect(
      service.assertTarget({
        context: {
          operation: 'update',
          tableName: 'course',
          source: null,
          target: { name: 'course', relations: [] },
          executionTarget: {
            name: 'course',
            relations: [
              {
                propertyName: 'students',
                type: 'many-to-many',
                targetTableName: 'student',
                inversePropertyName: 'courses',
              },
            ],
          },
        },
      } as any),
    ).rejects.toThrow(/inverse metadata.*courses/i);
  });

  it('rejects a Mongo target whose collection validator is stale', async () => {
    const indexes = [
      { name: '_id_', key: { _id: 1 } },
      {
        name: 'post_createdAt_idx',
        key: { createdAt: -1, _id: 1 },
      },
      {
        name: 'post_updatedAt_idx',
        key: { updatedAt: -1, _id: 1 },
      },
    ];
    const db = {
      listCollections: vi.fn(() => ({
        toArray: vi.fn(async () => [
          {
            name: 'post',
            options: {
              validator: {
                $jsonSchema: {
                  bsonType: 'object',
                  properties: {
                    title: { bsonType: 'int', description: 'title' },
                  },
                  required: ['title'],
                },
              },
              validationLevel: 'moderate',
              validationAction: 'error',
            },
          },
        ]),
      })),
      collection: vi.fn(() => ({
        listIndexes: vi.fn(() => ({
          toArray: vi.fn(async () => indexes),
        })),
      })),
    };
    const service = new RuntimeSchemaTargetAttestorService({
      queryBuilderService: { getMongoDb: () => db } as any,
      databaseConfigService: { isMongoDb: () => true } as any,
    });

    await expect(
      service.assertTarget({
        context: {
          operation: 'create',
          tableName: 'post',
          source: null,
          target: {
            name: 'post',
            columns: [
              {
                name: '_id',
                type: 'ObjectId',
                isPrimary: true,
                isGenerated: true,
                isNullable: false,
              },
              {
                name: 'title',
                type: 'varchar',
                isNullable: false,
                isGenerated: false,
              },
            ],
            relations: [],
            uniques: [],
            indexes: [['createdAt'], ['updatedAt']],
          },
        },
      } as any),
    ).rejects.toThrow(/validator/i);
  });

  it('does not treat a logical relation rename as a removed Mongo field when the stored field is stable', async () => {
    const countDocuments = vi.fn().mockResolvedValue(1);
    const service = new RuntimeSchemaTargetAttestorService({
      queryBuilderService: {
        getMongoDb: () => ({
          collection: () => ({ countDocuments }),
        }),
      } as any,
      databaseConfigService: { isMongoDb: () => true } as any,
    });
    const source = {
      name: 'course',
      columns: [],
      relations: [
        {
          propertyName: 'teacher',
          type: 'many-to-one',
          targetTableName: 'teacher',
          foreignKeyColumn: 'teacher',
        },
      ],
    };
    const target = {
      name: 'course',
      columns: [],
      relations: [
        {
          propertyName: 'mentor',
          type: 'many-to-one',
          targetTableName: 'teacher',
          foreignKeyColumn: 'teacher',
        },
      ],
    };

    await expect(
      (service as any).assertRemovedMongoFields(source, target),
    ).resolves.toBeUndefined();
    expect(countDocuments).not.toHaveBeenCalled();
  });

  it('excludes inverse one-to-one relations from the SQL column definition', () => {
    const service = new RuntimeSchemaTargetAttestorService({
      queryBuilderService: {} as any,
      databaseConfigService: { isMongoDb: () => false } as any,
    });
    const definition = (service as any).toSqlPhysicalDefinition({
      name: 'room',
      columns: [],
      relations: [
        {
          propertyName: 'course',
          type: 'one-to-one',
          targetTableName: 'course',
          mappedBy: 'room',
        },
      ],
      uniques: [],
      indexes: [],
    });

    expect(definition.relations).toEqual([]);
  });
});

describe('C2: Executor must verify contract inputs', () => {
  it('rejects mismatched ownerTableId', async () => {
    const contract = await compileRealContract();
    const { executor } = makeExecutor();
    await expect(
      executor.execute({
        contract,
        ownerTableId: 999,
        body: baseAfter as any,
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
        body: baseAfter as any,
      }),
    ).rejects.toThrow(/hash integrity/i);
  });

  it('rejects an execution body that differs from the compiled target', async () => {
    const contract = await compileRealContract();
    const { executor, journal, tableHandlerService } = makeExecutor();

    await expect(
      executor.execute({
        contract,
        ownerTableId: 42,
        body: {
          ...baseAfter,
          columns: [
            ...baseAfter.columns,
            { name: 'forged', type: 'text', isNullable: true },
          ],
        } as any,
      }),
    ).rejects.toThrow(/execution body.*target revision/i);
    expect(journal.create).not.toHaveBeenCalled();
    expect(tableHandlerService.updateTable).not.toHaveBeenCalled();
  });

  it('executes policy-only changes through the table metadata writer', async () => {
    const before = {
      ...baseBefore,
      columns: [
        {
          ...baseBefore.columns[0],
          fieldPermissions: [
            { id: 9, action: 'read', effect: 'deny', role: { id: 2 } },
          ],
        },
      ],
    };
    const after = {
      ...before,
      columns: [{ ...before.columns[0], fieldPermissions: [] }],
    };
    const contract = await compileRealContract({
      beforeMetadata: before,
      afterMetadata: after,
    });
    const { executor, tableHandlerService } = makeExecutor({
      queryBuilderService: {
        findOne: vi.fn().mockResolvedValueOnce(before).mockResolvedValue(after),
      },
    });

    await executor.execute({
      contract,
      ownerTableId: 42,
      body: after as any,
    });

    expect(contract.context.diff.schemaChanged).toBe(false);
    expect(contract.context.diff.policyMetadataChanged).toBe(true);
    expect(tableHandlerService.updateTable).toHaveBeenCalledOnce();
  });
});

describe('C3: Source revision must be attested under lock', () => {
  it('rejects create when target metadata already exists under the lock', async () => {
    const contract = await compileRealContract({
      operation: 'create',
      tableId: null,
      beforeMetadata: null,
      afterMetadata: baseAfter,
    });
    const tableHandlerService = {
      createTable: vi.fn(async (_body: any, ctx: any) => {
        if (ctx?.$onLockAcquired) await ctx.$onLockAcquired();
        return { id: 42, affectedTables: ['post'] };
      }),
    };
    const { executor } = makeExecutor({
      tableHandlerService,
      queryBuilderService: { findOne: vi.fn().mockResolvedValue(baseBefore) },
    });

    await expect(
      executor.execute({ contract, body: baseAfter as any }),
    ).rejects.toThrow(/source attestation failed.*already exists/i);
  });

  it('rejects stale sourceRevision when metadata has changed', async () => {
    const contract = await compileRealContract();
    const differentTable = {
      name: 'post',
      columns: [
        {
          id: 1,
          name: 'id',
          type: 'int',
          isPrimary: true,
          isGenerated: true,
          isNullable: false,
        },
        {
          id: 99,
          name: 'extra',
          type: 'text',
          isPrimary: false,
          isGenerated: false,
          isNullable: true,
        },
      ],
      relations: [],
    };
    const { executor } = makeExecutor({
      queryBuilderService: {
        findOne: vi.fn().mockResolvedValue(differentTable),
      },
    });
    await expect(
      executor.execute({
        contract,
        ownerTableId: 42,
        body: baseAfter as any,
      }),
    ).rejects.toThrow(/source revision stale/i);
  });

  it('passes when source revision matches current metadata', async () => {
    const contract = await compileRealContract();
    const { executor, tableHandlerService } = makeExecutor({
      queryBuilderService: {
        findOne: vi
          .fn()
          .mockResolvedValueOnce(baseBefore)
          .mockResolvedValue(baseAfter),
      },
    });
    const result = await executor.execute({
      contract,
      ownerTableId: 42,
      body: baseAfter as any,
    });
    expect(result.mutationId).toBe(contract.mutationId);
    expect(tableHandlerService.updateTable).toHaveBeenCalled();
  });
});

describe('C3b: Target revision must be attested inside the UOW', () => {
  it('rejects source physical drift before the table handler can mutate', async () => {
    const contract = await compileRealContract();
    const targetAttestor = {
      assertSource: vi
        .fn()
        .mockRejectedValue(
          new Error('unexpected physical index post(title, id)'),
        ),
      assertTarget: vi.fn().mockResolvedValue(undefined),
    };
    const { executor, tableHandlerService, journal } = makeExecutor({
      runtimeSchemaTargetAttestorService: targetAttestor,
    });

    await expect(
      executor.execute({
        contract,
        ownerTableId: 42,
        body: baseAfter as any,
      }),
    ).rejects.toThrow(/unexpected physical index/i);

    expect(tableHandlerService.updateTable).not.toHaveBeenCalled();
    expect(journal.markFailed).toHaveBeenCalledWith(
      contract.mutationId,
      expect.stringMatching(/unexpected physical index/i),
    );
  });

  it('does not publish target_attested when physical target proof fails', async () => {
    const contract = await compileRealContract();
    const targetAttestor = {
      assertSource: vi.fn().mockResolvedValue(undefined),
      assertTarget: vi
        .fn()
        .mockRejectedValue(
          new Error('physical index post.idx_post_title is missing'),
        ),
    };
    const { executor, journal } = makeExecutor({
      runtimeSchemaTargetAttestorService: targetAttestor,
    });

    await expect(
      executor.execute({
        contract,
        ownerTableId: 42,
        body: baseAfter as any,
      }),
    ).rejects.toThrow(/physical index/i);
    expect(journal.advanceStage).not.toHaveBeenCalledWith(
      contract.mutationId,
      'target_attested',
    );
  });

  it('rejects a handler that reports success without applying the metadata target', async () => {
    const contract = await compileRealContract();
    const { executor, journal } = makeExecutor({
      queryBuilderService: {
        findOne: vi.fn().mockResolvedValue(baseBefore),
      },
    });

    await expect(
      executor.execute({
        contract,
        ownerTableId: 42,
        body: baseAfter as any,
      }),
    ).rejects.toThrow(/target revision mismatch/i);
    expect(journal.advanceStage).not.toHaveBeenCalledWith(
      contract.mutationId,
      'target_attested',
    );
  });

  it('attests inverse deletion while preserving hydrated many-to-many relations', async () => {
    const inverseRoute = {
      id: 14,
      propertyName: 'allowedRoutePermissions',
      type: 'many-to-many',
      targetTable: { id: 8, name: 'enfyra_route_permission' },
      mappedBy: { id: 13, propertyName: 'allowedUsers' },
      junctionTableName: 'j_9475348e0853',
    };
    const inverseGraphql = {
      id: 2314,
      propertyName: 'allowedGraphqlPermissions',
      type: 'many-to-many',
      targetTable: { id: 824, name: 'enfyra_graphql_permission' },
      mappedBy: { id: 2309, propertyName: 'allowedUsers' },
      junctionTableName: 'j_ab366c6a77d1',
    };
    const ownerRole = {
      id: 6,
      propertyName: 'role',
      type: 'many-to-one',
      targetTable: { id: 7, name: 'enfyra_role' },
      foreignKeyColumn: 'roleId',
    };
    const deletedInverse = {
      id: 2379,
      propertyName: 'gwTestUsage',
      type: 'one-to-many',
      targetTable: { id: 896, name: 'gw_child' },
      mappedBy: { id: 2378, propertyName: 'testUser' },
    };
    const before = {
      name: 'enfyra_user',
      columns: [],
      relations: [ownerRole, inverseRoute, inverseGraphql, deletedInverse],
    };
    const target = {
      ...before,
      relations: [
        ownerRole,
        { ...inverseRoute, mappedBy: 'allowedUsers' },
        { ...inverseGraphql, mappedBy: 'allowedUsers' },
      ],
    };
    const persistedTarget = {
      ...before,
      relations: [ownerRole, inverseRoute, inverseGraphql],
    };
    const compiler = makeCompiler();
    const { contract } = await compiler.compile({
      operation: 'update',
      tableName: 'enfyra_user',
      tableId: '4',
      beforeMetadata: before,
      afterMetadata: target,
    });
    const updateTable = vi.fn(async () => ({
      id: 4,
      affectedTables: ['enfyra_user'],
    }));
    const { executor } = makeExecutor({
      tableHandlerService: { updateTable },
      queryBuilderService: {
        findOne: vi
          .fn()
          .mockResolvedValueOnce(before)
          .mockResolvedValue(persistedTarget),
      },
    });

    await expect(
      executor.execute({
        contract,
        ownerTableId: 4,
        body: target as any,
      }),
    ).resolves.toMatchObject({ recordId: 4 });
    expect(updateTable).toHaveBeenCalledWith(
      4,
      expect.objectContaining({
        relations: expect.arrayContaining([
          expect.objectContaining({ id: 6 }),
          expect.objectContaining({ id: 14 }),
          expect.objectContaining({ id: 2314 }),
        ]),
      }),
      expect.any(Object),
    );
    expect(updateTable.mock.calls[0][1].relations).toHaveLength(3);
    expect(updateTable.mock.calls[0][1].relations).not.toEqual(
      expect.arrayContaining([expect.objectContaining({ id: 2378 })]),
    );
  });

  it('rejects create when the created aggregate cannot be reloaded', async () => {
    const contract = await compileRealContract({
      operation: 'create',
      tableId: null,
      beforeMetadata: null,
      afterMetadata: baseAfter,
    });
    const tableHandlerService = {
      createTable: vi
        .fn()
        .mockResolvedValue({ id: 42, affectedTables: ['post'] }),
    };
    const { executor } = makeExecutor({
      tableHandlerService,
      queryBuilderService: { findOne: vi.fn().mockResolvedValue(null) },
    });

    await expect(
      executor.execute({ contract, body: baseAfter as any }),
    ).rejects.toThrow(/target attestation failed/i);
  });

  it('rejects delete when the target aggregate still exists', async () => {
    const contract = await compileRealContract({
      operation: 'delete',
      beforeMetadata: baseBefore,
      afterMetadata: null,
    });
    const tableHandlerService = {
      delete: vi.fn(async (_id: any, ctx: any) => {
        if (ctx?.$onLockAcquired) await ctx.$onLockAcquired();
        return { id: 42, affectedTables: ['post'] };
      }),
    };
    const { executor } = makeExecutor({
      tableHandlerService,
      queryBuilderService: { findOne: vi.fn().mockResolvedValue(baseBefore) },
    });

    await expect(executor.execute({ contract, tableId: 42 })).rejects.toThrow(
      /target attestation failed/i,
    );
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
      body: baseAfter as any,
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
    const runtimeSchemaTargetAttestorService = {
      assertSource: vi.fn().mockResolvedValue(undefined),
      assertTarget: vi.fn().mockResolvedValue(undefined),
    };
    const { executor } = makeExecutor({
      tableHandlerService,
      journal,
      runtimeSchemaTargetAttestorService,
    });
    const result = await executor.execute({
      contract,
      ownerTableId: 42,
      body: baseAfter as any,
    });
    expect(journal.markCompleted).not.toHaveBeenCalled();
    expect((result as any).preview).toBeDefined();
    expect(
      runtimeSchemaTargetAttestorService.assertTarget,
    ).not.toHaveBeenCalled();
  });
});

describe('Journal: must support retry after failure', () => {
  it('journal.create resets failed entry instead of throwing duplicate key', async () => {
    const { RuntimeSchemaJournalService } =
      await import('../../src/modules/table-management/services/runtime-schema-journal.service');
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
    await journal.create({
      mutationId: 'test-mutation',
      contractHash: 'hash1',
      backend: 'postgresql',
    });
    await journal.markFailed('test-mutation', 'some error');

    // Retry with same mutationId must succeed (reset failed entry)
    await expect(
      journal.create({
        mutationId: 'test-mutation',
        contractHash: 'hash1',
        backend: 'postgresql',
      }),
    ).resolves.toBeUndefined();
  });

  it('journal.create starts a new execution after a completed mutation', async () => {
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
            first: async () => store.get('completed-mutation') ?? null,
            update: async (fields: any) => {
              const existing = store.get('completed-mutation');
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

    await journal.create({
      mutationId: 'completed-mutation',
      contractHash: 'hash1',
      backend: 'postgresql',
    });
    await journal.markCompleted('completed-mutation');
    await journal.create({
      mutationId: 'completed-mutation',
      contractHash: 'hash1',
      backend: 'postgresql',
    });

    expect(store.get('completed-mutation')).toEqual(
      expect.objectContaining({ stage: 'captured', error: null }),
    );
  });

  it('journal.create rejects in-progress mutation', async () => {
    const { RuntimeSchemaJournalService } =
      await import('../../src/modules/table-management/services/runtime-schema-journal.service');
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

    await journal.create({
      mutationId: 'test-mutation-2',
      contractHash: 'hash1',
      backend: 'postgresql',
    });
    await journal.advanceStage('test-mutation-2', 'executing');

    // In-progress mutation must be rejected
    await expect(
      journal.create({
        mutationId: 'test-mutation-2',
        contractHash: 'hash1',
        backend: 'postgresql',
      }),
    ).rejects.toThrow(/already in progress/i);
  });
});

describe('C4: Batch create must not bypass router', () => {
  it('DynamicBatchCreationService checks handles() for schema tables', async () => {
    const fs = await import('node:fs');
    const source = fs.readFileSync(
      new URL(
        '../../src/modules/dynamic-api/services/dynamic-batch-creation.service.ts',
        import.meta.url,
      ),
      'utf-8',
    );
    expect(source).toContain("tableName === 'enfyra_table'");
    expect(source).toContain(
      'runtimeMetadataSchemaRouterService.handles(tableName)',
    );
  });
});

describe('C5: Delete must require confirmation', () => {
  it('policy returns preview for delete without confirmation hash', async () => {
    const { SchemaMigrationValidatorService } =
      await import('../../src/domain/policy/services/schema-migration-validator.service');
    const compiler = makeCompiler();
    const validator = new SchemaMigrationValidatorService({
      runtimeRegistryService: {
        getMetadata: () => ({ tables: new Map() }),
        requireMetadata: () => ({ tables: new Map() }),
      } as any,
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
      relations: [
        {
          id: 10,
          propertyName: 'author',
          type: 'many-to-one',
          targetTable: { name: 'user' },
          isNullable: true,
        },
      ],
    });
    // After router fix: body preserves targetTableName alongside numeric ID
    const after = normalizeRuntimeTableSchema({
      name: 'post',
      columns: [{ id: 1, name: 'id', type: 'int', isPrimary: true }],
      relations: [
        {
          id: 10,
          propertyName: 'author',
          type: 'many-to-one',
          targetTable: 5,
          targetTableName: 'user',
          isNullable: true,
        },
      ],
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
      columns: [
        {
          id: 1,
          name: 'id',
          type: 'int',
          isPrimary: true,
          isGenerated: true,
          isNullable: false,
        },
      ],
      relations: [
        {
          id: 10,
          propertyName: 'author',
          type: 'many-to-one',
          targetTableName: 'user',
          foreignKeyColumn: 'authorId',
          isNullable: true,
          onDelete: 'SET NULL',
        },
      ],
    };
    const after = {
      name: 'post',
      columns: [
        {
          id: 1,
          name: 'id',
          type: 'int',
          isPrimary: true,
          isGenerated: true,
          isNullable: false,
        },
      ],
      relations: [
        {
          id: 10,
          propertyName: 'author',
          type: 'many-to-one',
          targetTableName: 'user',
          foreignKeyColumn: 'authorId',
          isNullable: true,
          onDelete: 'CASCADE',
        },
      ],
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

describe('H8: contract diff surfaces canonical drift', () => {
  it('reports bounded structured diff on target revision mismatch', async () => {
    const executor = makeExecutor({});
    const contract = await compileRealContract({
      afterMetadata: {
        ...baseAfter,
        relations: [
          {
            id: 10,
            propertyName: 'author',
            type: 'many-to-one',
            targetTableName: 'user',
            mappedBy: '13',
            isNullable: true,
            onDelete: 'SET NULL',
          },
        ],
      },
    });

    const drifted = {
      ...baseAfter,
      relations: [
        {
          id: 10,
          propertyName: 'author',
          type: 'many-to-one',
          targetTable: { id: 5, name: 'user' },
          mappedBy: { id: 99, propertyName: 'posts' },
          isNullable: true,
          onDelete: 'SET NULL',
        },
      ],
    };
    executor.queryBuilderService.findOne
      .mockReset()
      .mockResolvedValueOnce(baseBefore)
      .mockResolvedValue(drifted);

    await expect(
      executor.executor.execute({
        contract,
        tableId: '42',
        context: {},
      }),
    ).rejects.toThrow(/diff=relations\[author\(mappedBy:"13"->"posts"\)\]/);
  });
});
