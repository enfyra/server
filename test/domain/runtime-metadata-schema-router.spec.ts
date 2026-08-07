import { RuntimeMetadataSchemaRouterService } from '../../src/modules/table-management';
import { getSqlJunctionPhysicalNames } from '../../src/modules/table-management/utils/sql-junction-naming.util';
import { TableManagementValidationService } from '../../src/modules/table-management/services/table-validation.service';

function createHarness(input?: {
  mongo?: boolean;
  preview?: boolean;
  database?: 'postgres' | 'mysql';
}) {
  const ownerTable = {
    id: 10,
    _id: 'owner-mongo',
    name: 'post',
    columns: [
      {
        id: 1,
        _id: 'column-id',
        name: input?.mongo ? '_id' : 'id',
        type: input?.mongo ? 'object-id' : 'int',
        isPrimary: true,
        isGenerated: true,
        table: input?.mongo ? 'owner-mongo' : 10,
      },
      {
        id: 2,
        _id: 'column-title',
        name: 'title',
        type: 'varchar',
        table: input?.mongo ? 'owner-mongo' : 10,
      },
    ],
    relations: [
      {
        id: 3,
        _id: 'relation-author',
        propertyName: 'author',
        type: 'many-to-one',
        sourceTable: input?.mongo ? 'owner-mongo' : 10,
        targetTable: { id: 20, _id: 'target-mongo', name: 'user' },
      },
    ],
    indexes: [],
    uniques: [],
  };
  const queryBuilderService = {
    findOne: vi.fn(async (query: any) => {
      if (query.table === 'enfyra_table') {
        const id = query.where?.id ?? query.where?._id;
        if (String(id) === String(input?.mongo ? 'target-mongo' : 20)) {
          return input?.mongo
            ? { _id: 'target-mongo', name: 'user' }
            : { id: 20, name: 'user' };
        }
        return ownerTable;
      }
      if (query.table === 'enfyra_column' && query.where?.name === 'slug') {
        return input?.mongo ? { _id: 'created-column' } : { id: 99 };
      }
      if (query.table === 'enfyra_column') return ownerTable.columns[1];
      if (query.table === 'enfyra_relation') return ownerTable.relations[0];
      return null;
    }),
  };
  const contract = {
    mutationId: 'test-mutation',
    contractHash: 'test-hash',
    context: {
      diff: {
        tableName: 'post',
        operation: 'update',
        schemaChanged: true,
        policyMetadataChanged: false,
        isDestructive: input?.preview === true,
        removedColumns: [],
        addedColumns: [],
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
    },
  };
  const runtimeSchemaContractCompilerService = {
    compile: vi.fn(async () => ({
      contract,
      requiredConfirmHash: 'confirm-me',
    })),
  };
  const runtimeSchemaExecutorService = {
    execute: vi.fn(async () => ({
      mutationId: 'test-mutation',
      contractHash: 'test-hash',
      outputs: new Map(),
      affectedTables: ['user'],
    })),
  };
  const service = new RuntimeMetadataSchemaRouterService({
    queryBuilderService: queryBuilderService as any,
    tableHandlerService: {} as any,
    databaseConfigService: {
      isMongoDb: () => input?.mongo === true,
      getDbType: () =>
        input?.mongo ? 'mongodb' : (input?.database ?? 'postgres'),
    } as any,
    tableManagementValidationService: new TableManagementValidationService(),
    runtimeSchemaContractCompilerService:
      runtimeSchemaContractCompilerService as any,
    runtimeSchemaExecutorService: runtimeSchemaExecutorService as any,
  });
  return {
    service,
    queryBuilderService,
    runtimeSchemaContractCompilerService,
    runtimeSchemaExecutorService,
    ownerTable,
    contract,
  };
}

describe('RuntimeMetadataSchemaRouterService', () => {
  it.each(['postgres', 'mysql'] as const)(
    'rejects reserved column names before compiling a %s schema mutation',
    async (database) => {
      const harness = createHarness({ database });

      await expect(
        harness.service.create({
          tableName: 'enfyra_column',
          data: {
            name: 'as',
            type: 'varchar',
            isNullable: true,
            table: { id: 10 },
          },
        }),
      ).rejects.toThrow(/reserved keyword/i);

      expect(harness.queryBuilderService.findOne).not.toHaveBeenCalled();
      expect(
        harness.runtimeSchemaContractCompilerService.compile,
      ).not.toHaveBeenCalled();
      expect(
        harness.runtimeSchemaExecutorService.execute,
      ).not.toHaveBeenCalled();
    },
  );

  it.each([
    {
      name: 'column rename',
      run: (harness: ReturnType<typeof createHarness>) =>
        harness.service.update({
          tableName: 'enfyra_column',
          recordId: 2,
          existing: harness.ownerTable.columns[1],
          data: { name: 'as' },
        }),
    },
    {
      name: 'table replacement',
      run: (harness: ReturnType<typeof createHarness>) =>
        harness.service.updateTable({
          tableId: 10,
          body: {
            columns: [{ id: 1 }, { id: 2, name: 'as' }],
          },
        }),
    },
    {
      name: 'table create',
      run: (harness: ReturnType<typeof createHarness>) =>
        harness.service.createTable({
          body: {
            name: 'comment',
            columns: [{ name: 'as', type: 'varchar' }],
          },
        }),
    },
  ])(
    'rejects a reserved keyword in a $name before compiling',
    async ({ run }) => {
      const harness = createHarness({ database: 'postgres' });

      await expect(run(harness)).rejects.toThrow(/reserved keyword/i);

      expect(harness.queryBuilderService.findOne).not.toHaveBeenCalled();
      expect(
        harness.runtimeSchemaContractCompilerService.compile,
      ).not.toHaveBeenCalled();
      expect(
        harness.runtimeSchemaExecutorService.execute,
      ).not.toHaveBeenCalled();
    },
  );

  it('allows an SQL-keyword column name for MongoDB before compilation', async () => {
    const harness = createHarness({ mongo: true });

    await harness.service.create({
      tableName: 'enfyra_column',
      data: {
        name: 'as',
        type: 'varchar',
        isNullable: true,
        table: { _id: 'owner-mongo' },
      },
    });

    expect(
      harness.runtimeSchemaContractCompilerService.compile,
    ).toHaveBeenCalledOnce();
  });

  it('routes direct column creation through the owning table contract', async () => {
    const harness = createHarness();
    const result = await harness.service.create({
      tableName: 'enfyra_column',
      data: {
        name: 'slug',
        type: 'varchar',
        isNullable: true,
        table: { id: 10 },
      },
    });

    expect(
      harness.runtimeSchemaContractCompilerService.compile,
    ).toHaveBeenCalledOnce();
    const compileInput =
      harness.runtimeSchemaContractCompilerService.compile.mock.calls[0][0];
    expect(compileInput.tableId).toBe(10);
    const body = compileInput.afterMetadata;
    expect(body.columns.map((column: any) => column.name)).toEqual([
      'id',
      'title',
      'slug',
    ]);
    expect(body.columns[2]).not.toHaveProperty('table');
    expect(harness.runtimeSchemaExecutorService.execute).toHaveBeenCalledOnce();
    expect(result).toEqual({
      recordId: 99,
      ownerTableId: 10,
      affectedTables: ['user'],
      mutationId: 'test-mutation',
    });
  });

  it('canonicalizes unique and index constraints together before compiling a column mutation', async () => {
    const harness = createHarness();
    harness.ownerTable.indexes = [['createdAt']];

    await harness.service.create({
      tableName: 'enfyra_column',
      data: {
        name: 'slug',
        type: 'varchar',
        isUnique: true,
        table: { id: 10 },
      },
    });

    const compileInput =
      harness.runtimeSchemaContractCompilerService.compile.mock.calls[0][0];
    expect(compileInput.afterMetadata.uniques).toEqual([['slug']]);
    expect(compileInput.afterMetadata.indexes).toEqual([['createdAt']]);
  });

  it('returns preview for destructive operations without confirm hash', async () => {
    const harness = createHarness({ preview: true });
    const result = await harness.service.update({
      tableName: 'enfyra_column',
      recordId: 2,
      existing: harness.ownerTable.columns[1],
      data: { name: 'heading' },
    });

    expect(result.preview).toBeDefined();
    expect(result.preview!._preview).toBe(true);
    expect(result.preview!.requiredConfirmHash).toBe('confirm-me');
    expect(harness.runtimeSchemaExecutorService.execute).not.toHaveBeenCalled();
  });

  it('accepts the snake_case confirmation query used by the app', async () => {
    const harness = createHarness();

    const result = await harness.service.updateTable({
      tableId: 10,
      body: { description: 'Updated description' },
      context: { $query: { schema_confirm_hash: 'confirm-me' } } as any,
    });

    expect(result.preview).toBeUndefined();
    expect(harness.runtimeSchemaExecutorService.execute).toHaveBeenCalledOnce();
  });

  it('exposes flattened diff details in a schema preview', async () => {
    const harness = createHarness();
    harness.contract.context.diff.changedColumns = ['title'];

    const result = await harness.service.updateTable({
      tableId: 10,
      body: { description: 'Updated description' },
    });

    expect(result.preview).toEqual(expect.objectContaining({
      _preview: true,
      changedColumns: ['title'],
      addedRelationsCount: 0,
      removedRelationsCount: 0,
    }));
  });

  it('routes relation deletion through a complete replacement aggregate', async () => {
    const harness = createHarness();
    await harness.service.delete({
      tableName: 'enfyra_relation',
      recordId: 3,
      existing: harness.ownerTable.relations[0],
      context: { $query: { schemaConfirmHash: 'confirm-me' } },
    });

    const compileInput =
      harness.runtimeSchemaContractCompilerService.compile.mock.calls[0][0];
    const body = compileInput.afterMetadata;
    expect(body.relations).toEqual([]);
    expect(body.columns).toHaveLength(2);
    expect(harness.runtimeSchemaExecutorService.execute).toHaveBeenCalledOnce();
  });

  it('reconstructs the complete table aggregate for sparse table patches', async () => {
    const harness = createHarness();
    await harness.service.updateTable({
      tableId: 10,
      body: { description: 'Updated description' },
      context: { $query: { schemaConfirmHash: 'confirm-me' } },
    });

    const compileInput =
      harness.runtimeSchemaContractCompilerService.compile.mock.calls[0][0];
    expect(compileInput.afterMetadata).toEqual(
      expect.objectContaining({
        id: 10,
        name: 'post',
        description: 'Updated description',
      }),
    );
    expect(compileInput.afterMetadata.columns).toEqual(
      harness.ownerTable.columns,
    );
    expect(compileInput.afterMetadata.relations).toEqual([
      {
        ...harness.ownerTable.relations[0],
        targetTableName: 'user',
      },
    ]);
  });

  it('expands id-only children before compiling a table replacement', async () => {
    const harness = createHarness();
    await harness.service.updateTable({
      tableId: 10,
      body: {
        columns: [{ id: 1 }, { id: 2 }, { name: 'slug', type: 'varchar' }],
      },
      context: { $query: { schemaConfirmHash: 'confirm-me' } },
    });

    const compileInput =
      harness.runtimeSchemaContractCompilerService.compile.mock.calls[0][0];
    expect(compileInput.afterMetadata.columns).toEqual([
      harness.ownerTable.columns[0],
      harness.ownerTable.columns[1],
      { name: 'slug', type: 'varchar' },
    ]);
    expect(
      harness.runtimeSchemaExecutorService.execute.mock.calls[0][0].body,
    ).toEqual(compileInput.afterMetadata);
  });

  it('matches SQL child ids canonically when the request uses numeric strings', async () => {
    const harness = createHarness();
    await harness.service.updateTable({
      tableId: 10,
      body: { columns: [{ id: '1' }, { id: '2' }] },
      context: { $query: { schemaConfirmHash: 'confirm-me' } },
    });

    const compileInput =
      harness.runtimeSchemaContractCompilerService.compile.mock.calls[0][0];
    expect(compileInput.afterMetadata.columns).toEqual([
      { ...harness.ownerTable.columns[0], id: '1' },
      { ...harness.ownerTable.columns[1], id: '2' },
    ]);
  });

  it('resolves relation target ids before compiling a table create', async () => {
    const harness = createHarness();
    await harness.service.createTable({
      body: {
        name: 'comment',
        columns: [],
        relations: [
          {
            propertyName: 'author',
            type: 'many-to-one',
            targetTable: '20',
            targetTableName: 'forged_name',
          },
        ],
      },
    });

    const compileInput =
      harness.runtimeSchemaContractCompilerService.compile.mock.calls[0][0];
    expect(compileInput.afterMetadata.relations[0]).toEqual(
      expect.objectContaining({
        targetTable: '20',
        targetTableName: 'user',
      }),
    );
  });

  it('deduplicates relation target lookups within one contract snapshot', async () => {
    const harness = createHarness();
    await harness.service.createTable({
      body: {
        name: 'comment',
        columns: [],
        relations: [
          { propertyName: 'author', type: 'many-to-one', targetTable: 20 },
          {
            propertyName: 'editor',
            type: 'many-to-one',
            targetTable: { id: 20 },
          },
        ],
      },
    });

    const targetLookups = harness.queryBuilderService.findOne.mock.calls
      .map(([query]: any[]) => query)
      .filter(
        (query: any) =>
          query.table === 'enfyra_table' && String(query.where?.id) === '20',
      );
    expect(targetLookups).toHaveLength(1);
  });

  it('rejects relation target objects without an id', async () => {
    const harness = createHarness();
    await expect(
      harness.service.createTable({
        body: {
          name: 'comment',
          columns: [],
          relations: [
            {
              propertyName: 'author',
              type: 'many-to-one',
              targetTable: { name: 'user' },
            },
          ],
        },
      }),
    ).rejects.toThrow(/requires a target table id/i);
  });

  it('resolves object relation targets before compiling a Mongo table update', async () => {
    const harness = createHarness({ mongo: true });
    await harness.service.updateTable({
      tableId: 'owner-mongo',
      body: {
        relations: [
          {
            propertyName: 'editor',
            type: 'many-to-one',
            targetTable: { _id: 'target-mongo' },
          },
        ],
      },
      context: { $query: { schemaConfirmHash: 'confirm-me' } },
    });

    const compileInput =
      harness.runtimeSchemaContractCompilerService.compile.mock.calls[0][0];
    expect(compileInput.afterMetadata.relations[0]).toEqual(
      expect.objectContaining({
        targetTable: { _id: 'target-mongo' },
        targetTableName: 'user',
      }),
    );
  });

  it('matches Mongo relation ids supplied through the canonical id field', async () => {
    const harness = createHarness({ mongo: true });
    Object.assign(harness.ownerTable.relations[0], {
      type: 'many-to-many',
      junctionTableName: 'j_existing',
      junctionSourceColumn: 'sourceId',
      junctionTargetColumn: 'targetId',
    });
    await harness.service.updateTable({
      tableId: 'owner-mongo',
      body: {
        relations: [
          {
            id: 'relation-author',
            propertyName: 'writers',
            type: 'many-to-many',
            targetTable: { id: 'target-mongo' },
          } as any,
        ],
      },
      context: { $query: { schemaConfirmHash: 'confirm-me' } },
    });

    const compileInput =
      harness.runtimeSchemaContractCompilerService.compile.mock.calls[0][0];
    expect(compileInput.afterMetadata.relations[0]).toEqual(
      expect.objectContaining({
        _id: 'relation-author',
        propertyName: 'writers',
        junctionTableName: 'j_existing',
      }),
    );
  });

  it('materializes Mongo junction mappings for a replacement relation without an id', async () => {
    const harness = createHarness({ mongo: true });
    Object.assign(harness.ownerTable.relations[0], {
      propertyName: 'targets_0',
      type: 'many-to-many',
      junctionTableName: 'j_existing',
      junctionSourceColumn: 'sourceId',
      junctionTargetColumn: 'targetId',
    });
    await harness.service.updateTable({
      tableId: 'owner-mongo',
      body: {
        relations: [
          {
            propertyName: 'targets',
            type: 'many-to-many',
            targetTable: { id: 'target-mongo' },
            inversePropertyName: 'owners',
          } as any,
        ],
      },
      context: { $query: { schemaConfirmHash: 'confirm-me' } },
    });

    const expected = getSqlJunctionPhysicalNames({
      sourceTable: 'post',
      propertyName: 'targets',
      targetTable: 'user',
    });
    const compileInput =
      harness.runtimeSchemaContractCompilerService.compile.mock.calls[0][0];
    expect(compileInput.afterMetadata.relations[0]).toEqual(
      expect.objectContaining(expected),
    );
  });

  it('canonicalizes constraint fields with column and relation renames', async () => {
    const harness = createHarness();
    harness.ownerTable.indexes = [['author']];
    harness.ownerTable.uniques = [['title']];
    await harness.service.updateTable({
      tableId: 10,
      body: {
        columns: [{ id: 1 }, { id: 2, name: 'heading' }] as any,
        relations: [
          {
            id: 3,
            propertyName: 'writer',
            type: 'many-to-one',
            targetTable: { id: 20 },
          } as any,
        ],
        uniques: [['title']] as any,
        indexes: [['author']] as any,
      },
      context: { $query: { schemaConfirmHash: 'confirm-me' } },
    });

    const compileInput =
      harness.runtimeSchemaContractCompilerService.compile.mock.calls[0][0];
    expect(compileInput.afterMetadata.uniques).toEqual([['heading']]);
    expect(compileInput.afterMetadata.indexes).toEqual([['writer']]);
  });

  it.each([
    { mongo: false, mappedBy: { id: 3 }, expected: 'author' },
    {
      mongo: true,
      mappedBy: { _id: 'relation-author' },
      expected: 'author',
    },
  ])(
    'resolves hydrated mappedBy references before compiling a table update',
    async ({ mongo, mappedBy, expected }) => {
      const harness = createHarness({ mongo });
      await harness.service.updateTable({
        tableId: mongo ? 'owner-mongo' : 10,
        body: {
          relations: [
            {
              propertyName: 'comments',
              type: 'one-to-many',
              targetTable: mongo ? { _id: 'target-mongo' } : { id: 20 },
              mappedBy,
            } as any,
          ],
        },
        context: { $query: { schemaConfirmHash: 'confirm-me' } },
      });

      const compileInput =
        harness.runtimeSchemaContractCompilerService.compile.mock.calls[0][0];
      expect(compileInput.afterMetadata.relations[0].mappedBy).toBe(expected);
      expect(
        harness.runtimeSchemaExecutorService.execute.mock.calls[0][0].body
          .relations[0].mappedBy,
      ).toBe(expected);
    },
  );

  it('uses Mongo ids and target references without leaking owner fields', async () => {
    const harness = createHarness({ mongo: true });
    await harness.service.update({
      tableName: 'enfyra_relation',
      recordId: 'relation-author',
      existing: harness.ownerTable.relations[0],
      data: { propertyName: 'writer' },
    });

    const compileInput =
      harness.runtimeSchemaContractCompilerService.compile.mock.calls[0][0];
    expect(compileInput.tableId).toBe('owner-mongo');
    const body = compileInput.afterMetadata;
    expect(body.relations[0]).toEqual(
      expect.objectContaining({
        _id: 'relation-author',
        propertyName: 'writer',
        targetTable: { _id: 'target-mongo' },
        targetTableName: 'user',
      }),
    );
    expect(body.relations[0]).not.toHaveProperty('sourceTable');
  });
});
