import { RuntimeMetadataSchemaRouterService } from '../../src/modules/table-management';

function createHarness(input?: { mongo?: boolean; preview?: boolean }) {
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
      if (query.table === 'enfyra_table') return ownerTable;
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
        schemaChanged: true,
        isDestructive: input?.preview === true,
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
    } as any,
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
  };
}

describe('RuntimeMetadataSchemaRouterService', () => {
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
    expect(
      harness.runtimeSchemaExecutorService.execute,
    ).toHaveBeenCalledOnce();
    expect(result).toEqual({
      recordId: 99,
      ownerTableId: 10,
      affectedTables: ['user'],
    });
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
    expect(
      harness.runtimeSchemaExecutorService.execute,
    ).not.toHaveBeenCalled();
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
    expect(
      harness.runtimeSchemaExecutorService.execute,
    ).toHaveBeenCalledOnce();
  });

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
        targetTableName: 'post',
      }),
    );
    expect(body.relations[0]).not.toHaveProperty('sourceTable');
  });
});
