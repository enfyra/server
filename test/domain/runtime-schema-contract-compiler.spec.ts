import { RuntimeSchemaContractCompilerService } from '../../src/modules/table-management';
import { verifySchemaMutationContractHash } from '../../src/shared/utils/schema-mutation-contract.util';

function createCompiler(input?: {
  dbType?: 'postgres' | 'mysql' | 'mongodb';
  tables?: Map<string, any>;
}) {
  return new RuntimeSchemaContractCompilerService({
    databaseConfigService: {
      getDbType: () => input?.dbType ?? 'postgres',
    } as any,
    runtimeRegistryService: {
      getMetadata: () => ({ tables: input?.tables ?? new Map() }),
    } as any,
  });
}

const baseTable = {
  id: 7,
  name: 'post',
  columns: [
    {
      id: 1,
      name: 'id',
      type: 'int',
      isNullable: false,
      isPrimary: true,
      isGenerated: true,
      defaultValue: null,
    },
    {
      id: 2,
      name: 'title',
      type: 'varchar',
      isNullable: false,
      isPrimary: false,
      isGenerated: false,
      defaultValue: null,
    },
  ],
  relations: [],
  uniques: null,
  indexes: null,
};

describe('RuntimeSchemaContractCompilerService', () => {
  it('compiles logical changes into dependency-derived phases', async () => {
    const compiler = createCompiler({ dbType: 'mysql' });
    const result = await compiler.compile({
      operation: 'update',
      tableName: 'post',
      tableId: 7,
      beforeMetadata: baseTable,
      afterMetadata: {
        ...baseTable,
        name: 'article',
        columns: [
          baseTable.columns[0],
          { ...baseTable.columns[1], name: 'heading' },
          {
            id: 99,
            name: 'slug',
            type: 'varchar',
            isNullable: true,
            isPrimary: false,
            isGenerated: false,
            defaultValue: null,
          },
        ],
        indexes: [['slug']],
      },
    });

    expect(result.contract.backend).toBe('mysql');
    expect(result.contract.context.diff.renamedColumns).toEqual([
      { from: 'title', to: 'heading' },
    ]);
    expect(result.contract.changes.map((change) => change.kind)).toEqual([
      'rename-table',
      'add-column',
      'rename-column',
      'add-index',
    ]);
    expect(result.contract.phases.length).toBeGreaterThan(
      result.contract.changes.length,
    );
    expect(
      result.contract.phases
        .flatMap((phase) => phase.nodes)
        .filter((node) => node.completesChange),
    ).toHaveLength(result.contract.changes.length);
    expect(verifySchemaMutationContractHash(result.contract)).toBe(true);
  });

  it('does not bake generated metadata ids into contract identity', async () => {
    const compiler = createCompiler();
    const buildAfter = (id: number) => ({
      ...baseTable,
      columns: [
        ...baseTable.columns,
        {
          id,
          name: 'slug',
          type: 'varchar',
          isNullable: true,
          isPrimary: false,
          isGenerated: false,
          defaultValue: null,
        },
      ],
    });
    const preview = await compiler.compile({
      operation: 'update',
      tableName: 'post',
      beforeMetadata: baseTable,
      afterMetadata: buildAfter(100),
    });
    const confirm = await compiler.compile({
      operation: 'update',
      tableName: 'post',
      beforeMetadata: baseTable,
      afterMetadata: buildAfter(250),
    });

    expect(confirm.requiredConfirmHash).toBe(preview.requiredConfirmHash);
    expect(confirm.contract.contractHash).toBe(preview.contract.contractHash);
  });

  it('adds inverse relation cascade owners to the affected closure', async () => {
    const before = {
      ...baseTable,
      relations: [
        {
          id: 40,
          propertyName: 'comments',
          type: 'many-to-many',
          targetTableName: 'comment',
          junctionTableName: 'j_post_comment',
        },
      ],
    };
    const tables = new Map([
      [
        'comment',
        {
          name: 'comment',
          relations: [
            {
              id: 41,
              propertyName: 'posts',
              mappedById: 40,
            },
          ],
        },
      ],
    ]);
    const result = await createCompiler({ tables }).compile({
      operation: 'update',
      tableName: 'post',
      beforeMetadata: before,
      afterMetadata: { ...before, relations: [] },
    });

    expect(result.contract.context.affectedResources.tables).toEqual([
      'comment',
      'post',
    ]);
    expect(result.contract.context.affectedResources.relationIds).toEqual([
      '40',
      '41',
    ]);
    expect(
      result.contract.context.diff.owningSideInverseCascadeWarnings,
    ).toHaveLength(1);
  });

  it('returns an empty execution graph for a schema no-op', async () => {
    const result = await createCompiler({ dbType: 'mongodb' }).compile({
      operation: 'update',
      tableName: 'post',
      beforeMetadata: baseTable,
      afterMetadata: structuredClone(baseTable),
    });

    expect(result.contract.backend).toBe('mongodb');
    expect(result.contract.context.diff.schemaChanged).toBe(false);
    expect(result.contract.changes).toEqual([]);
    expect(result.contract.phases).toEqual([]);
  });
});
