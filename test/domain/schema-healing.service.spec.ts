import { describe, it, expect, vi, beforeEach } from 'vitest';
import { SchemaHealingService } from '../../src/engines/bootstrap';
import { DatabaseConfigService } from '../../src/shared/services';
import { getSqlJunctionPhysicalNames } from '../../src/modules/table-management/utils/sql-junction-naming.util';

function makeSetting(flag: boolean | undefined) {
  return {
    id: 1,
    isInit: true,
    uniquesIndexesRepaired: flag,
  };
}

function makeTable(
  overrides: Partial<{
    id: number;
    name: string;
    isSystem: boolean;
    uniques: any;
    indexes: any;
    relations: any[];
    columns: any[];
  }>,
) {
  return {
    id: 10,
    name: 'post',
    isSystem: false,
    uniques: [],
    indexes: [],
    relations: [],
    columns: [],
    ...overrides,
  };
}

function makeQb(findImpl: (args: any) => any, updateMock: any) {
  const knex: any = vi.fn(() => ({
    leftJoin: vi.fn().mockReturnThis(),
    select: vi.fn().mockResolvedValue([]),
  }));
  return {
    find: vi.fn(findImpl as any),
    update: updateMock,
    getDatabaseType: vi.fn().mockReturnValue('postgres'),
    getKnex: vi.fn().mockReturnValue(knex),
    getMongoDb: vi.fn().mockReturnValue({
      collection: vi.fn().mockReturnValue({
        find: vi.fn().mockReturnValue({ toArray: vi.fn().mockResolvedValue([]) }),
        updateMany: vi.fn().mockResolvedValue({ modifiedCount: 0 }),
        createIndex: vi.fn().mockResolvedValue(undefined),
      }),
      createCollection: vi.fn().mockResolvedValue(undefined),
      listCollections: vi.fn().mockReturnValue({
        toArray: vi.fn().mockResolvedValue([]),
      }),
    }),
  } as any;
}

function makeCache(tables: any[]) {
  return {
    getAllTablesMetadata: vi.fn().mockResolvedValue(tables),
    clearMetadataCache: vi.fn().mockResolvedValue(undefined),
    getMetadata: vi
      .fn()
      .mockResolvedValue({ tables: new Map(), tablesList: tables }),
  } as any;
}

describe('SchemaHealingService.runIfNeeded', () => {
  beforeEach(() => {
    DatabaseConfigService.overrideForTesting?.('postgres');
  });

  it('skips when flag already true', async () => {
    const update = vi.fn();
    const qb = makeQb(() => ({ data: [makeSetting(true)] }), update);
    const cache = makeCache([]);
    const svc = new SchemaHealingService({
      queryBuilderService: qb,
      metadataCacheService: cache,
    });

    await svc.runIfNeeded();

    expect(cache.getAllTablesMetadata).not.toHaveBeenCalled();
    expect(update).not.toHaveBeenCalled();
  });

  it('skips when no setting record exists', async () => {
    const update = vi.fn();
    const qb = makeQb(() => ({ data: [] }), update);
    const cache = makeCache([]);
    const svc = new SchemaHealingService({
      queryBuilderService: qb,
      metadataCacheService: cache,
    });

    await svc.runIfNeeded();

    expect(cache.getAllTablesMetadata).not.toHaveBeenCalled();
    expect(update).not.toHaveBeenCalled();
  });

  it('marks flag true even when no repairs needed (no-op pass)', async () => {
    const update = vi.fn().mockResolvedValue(undefined);
    const qb = makeQb(() => ({ data: [makeSetting(false)] }), update);
    const cache = makeCache([
      makeTable({
        uniques: [['name']],
        indexes: [['createdAt']],
        relations: [{ propertyName: 'author', foreignKeyColumn: 'authorId' }],
      }),
    ]);
    const svc = new SchemaHealingService({
      queryBuilderService: qb,
      metadataCacheService: cache,
    });

    await svc.runIfNeeded();

    expect(update).toHaveBeenCalledTimes(1);
    const call = update.mock.calls[0];
    expect(call[0]).toBe('setting_definition');
    expect(call[2]).toEqual({ uniquesIndexesRepaired: true });
  });

  it('normalizes fkColumn to propertyName in uniques and indexes', async () => {
    const update = vi.fn().mockResolvedValue(undefined);
    const qb = makeQb(() => ({ data: [makeSetting(false)] }), update);
    const cache = makeCache([
      makeTable({
        id: 42,
        name: 'comment',
        uniques: [['authorId', 'slug']],
        indexes: [['authorId'], ['postId']],
        relations: [
          { propertyName: 'author', foreignKeyColumn: 'authorId' },
          { propertyName: 'post', foreignKeyColumn: 'postId' },
        ],
      }),
    ]);
    const svc = new SchemaHealingService({
      queryBuilderService: qb,
      metadataCacheService: cache,
    });

    await svc.runIfNeeded();

    expect(update).toHaveBeenCalledTimes(2);
    const tableUpdate = update.mock.calls.find(
      (c: any) => c[0] === 'table_definition',
    );
    expect(tableUpdate).toBeDefined();
    expect(tableUpdate![2]).toEqual({
      uniques: [['author', 'slug']],
      indexes: [['author'], ['post']],
    });

    const settingUpdate = update.mock.calls.find(
      (c: any) => c[0] === 'setting_definition',
    );
    expect(settingUpdate![2]).toEqual({ uniquesIndexesRepaired: true });
  });

  it('repairs Mongo primary key column metadata even when uniques/indexes flag is already true', async () => {
    DatabaseConfigService.overrideForTesting?.('mongodb');
    const update = vi.fn().mockResolvedValue(undefined);
    const columnId = '65f000000000000000000001';
    const qb = makeQb(
      (args: any) =>
        args.table === 'column_definition'
          ? {
              data: [
                {
                  _id: columnId,
                  name: '_id',
                  type: 'uuid',
                  isPrimary: true,
                },
              ],
            }
          : {
              data: [
                {
                  _id: 'setting-id',
                  isInit: true,
                  uniquesIndexesRepaired: true,
                },
              ],
            },
      update,
    );
    const cache = makeCache([]);
    const svc = new SchemaHealingService({
      queryBuilderService: qb,
      metadataCacheService: cache,
    });

    await svc.runIfNeeded();

    expect(update).toHaveBeenCalledTimes(1);
    expect(update).toHaveBeenCalledWith(
      'column_definition',
      { where: [{ field: '_id', operator: '=', value: columnId }] },
      { name: '_id', type: 'ObjectId' },
    );
  });

  it('skips system tables', async () => {
    const update = vi.fn().mockResolvedValue(undefined);
    const qb = makeQb(() => ({ data: [makeSetting(false)] }), update);
    const cache = makeCache([
      makeTable({
        id: 1,
        name: 'user_definition',
        isSystem: true,
        indexes: [['userId']],
        relations: [{ propertyName: 'user', foreignKeyColumn: 'userId' }],
      }),
    ]);
    const svc = new SchemaHealingService({
      queryBuilderService: qb,
      metadataCacheService: cache,
    });

    await svc.runIfNeeded();

    const tableCalls = update.mock.calls.filter(
      (c: any) => c[0] === 'table_definition',
    );
    expect(tableCalls).toHaveLength(0);
  });

  it('leaves uniques/indexes untouched when no fk match', async () => {
    const update = vi.fn().mockResolvedValue(undefined);
    const qb = makeQb(() => ({ data: [makeSetting(false)] }), update);
    const cache = makeCache([
      makeTable({
        uniques: [['provider', 'providerUserId']],
        indexes: [['user']],
        relations: [{ propertyName: 'user', foreignKeyColumn: 'userId' }],
      }),
    ]);
    const svc = new SchemaHealingService({
      queryBuilderService: qb,
      metadataCacheService: cache,
    });

    await svc.runIfNeeded();

    const tableCalls = update.mock.calls.filter(
      (c: any) => c[0] === 'table_definition',
    );
    expect(tableCalls).toHaveLength(0);
  });

  it('parses uniques/indexes when stored as JSON strings', async () => {
    const update = vi.fn().mockResolvedValue(undefined);
    const qb = makeQb(() => ({ data: [makeSetting(false)] }), update);
    const cache = makeCache([
      makeTable({
        id: 7,
        uniques: JSON.stringify([['authorId']]),
        indexes: JSON.stringify([]),
        relations: [{ propertyName: 'author', foreignKeyColumn: 'authorId' }],
      }),
    ]);
    const svc = new SchemaHealingService({
      queryBuilderService: qb,
      metadataCacheService: cache,
    });

    await svc.runIfNeeded();

    const tableUpdate = update.mock.calls.find(
      (c: any) => c[0] === 'table_definition',
    );
    expect(tableUpdate![2].uniques).toEqual([['author']]);
  });

  it('heals SQL M2M junction metadata to the physical contract', async () => {
    const update = vi.fn().mockResolvedValue(undefined);
    const setting = makeSetting(true);
    const relationUpdate = vi.fn().mockResolvedValue(undefined);
    const relationTable = vi.fn(() => ({ update: relationUpdate }));
    const rows = [
      {
        id: 10,
        type: 'many-to-many',
        propertyName: 'students',
        sourceTableName: 'test',
        targetTableName: 'students',
        junctionTableName: 'test_students_students',
        junctionSourceColumn: 'testId',
        junctionTargetColumn: 'studentsId',
      },
      {
        id: 11,
        type: 'many-to-many',
        propertyName: 'tests',
        mappedById: 10,
        sourceTableName: 'students',
        targetTableName: 'test',
        junctionTableName: 'test_students_students',
        junctionSourceColumn: 'studentsId',
        junctionTargetColumn: 'testId',
      },
    ];
    const knex: any = vi.fn((tableName: string) => {
      if (tableName === 'relation_definition as r') {
        return {
          leftJoin: vi.fn().mockReturnThis(),
          select: vi.fn().mockResolvedValue(rows),
        };
      }
      if (tableName === 'relation_definition') {
        return { where: vi.fn().mockReturnValue(relationTable()) };
      }
      throw new Error(`Unexpected table ${tableName}`);
    });
    knex.schema = {
      hasTable: vi.fn(async (tableName: string) => tableName === 'test_students_students'),
      hasColumn: vi.fn(async (_tableName: string, columnName: string) =>
        ['testId', 'studentsId'].includes(columnName),
      ),
      renameTable: vi.fn().mockResolvedValue(undefined),
      alterTable: vi.fn().mockImplementation(
        async (_tableName: string, callback: (table: any) => void) => {
          callback({ renameColumn: vi.fn() });
        },
      ),
      createTable: vi.fn().mockResolvedValue(undefined),
    };
    const qb = {
      find: vi.fn(() => ({ data: [setting] })),
      update,
      getDatabaseType: vi.fn().mockReturnValue('postgres'),
      getKnex: vi.fn().mockReturnValue(knex),
    } as any;
    const svc = new SchemaHealingService({
      queryBuilderService: qb,
      metadataCacheService: makeCache([]),
    });

    await svc.runIfNeeded();

    expect(knex.schema.renameTable).toHaveBeenCalledWith(
      'test_students_students',
      expect.stringMatching(/^j_[a-f0-9]{12}$/),
    );
    expect(knex.schema.alterTable).toHaveBeenCalledTimes(2);
    expect(relationUpdate).toHaveBeenCalledTimes(2);
    expect(relationUpdate.mock.calls[0][0]).toEqual({
      junctionTableName: expect.stringMatching(/^j_[a-f0-9]{12}$/),
      junctionSourceColumn: 'sourceId',
      junctionTargetColumn: 'targetId',
    });
    expect(relationUpdate.mock.calls[1][0]).toEqual({
      junctionTableName: relationUpdate.mock.calls[0][0].junctionTableName,
      junctionSourceColumn: 'targetId',
      junctionTargetColumn: 'sourceId',
    });
  });

  it('heals Mongo M2M junction collection and field names to the physical contract', async () => {
    DatabaseConfigService.overrideForTesting?.('mongodb');
    const junction = getSqlJunctionPhysicalNames({
      sourceTable: 'test',
      propertyName: 'students',
      targetTable: 'students',
    });
    const update = vi.fn().mockResolvedValue(undefined);
    const updateOne = vi.fn().mockResolvedValue({ modifiedCount: 1 });
    const rename = vi.fn().mockResolvedValue(undefined);
    const updateMany = vi.fn().mockResolvedValue({ modifiedCount: 1 });
    const createIndex = vi.fn().mockResolvedValue(undefined);
    const tables = [
      { _id: 'tests-id', name: 'test' },
      { _id: 'students-id', name: 'students' },
    ];
    const relations = [
      {
        _id: 'rel-id',
        type: 'many-to-many',
        propertyName: 'students',
        sourceTable: 'tests-id',
        targetTable: 'students-id',
        foreignKeyColumn: null,
        referencedColumn: null,
        constraintName: null,
        junctionTableName: 'bad_junction',
        junctionSourceColumn: 'testId',
        junctionTargetColumn: 'studentsId',
      },
      {
        _id: 'inverse-id',
        type: 'many-to-many',
        propertyName: 'tests',
        mappedBy: 'rel-id',
        sourceTable: 'students-id',
        targetTable: 'tests-id',
        foreignKeyColumn: null,
        referencedColumn: null,
        constraintName: null,
        junctionTableName: 'bad_junction',
        junctionSourceColumn: 'studentsId',
        junctionTargetColumn: 'testId',
      },
    ];
    const collections: Record<string, any> = {
      relation_definition: {
        find: vi.fn().mockReturnValue({ toArray: vi.fn().mockResolvedValue(relations) }),
        updateOne,
      },
      table_definition: {
        find: vi.fn().mockReturnValue({ toArray: vi.fn().mockResolvedValue(tables) }),
      },
      bad_junction: {
        rename,
      },
    };
    const db = {
      collection: vi.fn((name: string) => {
        collections[name] ||= {
          find: vi.fn().mockReturnValue({ toArray: vi.fn().mockResolvedValue([]) }),
          updateMany,
          createIndex,
        };
        return collections[name];
      }),
      listCollections: vi.fn(({ name }: { name: string }) => ({
        toArray: vi
          .fn()
          .mockResolvedValue(name === 'bad_junction' ? [{ name }] : []),
      })),
      createCollection: vi.fn().mockResolvedValue(undefined),
    };
    const qb = {
      find: vi.fn((args: any) =>
        args.table === 'column_definition'
          ? { data: [] }
          : { data: [{ _id: 'setting-id', isInit: true, uniquesIndexesRepaired: true }] },
      ),
      update,
      getMongoDb: vi.fn().mockReturnValue(db),
    } as any;
    const svc = new SchemaHealingService({
      queryBuilderService: qb,
      metadataCacheService: makeCache([]),
    });

    await svc.runIfNeeded();

    expect(rename).toHaveBeenCalledWith(junction.junctionTableName);
    expect(updateMany).toHaveBeenCalledWith(
      { testId: { $exists: true }, sourceId: { $exists: false } },
      { $rename: { testId: 'sourceId' } },
    );
    expect(updateMany).toHaveBeenCalledWith(
      { studentsId: { $exists: true }, targetId: { $exists: false } },
      { $rename: { studentsId: 'targetId' } },
    );
    expect(updateOne).toHaveBeenCalledTimes(2);
    expect(updateOne.mock.calls[0][1].$set).toEqual({
      junctionTableName: junction.junctionTableName,
      junctionSourceColumn: 'sourceId',
      junctionTargetColumn: 'targetId',
    });
    expect(updateOne.mock.calls[1][1].$set).toEqual({
      junctionTableName: updateOne.mock.calls[0][1].$set.junctionTableName,
      junctionSourceColumn: 'targetId',
      junctionTargetColumn: 'sourceId',
    });
  });
});
