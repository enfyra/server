import { describe, it, expect, vi, beforeEach } from 'vitest';
import { MetadataRepairService } from 'src/engines/bootstrap';
import { DatabaseConfigService } from 'src/shared/services';

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
  }>,
) {
  return {
    id: 10,
    name: 'post',
    isSystem: false,
    uniques: [],
    indexes: [],
    relations: [],
    ...overrides,
  };
}

function makeQb(findImpl: (args: any) => any, updateMock: any) {
  return {
    find: vi.fn(findImpl as any),
    update: updateMock,
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

describe('MetadataRepairService.runIfNeeded', () => {
  beforeEach(() => {
    DatabaseConfigService.overrideForTesting?.('postgres');
  });

  it('skips when flag already true', async () => {
    const update = vi.fn();
    const qb = makeQb(() => ({ data: [makeSetting(true)] }), update);
    const cache = makeCache([]);
    const svc = new MetadataRepairService({
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
    const svc = new MetadataRepairService({
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
    const svc = new MetadataRepairService({
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
    const svc = new MetadataRepairService({
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
    const svc = new MetadataRepairService({
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
    const svc = new MetadataRepairService({
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
    const svc = new MetadataRepairService({
      queryBuilderService: qb,
      metadataCacheService: cache,
    });

    await svc.runIfNeeded();

    const tableUpdate = update.mock.calls.find(
      (c: any) => c[0] === 'table_definition',
    );
    expect(tableUpdate![2].uniques).toEqual([['author']]);
  });
});
