import { DataMigrationService } from '../../src/core/bootstrap/services/data-migration.service';
import { DatabaseConfigService } from '../../src/shared/services/database-config.service';

function makeQueryBuilder(
  overrides: Partial<{
    find: jest.Mock;
    update: jest.Mock;
    delete: jest.Mock;
    isMongoDb: jest.Mock;
  }> = {},
) {
  return {
    find: jest.fn().mockResolvedValue({ data: [] }),
    update: jest.fn().mockResolvedValue(undefined),
    delete: jest.fn().mockResolvedValue(undefined),
    isMongoDb: jest.fn().mockReturnValue(false),
    ...overrides,
  } as any;
}

function makeService(qb: any): DataMigrationService {
  const svc = new DataMigrationService({ queryBuilderService: qb });
  (svc as any).initOld = null;
  return svc;
}

describe('DataMigrationService.transformRecord', () => {
  it('captures non-empty publishedMethods as relation update', () => {
    const svc = makeService(makeQueryBuilder());
    const { newRecord, relationUpdates } = (svc as any).transformRecord(
      'route_definition',
      {
        _unique: { path: { _eq: '/me' } },
        publishedMethods: ['GET', 'POST'],
        isEnabled: true,
      },
    );
    expect(relationUpdates.publishedMethods).toEqual(['GET', 'POST']);
    expect(newRecord.publishedMethods).toBeUndefined();
    expect(newRecord.isEnabled).toBe(true);
  });

  it('captures empty array publishedMethods as relation update (the bug fix)', () => {
    const svc = makeService(makeQueryBuilder());
    const { newRecord, relationUpdates } = (svc as any).transformRecord(
      'route_definition',
      { _unique: { path: { _eq: '/metadata' } }, publishedMethods: [] },
    );
    expect(relationUpdates.publishedMethods).toEqual([]);
    expect(newRecord.publishedMethods).toBeUndefined();
  });

  it('captures empty array availableMethods as relation update', () => {
    const svc = makeService(makeQueryBuilder());
    const { newRecord, relationUpdates } = (svc as any).transformRecord(
      'route_definition',
      { _unique: { path: { _eq: '/test' } }, availableMethods: [] },
    );
    expect(relationUpdates.availableMethods).toEqual([]);
    expect(newRecord.availableMethods).toBeUndefined();
  });

  it('does not capture undefined relation field', () => {
    const svc = makeService(makeQueryBuilder());
    const { relationUpdates } = (svc as any).transformRecord(
      'route_definition',
      { _unique: { path: { _eq: '/test' } }, name: 'hello' },
    );
    expect(relationUpdates.publishedMethods).toBeUndefined();
    expect(relationUpdates.availableMethods).toBeUndefined();
  });

  it('does not capture null relation field', () => {
    const svc = makeService(makeQueryBuilder());
    const { relationUpdates } = (svc as any).transformRecord(
      'route_definition',
      { _unique: { path: { _eq: '/test' } }, publishedMethods: null },
    );
    expect(relationUpdates.publishedMethods).toBeUndefined();
  });
});

describe('DataMigrationService.updateRelations', () => {
  beforeEach(() => {
    DatabaseConfigService.overrideForTesting('mysql');
  });

  afterEach(() => {
    DatabaseConfigService.resetForTesting();
  });

  it('calls update with empty array to clear publishedMethods (the bug fix)', async () => {
    const qb = makeQueryBuilder({
      find: jest.fn().mockResolvedValue({ data: [] }),
    });
    const svc = makeService(qb);

    await (svc as any).updateRelations('route_definition', 99, {
      publishedMethods: [],
    });

    expect(qb.update).toHaveBeenCalledWith('route_definition', 99, {
      publishedMethods: [],
    });
  });

  it('calls update with method IDs when methods are provided', async () => {
    const qb = makeQueryBuilder({
      find: jest.fn().mockResolvedValue({
        data: [{ id: 1 }, { id: 2 }],
      }),
    });
    const svc = makeService(qb);

    await (svc as any).updateRelations('route_definition', 10, {
      publishedMethods: ['GET', 'POST'],
    });

    expect(qb.update).toHaveBeenCalledWith('route_definition', 10, {
      publishedMethods: [1, 2],
    });
  });

  it('clears availableMethods when provided empty array', async () => {
    const qb = makeQueryBuilder({
      find: jest.fn().mockResolvedValue({ data: [] }),
    });
    const svc = makeService(qb);

    await (svc as any).updateRelations('route_definition', 5, {
      availableMethods: [],
    });

    expect(qb.update).toHaveBeenCalledWith('route_definition', 5, {
      availableMethods: [],
    });
  });

  it('does nothing for non-route_definition tables', async () => {
    const qb = makeQueryBuilder();
    const svc = makeService(qb);

    await (svc as any).updateRelations('user_definition', 1, {
      publishedMethods: [],
    });

    expect(qb.update).not.toHaveBeenCalled();
  });

  it('handles both publishedMethods and availableMethods in one call', async () => {
    const qb = makeQueryBuilder({
      find: jest
        .fn()
        .mockResolvedValueOnce({ data: [] })
        .mockResolvedValueOnce({ data: [{ id: 3 }] }),
    });
    const svc = makeService(qb);

    await (svc as any).updateRelations('route_definition', 7, {
      publishedMethods: [],
      availableMethods: ['POST'],
    });

    expect(qb.update).toHaveBeenCalledTimes(2);
    expect(qb.update).toHaveBeenCalledWith('route_definition', 7, {
      publishedMethods: [],
    });
    expect(qb.update).toHaveBeenCalledWith('route_definition', 7, {
      availableMethods: [3],
    });
  });
});

describe('DataMigrationService.migrateTable — end-to-end for publishedMethods clear', () => {
  beforeEach(() => {
    DatabaseConfigService.overrideForTesting('mysql');
  });

  afterEach(() => {
    DatabaseConfigService.resetForTesting();
  });

  it('clears publishedMethods on existing route when empty array specified', async () => {
    const qb = makeQueryBuilder({
      find: jest
        .fn()
        .mockResolvedValueOnce({ data: [{ id: 42 }] })
        .mockResolvedValueOnce({ data: [] }),
    });
    const svc = makeService(qb);

    await (svc as any).migrateTable('route_definition', [
      { _unique: { path: { _eq: '/metadata' } }, publishedMethods: [] },
    ]);

    expect(qb.update).toHaveBeenCalledWith(
      'route_definition',
      { where: [{ field: 'id', operator: '=', value: 42 }] },
      {},
    );
    expect(qb.update).toHaveBeenCalledWith('route_definition', 42, {
      publishedMethods: [],
    });
  });

  it('skips record not found in DB', async () => {
    const qb = makeQueryBuilder({
      find: jest.fn().mockResolvedValue({ data: [] }),
    });
    const svc = makeService(qb);

    await (svc as any).migrateTable('route_definition', [
      { _unique: { path: { _eq: '/nonexistent' } }, publishedMethods: [] },
    ]);

    expect(qb.update).not.toHaveBeenCalled();
  });

  it('does not call updateRelations when no relation fields in record', async () => {
    const qb = makeQueryBuilder({
      find: jest.fn().mockResolvedValue({ data: [{ id: 1 }] }),
    });
    const svc = makeService(qb);

    await (svc as any).migrateTable('route_definition', [
      { _unique: { path: { _eq: '/me' } }, isEnabled: true },
    ]);

    expect(qb.update).toHaveBeenCalledTimes(1);
  });
});
