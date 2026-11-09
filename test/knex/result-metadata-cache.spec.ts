import { describe, expect, it, vi } from 'vitest';
import type { Knex } from 'knex';
import { KnexService } from '../../src/engines/knex/knex.service';

function createService() {
  const metadata = {
    parent: {
      columns: [{ name: 'payload', type: 'simple-json' }],
      relations: [{ propertyName: 'owner', targetTableName: 'owner' }],
    },
    owner: {
      columns: [{ name: 'preferences', type: 'simple-json' }],
      relations: [],
    },
  };
  const getTableMetadata = vi.fn(
    async (name: string) => metadata[name as keyof typeof metadata] ?? null,
  );
  const getActiveMetadata = vi.fn((_name: string): unknown => null);
  const connection = vi.fn(() => {
    throw new Error('Unexpected metadata SQL fallback');
  });
  const service = new KnexService({
    envService: { get: () => undefined },
    databaseConfigService: { getDbType: () => 'postgres' },
    knexHookManagerService: {},
    lazyRef: {
      runtimeRegistryService: { getTableMetadata: getActiveMetadata },
      metadataCacheService: { getTableMetadata },
    },
  } as unknown as ConstructorParameters<typeof KnexService>[0]);
  Object.assign(service, { knexInstance: connection });
  return { service, metadata, getTableMetadata, getActiveMetadata, connection };
}

const row = {
  id: 1,
  payload: '{"ok":true}',
  owner: { id: 2, preferences: '{"theme":"dark"}' },
};
const decoded = {
  id: 1,
  payload: { ok: true },
  owner: { id: 2, preferences: { theme: 'dark' } },
};

describe('SQL result metadata cache', () => {
  it('awaits staged metadata before activation and decodes root and nested JSON without SQL', async () => {
    const { service, connection } = createService();
    await expect(service.parseResult(row, 'parent', false)).resolves.toEqual(
      decoded,
    );
    expect(connection).not.toHaveBeenCalled();
    expect(row.payload).toBe('{"ok":true}');
  });

  it('shares in-flight staged metadata reads across repeated relation rows', async () => {
    const { service, connection, getTableMetadata } = createService();
    const rows = Array.from({ length: 64 }, () => row);
    await expect(service.parseResult(rows, 'parent', false)).resolves.toEqual(
      rows.map(() => decoded),
    );
    expect(connection).not.toHaveBeenCalled();
    expect(
      getTableMetadata.mock.calls.filter(([name]) => name === 'owner'),
    ).toHaveLength(1);
    expect(
      getTableMetadata.mock.calls.filter(([name]) => name === 'parent'),
    ).toHaveLength(2);
  });

  it('prefers activated metadata without reading the staged builder', async () => {
    const {
      service,
      metadata,
      getActiveMetadata,
      getTableMetadata,
      connection,
    } = createService();
    getActiveMetadata.mockImplementation(
      (name) => metadata[name as keyof typeof metadata],
    );
    await expect(service.parseResult(row, 'parent', false)).resolves.toEqual(
      decoded,
    );
    expect(getTableMetadata).not.toHaveBeenCalled();
    expect(connection).not.toHaveBeenCalled();
  });

  it('reads current metadata after activation instead of retaining a staged relation target', async () => {
    const { service, metadata, getActiveMetadata, connection } =
      createService();
    await service.parseResult(row, 'parent', false);
    getActiveMetadata.mockImplementation((name) =>
      name === 'parent'
        ? {
            ...metadata.parent,
            relations: [
              { propertyName: 'owner', targetTableName: 'new_owner' },
            ],
          }
        : {
            columns: [{ name: 'preferences', type: 'simple-json' }],
            relations: [],
          },
    );
    await expect(service.parseResult(row, 'parent', false)).resolves.toEqual(
      decoded,
    );
    expect(getActiveMetadata).toHaveBeenCalledWith('new_owner');
    expect(connection).not.toHaveBeenCalled();
  });

  it.each(['columns', 'relation'])(
    'propagates staged %s lookup failure in a transaction and permits a later retry',
    async (lookup) => {
      const { service, getTableMetadata, connection } = createService();
      if (lookup === 'relation') {
        await service.parseResult(
          { id: 1, payload: row.payload },
          'parent',
          false,
        );
      }
      getTableMetadata.mockRejectedValueOnce(
        new Error('Staged metadata failed'),
      );
      const transaction = Object.assign(vi.fn(), {
        commit: vi.fn(),
        rollback: vi.fn(),
      }) as unknown as Knex.Transaction;
      await expect(
        service.runWithTransaction(transaction, () =>
          service.parseResult(row, 'parent', false),
        ),
      ).rejects.toThrow('Staged metadata failed');
      await expect(
        service.runWithTransaction(transaction, () =>
          service.parseResult(row, 'parent', false),
        ),
      ).resolves.toEqual(decoded);
      expect(connection).not.toHaveBeenCalled();
      expect(transaction).not.toHaveBeenCalled();
    },
  );

  it('falls back to transaction-owned SQL when staged metadata is unavailable', async () => {
    const { service, getTableMetadata, connection } = createService();
    getTableMetadata.mockResolvedValue(null);
    const inside = vi.fn((table: string) => ({
      where() {
        return this;
      },
      first: async () =>
        table === 'enfyra_relation'
          ? { targetTableId: 20 }
          : { id: 10, name: 'owner' },
      select: async () => [
        { name: 'payload', type: 'simple-json' },
        { name: 'preferences', type: 'simple-json' },
      ],
    }));
    const transaction = Object.assign(inside, {
      commit: vi.fn(),
      rollback: vi.fn(),
    }) as unknown as Knex.Transaction;
    await expect(
      service.runWithTransaction(transaction, () =>
        service.parseResult(row, 'parent', false),
      ),
    ).resolves.toEqual(decoded);
    expect(inside).toHaveBeenCalledWith('enfyra_table');
    expect(inside).toHaveBeenCalledWith('enfyra_column');
    expect(inside).toHaveBeenCalledWith('enfyra_relation');
    expect(connection).not.toHaveBeenCalled();
  });
});
