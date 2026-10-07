import { describe, expect, it, vi } from 'vitest';
import type { Knex } from 'knex';
import { KnexService } from '../../src/engines/knex/knex.service';

describe('relation metadata transaction scope', () => {
  it('uses the normalized targetTableName metadata without SQL fallback', async () => {
    const outside = vi.fn(() => {
      throw new Error('Unexpected metadata fallback');
    });
    const registry = {
      getTableMetadata: () => ({
        relations: [
          {
            propertyName: 'owner',
            targetTableName: 'owner',
            targetTableId: 20,
          },
        ],
      }),
    };
    const service = new KnexService({
      envService: { get: () => undefined },
      databaseConfigService: { getDbType: () => 'postgres' },
      knexHookManagerService: {},
      runtimeRegistryService: registry,
      lazyRef: { runtimeRegistryService: registry },
    } as unknown as ConstructorParameters<typeof KnexService>[0]);
    Object.assign(service, {
      knexInstance: outside,
      columnTypesMap: new Map([
        ['parent', new Map([['id', 'int']])],
        ['owner', new Map([['id', 'int']])],
      ]),
    });
    expect(
      await service.parseResult({ id: 1, owner: { id: 2 } }, 'parent', false),
    ).toEqual({ id: 1, owner: { id: 2 } });
    expect(outside).not.toHaveBeenCalled();
  });

  it('propagates metadata SQL failure inside a transaction', async () => {
    const outside = vi.fn();
    const transaction = Object.assign(
      vi.fn(() => ({
        where() {
          return this;
        },
        first: async () => {
          throw new Error('Metadata SQL failed');
        },
      })),
      { commit: vi.fn(), rollback: vi.fn() },
    ) as unknown as Knex.Transaction;
    const registry = { getTableMetadata: () => null };
    const service = new KnexService({
      envService: { get: () => undefined },
      databaseConfigService: { getDbType: () => 'postgres' },
      knexHookManagerService: {},
      runtimeRegistryService: registry,
      lazyRef: { runtimeRegistryService: registry },
    } as unknown as ConstructorParameters<typeof KnexService>[0]);
    Object.assign(service, {
      knexInstance: outside,
      columnTypesMap: new Map([['parent', new Map([['id', 'int']])]]),
    });
    await expect(
      service.runWithTransaction(transaction, () =>
        service.parseResult({ id: 1, owner: { id: 2 } }, 'parent', false),
      ),
    ).rejects.toThrow('Metadata SQL failed');
    expect(outside).not.toHaveBeenCalled();
  });

  it('resolves a relation fallback through the owning transaction without another pool connection', async () => {
    const outside = vi.fn(() => {
      throw new Error('Unexpected unscoped connection');
    });
    const inside = vi.fn((table: string) => {
      const conditions = new Map<string, unknown>();
      return {
        where(field: string, value: unknown) {
          conditions.set(field, value);
          return this;
        },
        async first() {
          if (table === 'enfyra_relation') return { targetTableId: 20 };
          return conditions.has('name')
            ? { id: 10, name: 'parent' }
            : { id: 20, name: 'owner' };
        },
      };
    });
    const transaction = Object.assign(inside, {
      commit: vi.fn(),
      rollback: vi.fn(),
    }) as unknown as Knex.Transaction;
    const registry = { getTableMetadata: () => null };
    const service = new KnexService({
      envService: { get: () => undefined },
      databaseConfigService: { getDbType: () => 'postgres' },
      knexHookManagerService: {},
      runtimeRegistryService: registry,
      lazyRef: { runtimeRegistryService: registry },
    } as unknown as ConstructorParameters<typeof KnexService>[0]);
    Object.assign(service, {
      knexInstance: outside,
      columnTypesMap: new Map([
        ['parent', new Map([['id', 'int']])],
        ['owner', new Map([['id', 'int']])],
      ]),
    });
    const parsed = await service.runWithTransaction(transaction, () =>
      service.parseResult({ id: 1, owner: { id: 2 } }, 'parent', false),
    );
    expect(parsed).toEqual({ id: 1, owner: { id: 2 } });
    expect(outside).not.toHaveBeenCalled();
    expect(inside.mock.calls.map(([table]) => table)).toEqual([
      'enfyra_table',
      'enfyra_relation',
      'enfyra_table',
    ]);
  });
});
