import { describe, expect, it, vi } from 'vitest';
import { MySqlBootstrapSnapshotService } from '../../src/engines/bootstrap/services/mysql-bootstrap-snapshot.service';

function fixture(tables: string[], incoming: string[] = []) {
  const service = new MySqlBootstrapSnapshotService({
    knexService: {},
  } as never);
  const internals = service as unknown as {
    listApplicationTables(): Promise<string[]>;
    resolveRuntimeScopedTables(
      db: unknown,
      contract: unknown,
    ): Promise<string[]>;
  };
  vi.spyOn(internals, 'listApplicationTables').mockResolvedValue(tables);
  const db = {
    raw: vi
      .fn()
      .mockResolvedValue([incoming.map((tableName) => ({ tableName }))]),
  };
  return { internals, db };
}

describe('MySQL runtime snapshot scope', () => {
  it('captures the business source and target instead of relying on reverse DDL', async () => {
    const { internals, db } = fixture([
      'enfyra_table',
      'enfyra_column',
      'orders',
      'renamed_orders',
      'unrelated',
    ]);
    const result = await internals.resolveRuntimeScopedTables(db, {
      context: {
        operation: 'update',
        source: { name: 'orders' },
        target: { name: 'renamed_orders' },
        diff: { policyMetadataChanged: false },
      },
    });
    expect(result).toEqual([
      'enfyra_column',
      'enfyra_table',
      'orders',
      'renamed_orders',
    ]);
    expect(result).not.toContain('unrelated');
  });

  it('captures retained and removed junctions plus incoming foreign-key tables', async () => {
    const { internals, db } = fixture(
      ['orders', 'children', 'tags', 'j_old', 'j_new', 'unrelated'],
      ['children'],
    );
    const result = await internals.resolveRuntimeScopedTables(db, {
      context: {
        operation: 'update',
        source: {
          name: 'orders',
          relations: [{ junctionTableName: 'j_old', targetTableName: 'tags' }],
        },
        target: {
          name: 'orders',
          relations: [{ junctionTableName: 'j_new', targetTableName: 'tags' }],
        },
        diff: { policyMetadataChanged: false },
      },
    });
    expect(result).toEqual(['children', 'j_new', 'j_old', 'orders', 'tags']);
  });

  it('fails before mutation when dependency catalog inspection fails', async () => {
    const { internals, db } = fixture(['orders']);
    db.raw.mockRejectedValue(new Error('foreign-key catalog unavailable'));
    await expect(
      internals.resolveRuntimeScopedTables(db, {
        context: {
          operation: 'delete',
          source: { name: 'orders' },
          target: null,
          diff: { policyMetadataChanged: false },
        },
      }),
    ).rejects.toThrow('foreign-key catalog unavailable');
  });
});
