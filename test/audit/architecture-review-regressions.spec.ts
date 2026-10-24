import { describe, expect, it, vi } from 'vitest';
import type { Knex } from 'knex';
import { MySqlBootstrapSnapshotService } from '../../src/engines/bootstrap/services/mysql-bootstrap-snapshot.service';

describe('architecture audit: durable MySQL restore contracts', () => {
  it('captures writable expression-default columns while excluding computed columns', async () => {
    const db = {
      raw: vi.fn(async () => [
        [
          { columnName: 'id', extra: 'auto_increment' },
          { columnName: 'createdAt', extra: 'DEFAULT_GENERATED' },
          {
            columnName: 'updatedAt',
            extra: 'DEFAULT_GENERATED on update CURRENT_TIMESTAMP',
          },
          { columnName: 'total', extra: 'STORED GENERATED' },
          { columnName: 'display', extra: 'VIRTUAL GENERATED' },
        ],
      ]),
    };
    const service = new MySqlBootstrapSnapshotService({
      knexService: {},
    } as never);
    const boundary = service as unknown as {
      readWritableColumns(db: unknown, tableName: string): Promise<string[]>;
    };
    expect(await boundary.readWritableColumns(db, 'audit_orders')).toEqual([
      'id',
      'createdAt',
      'updatedAt',
    ]);
  });

  it('fails before dropping live tables when a required backup is missing', async () => {
    const entry = {
      txId: 'audit-restore',
      tableName: 'audit_orders',
      backupTableName: 'audit_missing_backup',
      createSql: 'CREATE TABLE audit_orders (id int)',
      columnsJson: '["id"]',
      ordinal: 0,
    };
    const statements: string[] = [];
    const connection = {
      escapeId: (value: string) => '`' + value + '`',
      promise: () => ({
        query: vi.fn(async (sql: string) => {
          statements.push(sql);
          return [[]];
        }),
      }),
    };
    const query = {
      where: vi.fn(() => query),
      orderBy: vi.fn(async () => [entry]),
      update: vi.fn(async () => 1),
    };
    const db = vi.fn(() => query) as unknown as Knex;
    const service = new MySqlBootstrapSnapshotService({
      knexService: {},
    } as never);
    const boundary = service as unknown as {
      restore(db: Knex, txId: string, connection: unknown): Promise<void>;
      listApplicationTablesOnConnection(connection: unknown): Promise<string[]>;
      cleanup(db: Knex, txId: string): Promise<void>;
    };
    vi.spyOn(boundary, 'listApplicationTablesOnConnection').mockResolvedValue([
      'audit_orders',
    ]);
    vi.spyOn(boundary, 'cleanup').mockResolvedValue(undefined);
    const error = await boundary.restore(db, entry.txId, connection).then(
      () => null,
      (value: unknown) => value,
    );
    expect({
      rejected: error instanceof Error,
      destructiveStatements: statements.filter((sql) =>
        /^DROP TABLE/.test(sql),
      ),
    }).toEqual({
      rejected: true,
      destructiveStatements: [],
    });
  });

  it('releases an acquired connection when restore preflight fails', async () => {
    const query = {
      where: vi.fn(() => query),
      orderBy: vi.fn(async () => []),
    };
    const connection = {
      escapeId: (value: string) => '`' + value + '`',
      promise: () => ({ query: vi.fn(async () => [[]]) }),
    };
    const releaseConnection = vi.fn(async () => undefined);
    const db = Object.assign(
      vi.fn(() => query),
      {
        client: {
          acquireConnection: vi.fn(async () => connection),
          releaseConnection,
        },
      },
    ) as unknown as Knex;
    const service = new MySqlBootstrapSnapshotService({
      knexService: {},
    } as never);
    const boundary = service as unknown as {
      restore(db: Knex, txId: string): Promise<void>;
    };

    await expect(boundary.restore(db, 'audit-preflight')).rejects.toThrow(
      'has no complete table-count attestation',
    );
    expect(releaseConnection).toHaveBeenCalledOnce();
  });

  it('fails closed if a backup disappears after restore preflight', async () => {
    const entry = {
      txId: 'audit-raced-restore',
      tableName: 'audit_orders',
      backupTableName: 'audit_backup',
      createSql: 'CREATE TABLE audit_orders (id int)',
      columnsJson: '["id"]',
      ordinal: 0,
    };
    const statements: string[] = [];
    const connection = {
      escapeId: (value: string) => '`' + value + '`',
      promise: () => ({
        query: vi.fn(async (sql: string) => {
          statements.push(sql);
          return [[]];
        }),
      }),
    };
    const query = {
      where: vi.fn(() => query),
      orderBy: vi.fn(async () => [entry]),
    };
    const db = vi.fn(() => query) as unknown as Knex;
    const service = new MySqlBootstrapSnapshotService({
      knexService: {},
    } as never);
    const boundary = service as unknown as {
      restore(db: Knex, txId: string, connection: unknown): Promise<void>;
      assertSnapshotRestorable(): Promise<void>;
      listApplicationTablesOnConnection(): Promise<string[]>;
    };
    vi.spyOn(boundary, 'assertSnapshotRestorable').mockResolvedValue(undefined);
    vi.spyOn(boundary, 'listApplicationTablesOnConnection').mockResolvedValue([
      'audit_orders',
    ]);

    await expect(boundary.restore(db, entry.txId, connection)).rejects.toThrow(
      "lost backup 'audit_backup' during restore",
    );
    expect(statements).toContain('SET FOREIGN_KEY_CHECKS = 1');
  });
});
