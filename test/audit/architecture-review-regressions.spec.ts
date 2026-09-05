import { describe, expect, it, vi } from 'vitest';
import type { Knex } from 'knex';
import { MySqlBootstrapSnapshotService } from '../../src/engines/bootstrap/services/mysql-bootstrap-snapshot.service';
import { MongoSchemaHealingService } from '../../src/engines/bootstrap/services/schema-healing/mongo-schema-healing.service';

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

describe('architecture audit: Mongo junction conflict preflight', () => {
  it.each([false, true])(
    'preserves a legacy collection when old/new edge values conflict (conflict=%s)',
    async (conflicting) => {
      const row = {
        oldSource: 'user-A',
        sourceId: conflicting ? 'user-B' : 'user-A',
        oldTarget: 'role-A',
        targetId: 'role-A',
      };
      const cursor = () => {
        return {
          async *[Symbol.asyncIterator]() {
            yield row;
          },
        };
      };
      const drop = vi.fn(async () => true);
      const upsert = vi.fn(async () => ({ upsertedCount: 1 }));
      const source = {
        find: cursor,
        drop,
      };
      const target = {
        updateOne: upsert,
        updateMany: vi.fn(async () => ({ modifiedCount: 0 })),
        createIndex: vi.fn(async () => 'index'),
        find: () => ({
          async *[Symbol.asyncIterator]() {},
        }),
      };
      const db = {
        listCollections: () => ({ toArray: async () => [{}] }),
        collection: (name: string) =>
          name === 'audit_legacy' ? source : target,
      };
      const service = new MongoSchemaHealingService({
        log: () => undefined,
      } as never);
      const boundary = service as unknown as {
        ensureMongoJunctionCollection(
          db: unknown,
          input: unknown,
        ): Promise<void>;
      };
      const result = boundary.ensureMongoJunctionCollection(db, {
        oldJunctionTableName: 'audit_legacy',
        oldJunctionSourceColumn: 'oldSource',
        oldJunctionTargetColumn: 'oldTarget',
        junctionTableName: 'audit_canonical',
        junctionSourceColumn: 'sourceId',
        junctionTargetColumn: 'targetId',
      });
      if (conflicting) {
        const error = await result.then(
          () => null,
          (value: unknown) => value,
        );
        expect({
          rejected: error instanceof Error,
          dropped: drop.mock.calls.length,
          copied: upsert.mock.calls.length,
        }).toEqual({
          rejected: true,
          dropped: 0,
          copied: 0,
        });
      } else {
        await result;
        expect(upsert).toHaveBeenCalledOnce();
        expect(drop).toHaveBeenCalledOnce();
      }
    },
  );
});
