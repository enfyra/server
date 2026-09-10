import { describe, expect, it, vi } from 'vitest';
import knex from 'knex';
import { ObjectId } from 'mongodb';
import { QueryBuilderService } from '@enfyra/kernel';
import { UserRevocationService } from '../../src/domain/auth/services/user-revocation.service';
import { TableRouteRouter } from '../../src/modules/dynamic-api/repositories/table-route.router';

describe('UserRevocationService session deletion scope', () => {
  it('publishes revocation when a full user form includes unchanged roles', async () => {
    const postUserRevocation = vi.fn(async () => undefined);
    const router = new TableRouteRouter({ postUserRevocation } as never);
    await router.getStrategy('enfyra_user').afterUpdateReload!({
      tableName: 'enfyra_user',
      id: 'other-user',
      existing: { name: 'Before', roles: [{ id: 'role-1' }] },
      body: { name: 'After', roles: [{ id: 'role-1' }] },
    });
    expect(postUserRevocation).toHaveBeenCalledWith('other-user');
  });

  it.each(['pg', 'mysql2'])(
    'keeps other users sessions on %s',
    async (client) => {
      const statements: Array<{ sql: string; bindings: unknown[] }> = [];
      const sql = knex({ client });
      sql.client.acquireConnection = vi.fn(async () => ({}));
      sql.client.releaseConnection = vi.fn(async () => undefined);
      sql.client._query = vi.fn(async (_connection, query) => {
        statements.push({ sql: query.sql, bindings: query.bindings });
        query.response =
          client === 'pg'
            ? { command: 'DELETE', rowCount: 1 }
            : [{ affectedRows: 1 }, []];
        return query;
      });

      const queryBuilderService = new QueryBuilderService({
        knexService: { getKnex: () => sql },
        databaseConfigService: {
          getDbType: () => (client === 'pg' ? 'postgres' : 'mysql'),
        },
        lazyRef: {},
      });
      let handler: ((channel: string, message: string) => void) | undefined;
      const service = new UserRevocationService({
        queryBuilderService,
        cacheService: {} as never,
        redisPubSubService: {
          subscribeWithHandler: (
            _channel: string,
            callback: typeof handler,
          ) => {
            handler = callback;
          },
        } as never,
      });

      try {
        await service.init();
        handler!('user:revoked', JSON.stringify({ userId: 'other-user' }));
        await vi.waitFor(() => expect(statements).toHaveLength(1));
        expect(statements[0].sql.toLowerCase()).toContain('where');
        expect(statements[0].bindings).toContain('other-user');
      } finally {
        await sql.destroy();
      }
    },
  );

  it('deletes only the target users sessions on MongoDB', async () => {
    const target = new ObjectId();
    const admin = new ObjectId();
    const sessions = new Map([
      ['target-session', { _id: 'target-session', user: target }],
      ['admin-session', { _id: 'admin-session', user: admin }],
    ]);
    const find = vi.fn((filter: { user?: ObjectId }) => ({
      toArray: async () =>
        [...sessions.values()].filter((row) =>
          filter.user ? row.user.equals(filter.user) : true,
        ),
    }));
    const deleteOne = vi.fn(async (_table: string, id: string) =>
      sessions.delete(id),
    );
    const queryBuilderService = new QueryBuilderService({
      mongoService: { collection: () => ({ find }), deleteOne },
      databaseConfigService: { getDbType: () => 'mongodb' },
      lazyRef: {},
    });
    let handler: ((channel: string, message: string) => void) | undefined;
    const service = new UserRevocationService({
      queryBuilderService,
      cacheService: {} as never,
      redisPubSubService: {
        subscribeWithHandler: (_channel: string, callback: typeof handler) => {
          handler = callback;
        },
      } as never,
    });
    await service.init();
    handler!('user:revoked', JSON.stringify({ userId: target.toHexString() }));
    await vi.waitFor(() => expect(deleteOne).toHaveBeenCalledTimes(1));
    expect(find).toHaveBeenCalledWith({ user: target }, {});
    expect([...sessions.keys()]).toEqual(['admin-session']);
  });

  it.each(['postgres', 'mysql', 'mongodb'])(
    'rejects object delete predicates before database IO on %s',
    async (dbType) => {
      const write = vi.fn(async () => 1);
      const getKnex = vi.fn(() => () => ({ delete: write }));
      const collection = vi.fn();
      const queryBuilder = new QueryBuilderService({
        knexService: { getKnex },
        mongoService: { collection },
        databaseConfigService: { getDbType: () => dbType },
        lazyRef: {},
      });
      const where = { userId: 'other-user' };
      await expect(
        queryBuilder.delete('enfyra_session', { where }),
      ).rejects.toThrow('Delete conditions must be an array');
      await expect(
        queryBuilder.deleteWithOptions({
          table: 'enfyra_session',
          where: where as never,
        }),
      ).rejects.toThrow('Delete conditions must be an array');
      expect(getKnex).not.toHaveBeenCalled();
      expect(collection).not.toHaveBeenCalled();
      expect(write).not.toHaveBeenCalled();
    },
  );
});
