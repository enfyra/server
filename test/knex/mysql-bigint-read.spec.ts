import { beforeEach, describe, expect, it, vi } from 'vitest';

const captured = vi.hoisted(() => ({ configs: [] as any[] }));
vi.mock('knex', () => ({
  knex: vi.fn((config: any) => {
    captured.configs.push(config);
    return { raw: vi.fn().mockResolvedValue([]) };
  }),
}));
import { KnexService } from '../../src/engines/knex/knex.service';
import { ReplicationManager } from '../../src/engines/knex/services/replication-manager.service';

describe('MySQL BIGINT decoding', () => {
  beforeEach(() => {
    captured.configs.length = 0;
  });
  for (const path of ['direct', 'replication'] as const) {
    it(`${path} preserves the full decimal value and null without Number coercion`, async () => {
      const databaseConfigService = {
        getDbType: () => 'mysql',
        isMongoDb: () => false,
        isPostgres: () => false,
      };
      const envService = {
        get: (key: string) =>
          key === 'DB_URI'
            ? 'mysql://fixture:fixture@localhost:3306/fixture'
            : undefined,
      };
      if (path === 'direct') {
        const service = new KnexService({
          databaseConfigService,
          envService,
          knexHookManagerService: { initialize: vi.fn() },
          lazyRef: {},
        } as any);
        await service.init();
      } else {
        const service = new ReplicationManager({
          databaseConfigService,
          envService,
        } as any);
        (service as any).createKnexInstance(
          {
            host: 'localhost',
            port: 3306,
            user: 'fixture',
            password: 'fixture',
            database: 'fixture',
          },
          0,
          1,
        );
      }
      const decode = captured.configs[0].connection.typeCast;
      for (const value of [
        '9007199254740993',
        '-9007199254740993',
        '9223372036854775807',
        '18446744073709551615',
        '3',
        null,
      ]) {
        expect(
          decode({ type: 'LONGLONG', string: () => value }, () =>
            Number(value),
          ),
        ).toBe(value);
      }
      const next = vi.fn(() => 42);
      expect(decode({ type: 'LONG' }, next)).toBe(42);
      expect(next).toHaveBeenCalledOnce();
    });
  }
});
