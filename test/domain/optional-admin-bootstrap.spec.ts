import type { Knex } from 'knex';
import type { Db } from 'mongodb';
import { afterEach, describe, expect, it, vi } from 'vitest';
import type { BcryptService } from '../../src/domain/auth';
import { UserDefinitionProcessor } from '../../src/domain/bootstrap/processors/user-definition.processor';
import type { IQueryBuilder } from '../../src/domain/shared/interfaces/query-builder.interface';

afterEach(() => {
  vi.unstubAllEnvs();
});

describe('optional initial administrator provisioning', () => {
  for (const backend of ['postgres', 'mysql', 'mariadb', 'mongodb']) {
    it.each(['omitted', 'empty', 'existing'] as const)(
      `${backend} skips admin writes for %s bootstrap credentials`,
      async (scenario) => {
        const existing = scenario === 'existing';
        vi.stubEnv('ADMIN_EMAIL', 'operator@localhost.test');
        vi.stubEnv(
          'ADMIN_PASSWORD',
          existing
            ? 'replacement-fixture'
            : scenario === 'empty'
              ? ''
              : undefined,
        );
        const hash = vi.fn();
        const insert = vi.fn();
        const update = vi.fn();
        const first = vi
          .fn()
          .mockResolvedValue(
            existing
              ? { email: 'existing@localhost.test', isRootAdmin: true }
              : null,
          );
        const processor = new UserDefinitionProcessor({
          bcryptService: { hash } as unknown as BcryptService,
          queryBuilderService: {} as IQueryBuilder,
        });
        let result;
        if (backend === 'mongodb') {
          const collection = {
            findOne: first,
            insertOne: insert,
            updateOne: update,
          };
          const db = {
            collection: vi.fn().mockReturnValue(collection),
          } as unknown as Db;
          result = await processor.processMongo([], db, 'enfyra_user');
          expect(first).toHaveBeenCalledWith({ isRootAdmin: true });
        } else {
          const table = { where: vi.fn(), first, insert, update };
          table.where.mockReturnValue(table);
          const knex = vi.fn().mockReturnValue(table) as unknown as Knex;
          result = await processor.processSql([], knex, 'enfyra_user');
          expect(table.where).toHaveBeenCalledWith({ isRootAdmin: true });
        }
        expect(result).toEqual({ created: 0, skipped: 0 });
        expect(first).toHaveBeenCalledTimes(1);
        expect(hash).not.toHaveBeenCalled();
        expect(insert).not.toHaveBeenCalled();
        expect(update).not.toHaveBeenCalled();
      },
    );
  }
});
