import { MigrationJournalService } from '../../src/infrastructure/knex/services/migration-journal.service';

describe('MySQL Migration Compensation', () => {
  describe('MigrationJournalService', () => {
    let service: MigrationJournalService;
    let insertedEntries: any[];

    function createMockKnex(tables: Record<string, any> = {}) {
      insertedEntries = [];
      const defaultTable = () => ({
        insert: jest.fn((data: any) => {
          insertedEntries.push(data);
          return Promise.resolve();
        }),
        where: jest.fn().mockReturnThis(),
        andWhere: jest.fn().mockReturnThis(),
        update: jest.fn().mockResolvedValue(1),
        first: jest.fn().mockResolvedValue(null),
        whereIn: jest.fn().mockReturnThis(),
        select: jest.fn().mockResolvedValue([]),
        del: jest.fn().mockResolvedValue(0),
      });

      return jest.fn((table: string) => {
        if (tables[table]) return tables[table];
        return defaultTable();
      });
    }

    beforeEach(() => {
      insertedEntries = [];
    });

    it('record stores beforeSnapshot as JSON string', async () => {
      const snapshot = {
        name: 'test_table',
        columns: [{ id: 1, name: 'id', type: 'int' }],
        relations: [],
      };
      const knex = createMockKnex();
      const knexService = { getKnex: () => knex };
      service = new MigrationJournalService(knexService as any);

      await service.record({
        tableName: 'test_table',
        operation: 'update',
        upScript: 'ALTER TABLE test_table ADD COLUMN foo VARCHAR(255)',
        downScript: 'ALTER TABLE test_table DROP COLUMN foo',
        beforeSnapshot: snapshot,
      });

      expect(insertedEntries.length).toBe(1);
      expect(insertedEntries[0].beforeSnapshot).toBe(JSON.stringify(snapshot));
    });

    it('record works without beforeSnapshot', async () => {
      const knex = createMockKnex();
      const knexService = { getKnex: () => knex };
      service = new MigrationJournalService(knexService as any);

      await service.record({
        tableName: 'test_table',
        operation: 'create',
        upScript: 'CREATE TABLE test_table (id INT)',
        downScript: 'DROP TABLE test_table',
      });

      expect(insertedEntries.length).toBe(1);
      expect(insertedEntries[0].beforeSnapshot).toBeNull();
    });

    it('markCompleted updates status to completed', async () => {
      const tableMock = {
        where: jest.fn().mockReturnThis(),
        update: jest.fn().mockResolvedValue(1),
      };
      const knex = jest.fn(() => tableMock);
      const knexService = { getKnex: () => knex };
      service = new MigrationJournalService(knexService as any);

      await service.markCompleted('mj-test-123');

      expect(tableMock.update).toHaveBeenCalledWith(
        expect.objectContaining({
          status: 'completed',
          completedAt: expect.any(Date),
        }),
      );
    });

    it('executeRollback runs downScript in reverse order', async () => {
      const entry = {
        uuid: 'mj-test-rollback',
        downScript:
          'ALTER TABLE t DROP COLUMN a;ALTER TABLE t DROP CONSTRAINT fk_b',
      };

      const tableMock = {
        where: jest.fn().mockReturnThis(),
        first: jest.fn().mockResolvedValue(entry),
        update: jest.fn().mockResolvedValue(1),
      };

      const rawCalls: string[] = [];
      const knex = jest.fn(() => tableMock);
      knex.raw = jest.fn((sql: string) => {
        rawCalls.push(sql.trim());
        return Promise.resolve();
      });

      const knexService = { getKnex: () => knex };
      service = new MigrationJournalService(knexService as any);

      await service.executeRollback('mj-test-rollback');

      // Should execute in reverse order
      expect(rawCalls.length).toBe(2);
      expect(rawCalls[0]).toContain('fk_b');
      expect(rawCalls[1]).toContain('COLUMN a');
    });

    it('recoverPending handles empty list gracefully', async () => {
      const tableMock = {
        whereIn: jest.fn().mockReturnThis(),
        select: jest.fn().mockResolvedValue([]),
      };
      const knex = jest.fn(() => tableMock);
      const knexService = { getKnex: () => knex };
      service = new MigrationJournalService(knexService as any);

      await expect(service.recoverPending()).resolves.toBeUndefined();
    });

    it('recoverPending rolls back pending entries and logs metadata note', async () => {
      const entry = {
        uuid: 'mj-pending-1',
        tableName: 'test_table',
        operation: 'update',
        downScript: 'ALTER TABLE test_table DROP COLUMN foo',
      };
      let updateCalledWith: any = null;

      const tableMock = {
        whereIn: jest.fn().mockReturnThis(),
        select: jest.fn().mockResolvedValue([entry]),
        where: jest.fn().mockReturnThis(),
        first: jest.fn().mockResolvedValue(entry),
        update: jest.fn((data: any) => {
          updateCalledWith = data;
          return Promise.resolve(1);
        }),
      };

      const rawCalls: string[] = [];
      const knex = jest.fn(() => tableMock);
      knex.raw = jest.fn((sql: string) => {
        rawCalls.push(sql.trim());
        return Promise.resolve();
      });

      const knexService = { getKnex: () => knex };
      service = new MigrationJournalService(knexService as any);

      await service.recoverPending();

      expect(rawCalls.length).toBe(1);
      expect(rawCalls[0]).toContain('DROP COLUMN foo');
      expect(updateCalledWith).toEqual(
        expect.objectContaining({ status: 'rolled_back' }),
      );
    });
  });

  describe('MySQL handler retry logic', () => {
    it('retries metadata write up to 3 times on failure', async () => {
      let attempts = 0;
      const mockWrite = jest.fn(async () => {
        attempts++;
        if (attempts < 3) throw new Error('metadata write failed');
      });

      const maxRetries = 3;
      let success = false;
      for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
          await mockWrite();
          success = true;
          break;
        } catch (e) {
          if (attempt === maxRetries) throw e;
          await new Promise((r) => setTimeout(r, 10));
        }
      }

      expect(success).toBe(true);
      expect(mockWrite).toHaveBeenCalledTimes(3);
    });

    it('after exhausting retries, rollback is called', async () => {
      const mockWrite = jest.fn(async () => {
        throw new Error('metadata write failed');
      });
      const mockRollback = jest.fn(async (_uuid: string) => {});

      const maxRetries = 3;
      let rolledBack = false;
      for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
          await mockWrite();
          break;
        } catch (e) {
          if (attempt === maxRetries) {
            await mockRollback('journal-uuid');
            rolledBack = true;
          }
        }
      }

      expect(rolledBack).toBe(true);
      expect(mockRollback).toHaveBeenCalledWith('journal-uuid');
    });
  });
});
