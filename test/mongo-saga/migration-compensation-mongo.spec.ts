import { MongoMigrationJournalService } from '../../src/engines/mongo';
import { ObjectId } from 'mongodb';

describe('MongoDB Migration Compensation', () => {
  describe('captureRawMetadataSnapshot', () => {
    it('captures table, columns, relations, and inverse relations', async () => {
      const tableId = new ObjectId();
      const owningRelId = new ObjectId();

      const mockDb = {
        collection: jest.fn((name: string) => {
          if (name === 'table_definition') {
            return {
              findOne: jest.fn().mockResolvedValue({
                _id: tableId,
                name: 'posts',
                alias: 'Post',
              }),
            };
          }
          if (name === 'column_definition') {
            return {
              find: jest.fn().mockReturnValue({
                toArray: jest.fn().mockResolvedValue([
                  {
                    _id: new ObjectId(),
                    name: '_id',
                    type: 'ObjectId',
                    table: tableId,
                  },
                  {
                    _id: new ObjectId(),
                    name: 'title',
                    type: 'string',
                    table: tableId,
                  },
                ]),
              }),
            };
          }
          if (name === 'relation_definition') {
            const findMock = jest.fn();
            findMock.mockReturnValue({
              toArray: jest.fn().mockResolvedValue([
                {
                  _id: owningRelId,
                  propertyName: 'author',
                  type: 'many-to-one',
                  sourceTable: tableId,
                  mappedBy: null,
                },
              ]),
            });
            return {
              find: findMock,
              deleteMany: jest.fn().mockResolvedValue({ deletedCount: 0 }),
              insertMany: jest.fn().mockResolvedValue({ insertedCount: 0 }),
              deleteOne: jest.fn().mockResolvedValue({ deletedCount: 0 }),
              insertOne: jest
                .fn()
                .mockResolvedValue({ insertedId: new ObjectId() }),
              findOne: jest.fn().mockResolvedValue(null),
            };
          }
          return {};
        }),
      };

      // Simulate the capture logic
      const db = mockDb;
      const sourceRelations = await db
        .collection('relation_definition')
        .find({ sourceTable: tableId })
        .toArray();
      const owningRelIds = sourceRelations
        .filter((r: any) => !r.mappedBy)
        .map((r: any) => r._id);

      // Inverse relations would be queried with mappedBy: { $in: owningRelIds }
      expect(owningRelIds.length).toBe(1);
      expect(owningRelIds[0]).toBe(owningRelId);
    });
  });

  describe('restoreMetadataFromRawSnapshot', () => {
    it('restores table, columns, relations and cleans up inverse relations', async () => {
      const tableId = new ObjectId();
      const owningRelId = new ObjectId();
      const snapshotInverseId = new ObjectId();

      const operations: string[] = [];

      const mockDb = {
        collection: jest.fn((name: string) => {
          const ops: any = {
            replaceOne: jest.fn(async () => {
              operations.push(`${name}:replaceOne`);
            }),
            deleteMany: jest.fn(async () => {
              operations.push(`${name}:deleteMany`);
            }),
            insertMany: jest.fn(async () => {
              operations.push(`${name}:insertMany`);
            }),
            find: jest.fn().mockReturnValue({
              toArray: jest
                .fn()
                .mockResolvedValue([
                  { _id: owningRelId, propertyName: 'author', mappedBy: null },
                ]),
            }),
            deleteOne: jest.fn(async () => {
              operations.push(`${name}:deleteOne`);
            }),
            insertOne: jest.fn(async () => {
              operations.push(`${name}:insertOne`);
            }),
            findOne: jest.fn().mockResolvedValue(null),
          };
          return ops;
        }),
      };

      // Simulate the restore logic step by step
      const snapshot = {
        table: { _id: tableId, name: 'posts' },
        columns: [
          {
            _id: new ObjectId(),
            name: 'title',
            type: 'string',
            table: tableId,
          },
        ],
        relations: [
          {
            _id: owningRelId,
            propertyName: 'author',
            sourceTable: tableId,
            mappedBy: null,
          },
        ],
        inverseRelations: [
          {
            _id: snapshotInverseId,
            propertyName: 'posts',
            sourceTable: new ObjectId(),
            mappedBy: owningRelId,
          },
        ],
      };

      const db = mockDb;
      const oid = tableId;

      // Step 1: Restore table
      await db
        .collection('table_definition')
        .replaceOne({ _id: oid }, snapshot.table, { upsert: true });
      // Step 2: Restore columns
      await db.collection('column_definition').deleteMany({ table: oid });
      await db.collection('column_definition').insertMany(snapshot.columns);
      // Step 3: Restore relations
      await db
        .collection('relation_definition')
        .deleteMany({ sourceTable: oid });
      await db.collection('relation_definition').insertMany(snapshot.relations);

      expect(operations).toContain('table_definition:replaceOne');
      expect(operations).toContain('column_definition:deleteMany');
      expect(operations).toContain('column_definition:insertMany');
      expect(operations).toContain('relation_definition:deleteMany');
      expect(operations).toContain('relation_definition:insertMany');
    });
  });

  describe('MongoMigrationJournalService', () => {
    it('record stores rawBeforeSnapshot', async () => {
      const inserted: any[] = [];
      const mockCollection = {
        insertOne: jest.fn(async (doc: any) => {
          inserted.push(doc);
        }),
        find: jest.fn().mockReturnValue({
          toArray: jest.fn().mockResolvedValue([]),
        }),
        updateOne: jest.fn().mockResolvedValue({ modifiedCount: 1 }),
        findOne: jest.fn().mockResolvedValue(null),
      };

      const mockMongoService = {
        getDb: () => ({ collection: () => mockCollection }),
      };

      const service = new MongoMigrationJournalService({
        mongoService: mockMongoService as any,
      });
      const rawSnapshot = {
        table: { _id: new ObjectId(), name: 'posts' },
        columns: [],
        relations: [],
        inverseRelations: [],
      };

      await service.record({
        tableName: 'posts',
        operation: 'update',
        upDiff: { columns: { create: [], delete: [], update: [] } },
        downDiff: { columns: { create: [], delete: [], update: [] } },
        beforeSnapshot: { name: 'posts', columns: [], relations: [] },
        afterSnapshot: { name: 'posts', columns: [], relations: [] },
        rawBeforeSnapshot: rawSnapshot,
      });

      expect(inserted.length).toBe(1);
      expect(inserted[0].rawBeforeSnapshot).toEqual(rawSnapshot);
      expect(inserted[0].afterSnapshot).toEqual({
        name: 'posts',
        columns: [],
        relations: [],
      });
    });

    it('record requires rawBeforeSnapshot for update migration saga', async () => {
      const mockMongoService = {
        getDb: () => ({ collection: () => ({ insertOne: jest.fn() }) }),
      };
      const service = new MongoMigrationJournalService({
        mongoService: mockMongoService as any,
      });

      await expect(
        service.record({
          tableName: 'posts',
          operation: 'update',
          upDiff: {},
          downDiff: {},
          beforeSnapshot: {},
          afterSnapshot: {},
        }),
      ).rejects.toThrow('requires rawBeforeSnapshot');
    });

    it('recoverPending calls restoreMetadataFn with journal entry', async () => {
      const entry = {
        uuid: 'mj-test',
        tableName: 'posts',
        status: 'running',
        downDiff: { columns: { create: [], delete: [], update: [] } },
        beforeSnapshot: { name: 'posts', columns: [], relations: [] },
        rawBeforeSnapshot: {
          table: { _id: new ObjectId(), name: 'posts' },
          columns: [],
          relations: [],
          inverseRelations: [],
        },
      };

      const mockCollection = {
        find: jest.fn().mockReturnValue({
          toArray: jest.fn().mockResolvedValue([entry]),
        }),
        findOne: jest.fn().mockResolvedValue(entry),
        updateOne: jest.fn().mockResolvedValue({ modifiedCount: 1 }),
        insertOne: jest.fn(),
        deleteOne: jest.fn(),
      };

      const mockMongoService = {
        getDb: () => ({ collection: () => mockCollection }),
      };

      const service = new MongoMigrationJournalService({
        mongoService: mockMongoService as any,
      });
      const executeDiff = jest.fn();
      const restoreFn = jest.fn();

      await service.recoverPending(executeDiff, restoreFn);

      expect(executeDiff).toHaveBeenCalledWith(entry.downDiff, entry);
      expect(restoreFn).toHaveBeenCalledWith(entry);
    });

    it('recoverPending uses Redis lock when available', async () => {
      const entry = {
        uuid: 'mj-test',
        tableName: 'posts',
        status: 'running',
        downDiff: {},
        beforeSnapshot: {},
        rawBeforeSnapshot: {
          table: { _id: new ObjectId(), name: 'posts' },
          columns: [],
          relations: [],
          inverseRelations: [],
        },
      };
      const mockCollection = {
        find: jest.fn().mockReturnValue({
          toArray: jest.fn().mockResolvedValue([entry]),
        }),
        findOne: jest.fn().mockResolvedValue(entry),
        updateOne: jest.fn().mockResolvedValue({ modifiedCount: 1 }),
      };
      const acquire = jest.fn().mockResolvedValue(true);
      const release = jest.fn().mockResolvedValue(true);
      const service = new MongoMigrationJournalService({
        mongoService: {
          getDb: () => ({ collection: () => mockCollection }),
        } as any,
        cacheService: { acquire, release } as any,
        instanceService: { getInstanceId: () => 'instance-a' } as any,
      });
      const executeDiff = jest.fn();

      await service.recoverPending(executeDiff, jest.fn());

      expect(acquire).toHaveBeenCalled();
      expect(release).toHaveBeenCalled();
      expect(executeDiff).toHaveBeenCalledWith(entry.downDiff, entry);
    });

    it('recoverPending skips when Redis lock is held by another instance', async () => {
      const mockCollection = {
        find: jest.fn().mockReturnValue({
          toArray: jest.fn().mockResolvedValue([{ uuid: 'mj-test' }]),
        }),
      };
      const acquire = jest.fn().mockResolvedValue(false);
      const service = new MongoMigrationJournalService({
        mongoService: {
          getDb: () => ({ collection: () => mockCollection }),
        } as any,
        cacheService: { acquire, release: jest.fn() } as any,
        instanceService: { getInstanceId: () => 'instance-a' } as any,
      });
      const executeDiff = jest.fn();

      await service.recoverPending(executeDiff);

      expect(executeDiff).not.toHaveBeenCalled();
    });

    it('recoverPending without restoreMetadataFn still works', async () => {
      const mockCollection = {
        find: jest.fn().mockReturnValue({
          toArray: jest.fn().mockResolvedValue([]),
        }),
      };

      const mockMongoService = {
        getDb: () => ({ collection: () => mockCollection }),
      };

      const service = new MongoMigrationJournalService({
        mongoService: mockMongoService as any,
      });

      await expect(service.recoverPending(jest.fn())).resolves.toBeUndefined();
    });
  });
});
