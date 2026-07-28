import { ObjectId } from 'mongodb';
import { MongoSagaSnapshotService } from '../../src/engines/mongo';

describe('MongoSagaSnapshotService snapshot rollback', () => {
  function snapshot(overrides: Record<string, any>) {
    const seq = overrides.seq ?? 1;
    const op = overrides.op ?? 'update';
    const snapshotId = overrides.snapshotId ?? `tx-test-${seq}`;
    return {
      sessionId: 'tx-test',
      snapshotId,
      seq,
      op,
      collection: 'posts',
      documentId: new ObjectId('65f000000000000000000001'),
      before: null,
      afterPatch: null,
      metadata: {},
      status: 'completed',
      createdAt: new Date(),
      ...overrides,
    };
  }

  function createService() {
    const posts = {
      bulkWrite: jest.fn().mockResolvedValue({}),
      deleteMany: jest.fn().mockResolvedValue({ deletedCount: 1 }),
    };
    const snapshots = {
      updateMany: jest.fn().mockResolvedValue({ modifiedCount: 1 }),
    };
    const mongoService = {
      getDb: () => ({
        collection: (name: string) => (name === 'posts' ? posts : snapshots),
      }),
      getRawDb: () => ({
        collection: (name: string) => (name === 'posts' ? posts : snapshots),
      }),
    };
    const service = new MongoSagaSnapshotService({
      mongoService: mongoService as any,
    });
    return { service, posts, snapshots };
  }

  it('restores the first before snapshot when one document is updated multiple times', async () => {
    const { service, posts } = createService();
    const id = new ObjectId('65f000000000000000000001');

    const result = await (service as any).rollbackBatch('tx-test', [
      snapshot({
        seq: 2,
        documentId: id,
        before: { _id: id, title: 'intermediate' },
        afterPatch: { title: 'final' },
      }),
      snapshot({
        seq: 1,
        documentId: id,
        before: { _id: id, title: 'original' },
        afterPatch: { title: 'intermediate' },
      }),
    ]);

    expect(result.success).toBe(true);
    expect(posts.bulkWrite).toHaveBeenCalledWith(
      [
        {
          replaceOne: {
            filter: { _id: id },
            replacement: { _id: id, title: 'original' },
            upsert: true,
          },
        },
      ],
      { ordered: false },
    );
    expect(posts.deleteMany).not.toHaveBeenCalled();
  });

  it('deletes a document when its first session snapshot is an insert', async () => {
    const { service, posts } = createService();
    const id = new ObjectId('65f000000000000000000001');

    const result = await (service as any).rollbackBatch('tx-test', [
      snapshot({
        seq: 3,
        op: 'delete',
        documentId: id,
        before: { _id: id, title: 'new' },
      }),
      snapshot({
        seq: 2,
        op: 'update',
        documentId: id,
        before: { _id: id, title: 'new' },
      }),
      snapshot({
        seq: 1,
        op: 'insert',
        documentId: id,
        before: null,
        afterPatch: { title: 'new' },
      }),
    ]);

    expect(result.success).toBe(true);
    expect(posts.deleteMany).toHaveBeenCalledWith({ _id: { $in: [id] } });
    expect(posts.bulkWrite).not.toHaveBeenCalled();
  });

  it('deletes replacement rows before restoring originals sharing a unique key', async () => {
    const { service, posts } = createService();
    const oldId = new ObjectId('65f000000000000000000001');
    const newId = new ObjectId('65f000000000000000000002');
    const callOrder: string[] = [];
    posts.deleteMany.mockImplementation(async () => {
      callOrder.push('delete');
      return { deletedCount: 1 };
    });
    posts.bulkWrite.mockImplementation(async () => {
      callOrder.push('restore');
      return {};
    });

    const result = await (service as any).rollbackBatch('tx-race', [
      snapshot({
        sessionId: 'tx-race',
        seq: 2,
        op: 'insert',
        snapshotId: 'tx-race-2',
        documentId: newId,
        before: null,
        afterPatch: { sourceId: 'src1', targetId: 'tgt1' },
      }),
      snapshot({
        sessionId: 'tx-race',
        seq: 1,
        op: 'delete',
        snapshotId: 'tx-race-1',
        documentId: oldId,
        before: { _id: oldId, sourceId: 'src1', targetId: 'tgt1' },
      }),
    ]);

    expect(result.success).toBe(true);
    expect(callOrder).toEqual(['delete', 'restore']);
    expect(posts.deleteMany).toHaveBeenCalledWith({
      _id: { $in: [newId] },
    });
    expect(posts.bulkWrite).toHaveBeenCalledWith(
      [
        {
          replaceOne: {
            filter: { _id: oldId },
            replacement: { _id: oldId, sourceId: 'src1', targetId: 'tgt1' },
            upsert: true,
          },
        },
      ],
      { ordered: false },
    );
  });
});
