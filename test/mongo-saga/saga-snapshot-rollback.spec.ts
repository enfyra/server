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
});
