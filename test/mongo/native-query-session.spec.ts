import { describe, expect, it, vi } from 'vitest';
import { MongoService, NativeSessionCollection } from '../../src/engines/mongo/services/mongo.service';

function fixture() {
  const aggregate = vi.fn(() => ({ toArray: async () => [] }));
  const raw = { collection: () => ({ aggregate }) };
  const service = new MongoService({});
  const session = { withTransaction: async (work: () => unknown) => work(), endSession: async () => {} };
  Object.assign(service, { db: raw, client: { startSession: () => session }, nativeMultiDocSupported: true });
  return { service, session, raw, aggregate };
}

describe('native Mongo query session routing', () => {
  it('binds getDb collections to the active native transaction by default', async () => {
    const { service, session, raw, aggregate } = fixture();
    expect(service.getDb()).toBe(raw);
    await service.runInSaga(async () => {
      expect(service.getDb().collection('posts')).toBeInstanceOf(NativeSessionCollection);
      await service.getDb().collection('posts').aggregate([]).toArray();
    });
    expect(aggregate).toHaveBeenCalledWith([], expect.objectContaining({ session }));
    expect(service.getDb()).toBe(raw);
  });

  it('restores the query session on transaction scope re-entry', async () => {
    const { service, session, aggregate } = fixture();
    await service.runWithTransactionScope({ kind: 'native', bundle: { session: session as any, logicalTxId: 'synthetic' } }, async () => {
      await service.getDb().collection('posts').aggregate([]).toArray();
    });
    expect(aggregate).toHaveBeenCalledWith([], expect.objectContaining({ session }));
  });
});
