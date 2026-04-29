import { ObjectId } from 'mongodb';
import {
  SagaCollection,
  MongoService,
} from '../../src/engines/mongo';

describe('SagaCollection (app-level saga)', () => {
  const rawColl = {
    countDocuments: jest.fn().mockResolvedValue(5),
    aggregate: jest.fn().mockReturnValue({ toArray: async () => [] }),
    bulkWrite: jest.fn().mockResolvedValue({}),
    find: jest.fn().mockReturnValue({
      skip: () => ({ limit: () => ({ toArray: async () => [] }) }),
    }),
    findOne: jest.fn().mockResolvedValue(null),
    insertMany: jest.fn(),
    updateOne: jest.fn(),
    deleteMany: jest.fn(),
  };

  const txApi = {
    txId: 'tx-test',
    assertWithinMaxDuration: jest.fn(),
    countDocuments: jest.fn().mockResolvedValue(5),
    aggregate: jest.fn().mockReturnValue({
      toArray: jest.fn().mockResolvedValue([]),
    }),
    find: jest.fn().mockResolvedValue([]),
    findOne: jest.fn().mockResolvedValue(null),
    insertOne: jest.fn(),
    insertMany: jest.fn(),
    updateOne: jest.fn(),
    updateOneByFilter: jest.fn(),
    updateManyByFilter: jest.fn(),
    deleteOne: jest.fn(),
    deleteMany: jest.fn(),
  };

  const mongo = {
    getDb: jest.fn(() => ({
      collection: jest.fn(() => rawColl),
    })),
    getActiveSagaSession: jest.fn(() => txApi),
  } as unknown as MongoService;

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('countDocuments delegates to saga session (visibility merge in session impl)', async () => {
    const c = new SagaCollection('items', mongo);
    const n = await c.countDocuments({ a: 1 });
    expect(n).toBe(5);
    expect(txApi.countDocuments).toHaveBeenCalledWith('items', { a: 1 });
  });

  it('aggregate delegates to saga session', () => {
    const c = new SagaCollection('items', mongo);
    c.aggregate([{ $match: {} }]);
    expect(txApi.aggregate).toHaveBeenCalledWith(
      'items',
      [{ $match: {} }],
      undefined,
    );
  });

  it('bulkWrite asserts duration then uses raw collection', () => {
    const c = new SagaCollection('items', mongo);
    c.bulkWrite([]);
    expect(txApi.assertWithinMaxDuration).toHaveBeenCalled();
    expect(mongo.getDb).toHaveBeenCalled();
    expect(rawColl.bulkWrite).toHaveBeenCalledWith([], undefined);
  });

  it('insertOne routes through saga session', async () => {
    txApi.insertOne.mockResolvedValue({ _id: new ObjectId(), id: 'x' });
    const c = new SagaCollection('items', mongo);
    await c.insertOne({ name: 'a' });
    expect(txApi.insertOne).toHaveBeenCalledWith(
      'items',
      { name: 'a' },
      undefined,
    );
  });

  it('throws when no active app transaction', () => {
    const m = {
      getDb: jest.fn(() => ({ collection: jest.fn(() => rawColl) })),
      getActiveSagaSession: jest.fn(() => undefined),
    } as unknown as MongoService;
    const c = new SagaCollection('items', m);
    expect(() => c.findOne({})).toThrow('No active saga session');
  });
});
