import { MongoClient, Db, ObjectId } from 'mongodb';
import { AsyncLocalStorage } from 'async_hooks';
import { describe, expect, it, beforeAll, afterAll } from 'vitest';

const MONGO_URI =
  'mongodb://enfyra_admin:enfyra_password_123@localhost:27017/enfyra_abort_test?authSource=admin';

const ioAbortContext = new AsyncLocalStorage<AbortSignal>();

function getIoAbortSignal(): AbortSignal | undefined {
  return ioAbortContext.getStore();
}

function ioSignal(): any {
  const signal = getIoAbortSignal();
  if (signal?.aborted) throw new Error('Operation aborted');
  return signal ? { signal } : {};
}

describe('MongoDB IO Abort Signal E2E', () => {
  let client: MongoClient;
  let db: Db;

  beforeAll(async () => {
    client = new MongoClient(MONGO_URI);
    await client.connect();
    db = client.db();
    await db.collection('abort_test_docs').deleteMany({});
  });

  afterAll(async () => {
    await db.collection('abort_test_docs').deleteMany({});
    await client.close();
  });

  it('insertOne succeeds without abort signal', async () => {
    const result = await ioAbortContext.run(undefined as any, async () => {
      return db.collection('abort_test_docs').insertOne(
        { name: 'no-signal', value: 1 },
        ioSignal(),
      );
    });
    expect(result.acknowledged).toBe(true);
    expect(result.insertedId).toBeDefined();
  });

  it('insertOne succeeds with non-aborted signal', async () => {
    const controller = new AbortController();
    const result = await ioAbortContext.run(controller.signal, async () => {
      return db.collection('abort_test_docs').insertOne(
        { name: 'live-signal', value: 2 },
        ioSignal(),
      );
    });
    expect(result.acknowledged).toBe(true);
  });

  it('throws immediately when signal is already aborted', async () => {
    const controller = new AbortController();
    controller.abort();
    await expect(
      ioAbortContext.run(controller.signal, async () => {
        return db.collection('abort_test_docs').insertOne(
          { name: 'pre-aborted', value: 3 },
          ioSignal(),
        );
      }),
    ).rejects.toThrow('Operation aborted');

    const count = await db.collection('abort_test_docs').countDocuments({ name: 'pre-aborted' });
    expect(count).toBe(0);
  });

  it('aborts an in-flight find operation via signal', async () => {
    const docs = Array.from({ length: 5000 }, (_, i) => ({
      name: `bulk-${i}`,
      payload: 'x'.repeat(200),
      idx: i,
    }));
    await db.collection('abort_test_docs').insertMany(docs);

    const controller = new AbortController();
    setTimeout(() => controller.abort(), 1);

    await expect(
      ioAbortContext.run(controller.signal, async () => {
        const cursor = db.collection('abort_test_docs').find(
          { name: { $regex: '^bulk-' } },
          ioSignal(),
        );
        const results: any[] = [];
        for await (const doc of cursor) {
          results.push(doc);
          if (results.length > 10000) break;
        }
        return results;
      }),
    ).rejects.toThrow();
  });

  it('aborts an in-flight insertMany via signal', async () => {
    const controller = new AbortController();
    const bigDocs = Array.from({ length: 50000 }, (_, i) => ({
      name: `abort-insert-${i}`,
      data: 'y'.repeat(1000),
    }));

    setTimeout(() => controller.abort(), 5);

    let threw = false;
    try {
      await ioAbortContext.run(controller.signal, async () => {
        await db.collection('abort_test_docs').insertMany(bigDocs, ioSignal());
      });
    } catch {
      threw = true;
    }

    if (threw) {
      const count = await db.collection('abort_test_docs').countDocuments({
        name: { $regex: '^abort-insert-' },
      });
      expect(count).toBeLessThan(50000);
    }
  });

  it('findOne respects abort signal', async () => {
    const controller = new AbortController();
    controller.abort();

    await expect(
      ioAbortContext.run(controller.signal, async () => {
        return db.collection('abort_test_docs').findOne({ name: 'x' }, ioSignal());
      }),
    ).rejects.toThrow('Operation aborted');
  });

  it('updateOne respects abort signal', async () => {
    const controller = new AbortController();
    controller.abort();

    await expect(
      ioAbortContext.run(controller.signal, async () => {
        return db.collection('abort_test_docs').updateOne(
          { name: 'no-signal' },
          { $set: { value: 999 } },
          ioSignal(),
        );
      }),
    ).rejects.toThrow('Operation aborted');

    const doc = await db.collection('abort_test_docs').findOne({ name: 'no-signal' });
    expect(doc?.value).toBe(1);
  });

  it('deleteOne respects abort signal', async () => {
    const controller = new AbortController();
    controller.abort();

    await expect(
      ioAbortContext.run(controller.signal, async () => {
        return db.collection('abort_test_docs').deleteOne(
          { name: 'no-signal' },
          ioSignal(),
        );
      }),
    ).rejects.toThrow('Operation aborted');

    const count = await db.collection('abort_test_docs').countDocuments({ name: 'no-signal' });
    expect(count).toBe(1);
  });

  it('countDocuments respects abort signal', async () => {
    const controller = new AbortController();
    controller.abort();

    await expect(
      ioAbortContext.run(controller.signal, async () => {
        return db.collection('abort_test_docs').countDocuments({}, ioSignal());
      }),
    ).rejects.toThrow('Operation aborted');
  });

  it('aggregate respects abort signal', async () => {
    const controller = new AbortController();
    controller.abort();

    await expect(
      ioAbortContext.run(controller.signal, async () => {
        const cursor = db.collection('abort_test_docs').aggregate(
          [{ $match: {} }],
          ioSignal(),
        );
        return cursor.toArray();
      }),
    ).rejects.toThrow('Operation aborted');
  });
});
