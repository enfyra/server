import { describe, expect, it, vi } from 'vitest';
import { MongoSchemaMigrationLockService } from '../../src/engines/mongo/services/mongo-schema-migration-lock.service';

describe('MongoSchemaMigrationLockService', () => {
  it('uses the raw database outside the protected saga scope', async () => {
    const collection = {
      findOneAndUpdate: vi.fn(async (_filter: any, update: any) => ({
        _id: 'global',
        isLocked: true,
        lockToken: update.$set.lockToken,
        lockExpiresAt: update.$set.lockExpiresAt,
      })),
    };
    const rawDb = {
      listCollections: vi.fn(() => ({ toArray: vi.fn(async () => [{}]) })),
      collection: vi.fn(() => collection),
    };
    const mongoService = {
      getRawDb: vi.fn(() => rawDb),
      getActiveSagaSession: vi.fn(() => ({
        txId: 'tx-runtime-schema',
        purpose: 'runtime-schema',
        mutationId: 'runtime-schema:test',
      })),
      getDb: vi.fn(() => {
        throw new Error('scoped database must not be used for lock state');
      }),
    };
    const service = new MongoSchemaMigrationLockService({
      mongoService: mongoService as any,
    });

    await expect(service.acquire('test')).resolves.toEqual({
      token: expect.any(String),
    });
    expect(mongoService.getRawDb).toHaveBeenCalledOnce();
    expect(mongoService.getDb).not.toHaveBeenCalled();
    expect(collection.findOneAndUpdate).toHaveBeenCalledWith(
      expect.any(Object),
      expect.objectContaining({
        $set: expect.objectContaining({
          sagaSessionId: 'tx-runtime-schema',
          purpose: 'runtime-schema',
          mutationId: 'runtime-schema:test',
        }),
      }),
      expect.any(Object),
    );
  });
});
