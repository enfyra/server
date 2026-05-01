import { describe, expect, it, vi } from 'vitest';
import { MongoService } from '../../src/engines/mongo';

function makeService() {
  return new MongoService({
    envService: {} as any,
    databaseConfigService: {} as any,
    metadataCacheService: {} as any,
    mongoRelationManagerService: {} as any,
    lazyRef: {} as any,
  });
}

describe('MongoService native transaction capability detection', () => {
  it('falls back to application transactions when the real transaction probe operation fails', async () => {
    const service = makeService();
    const session = {
      startTransaction: vi.fn(),
      abortTransaction: vi.fn().mockResolvedValue(undefined),
      endSession: vi.fn().mockResolvedValue(undefined),
      inTransaction: vi.fn().mockReturnValue(true),
    };
    const findOne = vi
      .fn()
      .mockRejectedValue(
        new Error(
          'Transaction numbers are only allowed on a replica set member or mongos',
        ),
      );
    const db = {
      admin: vi.fn(() => ({
        command: vi.fn().mockResolvedValue({ ok: 1 }),
      })),
      collection: vi.fn(() => ({ findOne })),
    };

    (service as any).client = {
      startSession: vi.fn(() => session),
    };
    (service as any).db = db;

    await (service as any).refreshNativeTransactionCapability();

    expect(service.supportsNativeMultiDocumentTransactions()).toBe(false);
    expect(findOne).toHaveBeenCalledWith(
      {},
      expect.objectContaining({ session }),
    );
    expect(session.abortTransaction).toHaveBeenCalledTimes(1);
    expect(session.endSession).toHaveBeenCalledTimes(1);
  });

  it('enables native transactions immediately when hello reports a replica set', async () => {
    const service = makeService();
    const startSession = vi.fn();
    const db = {
      admin: vi.fn(() => ({
        command: vi.fn().mockResolvedValue({ ok: 1, setName: 'rs0' }),
      })),
    };

    (service as any).client = { startSession };
    (service as any).db = db;

    await (service as any).refreshNativeTransactionCapability();

    expect(service.supportsNativeMultiDocumentTransactions()).toBe(true);
    expect(startSession).not.toHaveBeenCalled();
  });
});
