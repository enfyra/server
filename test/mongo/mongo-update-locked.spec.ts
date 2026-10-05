import { describe, expect, it, vi } from 'vitest';
import { MongoService } from '../../src/engines/mongo/services/mongo.service';

describe('Mongo updateLocked capability and scope', () => {
  it('rejects standalone without invoking the callback or starting a saga', async () => {
    const service = new MongoService({});
    const callback = vi.fn();
    const saga = vi.spyOn(service, 'runInSaga');
    await expect(service.runWithLockedRecord('accounts', 'id', callback)).rejects.toThrow('requires native MongoDB transactions');
    expect(callback).not.toHaveBeenCalled();
    expect(saga).not.toHaveBeenCalled();
  });

  it('joins an existing native transaction and gives the owner responsibility for retry', async () => {
    const service = new MongoService({});
    const session = { withTransaction: vi.fn(async (work: () => Promise<void>, _options: unknown) => work()), endSession: vi.fn(async () => {}) };
    Object.assign(service, { nativeMultiDocSupported: true, client: { startSession: () => session } });
    const result = await service.runWithLockedRecord('accounts', 'id', () =>
      service.runWithLockedRecord('accounts', 'id', async () => 90),
    );
    expect(result).toBe(90);
    expect(session.withTransaction).toHaveBeenCalledTimes(1);
    expect(session.withTransaction.mock.calls[0][1]).toEqual({ timeoutMS: 30000 });
    expect(session.endSession).toHaveBeenCalledTimes(1);
  });
});
