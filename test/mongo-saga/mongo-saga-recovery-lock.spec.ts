import {
  MongoSagaCoordinator,
  MongoService,
  MongoSagaLockService,
  MongoSagaSnapshotService,
} from '../../src/engines/mongo';
import { InstanceService } from '../../src/shared/services';
import {
  SAGA_ORPHAN_RECOVERY_LOCK_KEY,
  REDIS_TTL,
} from '../../src/shared/utils/constant';

describe('MongoSagaCoordinator orphan recovery Redis lock', () => {
  let coordinator: MongoSagaCoordinator;
  let acquire: jest.Mock;
  let release: jest.Mock;
  let cleanupOrphanedLocks: jest.Mock;
  let cleanupOldSnapshots: jest.Mock;
  let getOpenSessions: jest.Mock;
  let claimNextRecoverableSession: jest.Mock;

  async function createModule() {
    acquire = jest.fn();
    release = jest.fn().mockResolvedValue(true);
    cleanupOrphanedLocks = jest.fn().mockResolvedValue(0);
    cleanupOldSnapshots = jest.fn().mockResolvedValue(0);
    getOpenSessions = jest.fn().mockResolvedValue([]);
    claimNextRecoverableSession = jest.fn().mockResolvedValue(null);

    const db = {
      listCollections: () => ({
        toArray: async () => [],
      }),
    };

    // Manual dependency injection
    const envService = {
      get: (key: string) => process.env[key],
    } as any;

    const mongoService = new MongoService({ envService });
    Object.defineProperty(mongoService, 'db', { value: db });

    const lockService = new MongoSagaLockService({ mongoService });
    (lockService as any).getOpenSessions = getOpenSessions;
    (lockService as any).claimNextRecoverableSession =
      claimNextRecoverableSession;
    (lockService as any).cleanupOrphanedLocks = cleanupOrphanedLocks;
    (lockService as any).getOrphanMarkerRecoveryPlan = jest
      .fn()
      .mockResolvedValue({
        shouldUnsetMarkers: false,
        needsRollbackFirst: false,
      });

    const snapshotService = new MongoSagaSnapshotService({ mongoService });
    (snapshotService as any).cleanupOldSnapshots = cleanupOldSnapshots;

    const instanceService = new InstanceService();
    Object.defineProperty(instanceService, 'instanceId', {
      value: 'instance-test-abc',
    });

    const cacheService = { acquire, release } as any;

    coordinator = new MongoSagaCoordinator({
      mongoService,
      lockService,
      snapshotService,
      instanceService,
      cacheService,
    });
  }

  it('waits and reacquires the recovery barrier during boot', async () => {
    await createModule();
    acquire.mockResolvedValueOnce(false).mockResolvedValueOnce(true);

    const before = coordinator.getSagaRecoveryMetrics();
    const result = await coordinator.recoverOrphanedSagas('boot');

    expect(result).toEqual({ cleaned: 0, recovered: 0 });
    expect(acquire).toHaveBeenNthCalledWith(
      1,
      SAGA_ORPHAN_RECOVERY_LOCK_KEY,
      'instance-test-abc',
      REDIS_TTL.SAGA_ORPHAN_RECOVERY_LOCK_TTL,
    );
    expect(acquire).toHaveBeenCalledTimes(2);
    expect(release).toHaveBeenCalled();
    expect(cleanupOrphanedLocks).toHaveBeenCalled();
    const after = coordinator.getSagaRecoveryMetrics();
    expect(after.skippedDueToRedisLock).toBe(before.skippedDueToRedisLock + 1);
    expect(after.totalRuns).toBe(before.totalRuns + 1);
  });

  it('runs recovery and releases Redis lock when acquired', async () => {
    await createModule();
    acquire.mockResolvedValue(true);

    const before = coordinator.getSagaRecoveryMetrics();
    await coordinator.recoverOrphanedSagas('periodic');

    expect(cleanupOrphanedLocks).toHaveBeenCalled();
    expect(cleanupOldSnapshots).not.toHaveBeenCalled();
    expect(release).toHaveBeenCalledWith(
      SAGA_ORPHAN_RECOVERY_LOCK_KEY,
      'instance-test-abc',
    );
    const after = coordinator.getSagaRecoveryMetrics();
    expect(after.totalRuns).toBe(before.totalRuns + 1);
    expect(after.periodicRuns).toBe(before.periodicRuns + 1);
  });

  it('releases Redis lock when recovery body throws', async () => {
    await createModule();
    acquire.mockResolvedValue(true);
    cleanupOrphanedLocks.mockRejectedValueOnce(new Error('db unavailable'));

    await expect(coordinator.recoverOrphanedSagas('boot')).rejects.toThrow(
      'db unavailable',
    );
    expect(release).toHaveBeenCalledWith(
      SAGA_ORPHAN_RECOVERY_LOCK_KEY,
      'instance-test-abc',
    );
  });

  it('runs recovery without Redis when CacheService is absent', async () => {
    const cleanupOrphanedLocksLocal = jest.fn().mockResolvedValue(0);
    const cleanupOldSnapshotsLocal = jest.fn().mockResolvedValue(0);
    const db = {
      listCollections: () => ({
        toArray: async () => [],
      }),
    };

    // Manual dependency injection without CacheService
    const envService = {
      get: (key: string) => process.env[key],
    } as any;

    const mongoService = new MongoService({ envService });
    Object.defineProperty(mongoService, 'db', { value: db });

    const lockService = new MongoSagaLockService({ mongoService });
    (lockService as any).getOpenSessions = jest.fn().mockResolvedValue([]);
    (lockService as any).claimNextRecoverableSession = jest
      .fn()
      .mockResolvedValue(null);
    (lockService as any).cleanupOrphanedLocks = cleanupOrphanedLocksLocal;
    (lockService as any).getOrphanMarkerRecoveryPlan = jest
      .fn()
      .mockResolvedValue({
        shouldUnsetMarkers: false,
        needsRollbackFirst: false,
      });

    const snapshotService = new MongoSagaSnapshotService({ mongoService });
    (snapshotService as any).cleanupOldSnapshots = cleanupOldSnapshotsLocal;

    const instanceService = new InstanceService();
    Object.defineProperty(instanceService, 'instanceId', { value: 'solo' });

    const solo = new MongoSagaCoordinator({
      mongoService,
      lockService,
      snapshotService,
      instanceService,
      cacheService: undefined,
    });
    await solo.recoverOrphanedSagas('boot');
    expect(cleanupOrphanedLocksLocal).toHaveBeenCalled();
    expect(cleanupOldSnapshotsLocal).not.toHaveBeenCalled();
  });

  it('waits for a live bootstrap saga lease before recovering and continuing', async () => {
    await createModule();
    const recover = jest
      .spyOn(coordinator, 'recoverOrphanedSagas')
      .mockResolvedValue({ cleaned: 0, recovered: 0 });
    getOpenSessions
      .mockResolvedValueOnce([
        {
          txId: 'crashed-bootstrap',
          purpose: 'bootstrap',
          status: 'active',
          expiresAt: new Date(Date.now() + 5),
        },
      ])
      .mockResolvedValueOnce([]);

    await coordinator.recoverOrWaitForPurpose('bootstrap', 100);

    expect(recover).toHaveBeenCalledTimes(2);
    expect(getOpenSessions).toHaveBeenCalledTimes(2);
  });
});
