import { MongoSagaCoordinator } from '../../src/infrastructure/mongo/services/mongo-saga-coordinator.service';
import { MongoService } from '../../src/infrastructure/mongo/services/mongo.service';
import { MongoSagaLockService } from '../../src/infrastructure/mongo/services/mongo-saga-lock.service';
import { MongoOperationLogService } from '../../src/infrastructure/mongo/services/mongo-operation-log.service';
import { InstanceService } from '../../src/shared/services/instance.service';
import {
  SAGA_ORPHAN_RECOVERY_LOCK_KEY,
  REDIS_TTL,
} from '../../src/shared/utils/constant';

describe('MongoSagaCoordinator orphan recovery Redis lock', () => {
  let coordinator: MongoSagaCoordinator;
  let acquire: jest.Mock;
  let release: jest.Mock;
  let cleanupOrphanedLocks: jest.Mock;
  let cleanupOldLogs: jest.Mock;

  async function createModule() {
    acquire = jest.fn();
    release = jest.fn().mockResolvedValue(true);
    cleanupOrphanedLocks = jest.fn().mockResolvedValue(0);
    cleanupOldLogs = jest.fn().mockResolvedValue(0);

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
    (lockService as any).cleanupOrphanedLocks = cleanupOrphanedLocks;
    (lockService as any).getOrphanMarkerRecoveryPlan = jest
      .fn()
      .mockResolvedValue({
        shouldUnsetMarkers: false,
        needsRollbackFirst: false,
      });

    const logService = new MongoOperationLogService({ mongoService });
    (logService as any).cleanupOldLogs = cleanupOldLogs;

    const instanceService = new InstanceService();
    Object.defineProperty(instanceService, 'instanceId', {
      value: 'instance-test-abc',
    });

    const cacheService = { acquire, release } as any;

    coordinator = new MongoSagaCoordinator({
      mongoService,
      lockService,
      logService,
      instanceService,
      cacheService,
    });
  }

  it('skips recovery when Redis lock not acquired', async () => {
    await createModule();
    acquire.mockResolvedValue(false);

    const before = coordinator.getSagaRecoveryMetrics();
    const result = await coordinator.recoverOrphanedSagas('boot');

    expect(result).toEqual({ cleaned: 0, recovered: 0 });
    expect(acquire).toHaveBeenCalledWith(
      SAGA_ORPHAN_RECOVERY_LOCK_KEY,
      'instance-test-abc',
      REDIS_TTL.SAGA_ORPHAN_RECOVERY_LOCK_TTL,
    );
    expect(release).not.toHaveBeenCalled();
    expect(cleanupOrphanedLocks).not.toHaveBeenCalled();
    const after = coordinator.getSagaRecoveryMetrics();
    expect(after.skippedDueToRedisLock).toBe(before.skippedDueToRedisLock + 1);
    expect(after.totalRuns).toBe(before.totalRuns);
  });

  it('runs recovery and releases Redis lock when acquired', async () => {
    await createModule();
    acquire.mockResolvedValue(true);

    const before = coordinator.getSagaRecoveryMetrics();
    await coordinator.recoverOrphanedSagas('periodic');

    expect(cleanupOrphanedLocks).toHaveBeenCalled();
    expect(cleanupOldLogs).toHaveBeenCalled();
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
    const cleanupOldLogsLocal = jest.fn().mockResolvedValue(0);
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
    (lockService as any).cleanupOrphanedLocks = cleanupOrphanedLocksLocal;
    (lockService as any).getOrphanMarkerRecoveryPlan = jest
      .fn()
      .mockResolvedValue({
        shouldUnsetMarkers: false,
        needsRollbackFirst: false,
      });

    const logService = new MongoOperationLogService({ mongoService });
    (logService as any).cleanupOldLogs = cleanupOldLogsLocal;

    const instanceService = new InstanceService();
    Object.defineProperty(instanceService, 'instanceId', { value: 'solo' });

    const solo = new MongoSagaCoordinator({
      mongoService,
      lockService,
      logService,
      instanceService,
      cacheService: undefined,
    });
    await solo.recoverOrphanedSagas('boot');
    expect(cleanupOrphanedLocksLocal).toHaveBeenCalled();
    expect(cleanupOldLogsLocal).toHaveBeenCalled();
  });
});
