import { Test, TestingModule } from '@nestjs/testing';
import { MongoSagaCoordinator } from '../../src/infrastructure/mongo/services/mongo-saga-coordinator.service';
import { MongoService } from '../../src/infrastructure/mongo/services/mongo.service';
import { MongoSagaLockService } from '../../src/infrastructure/mongo/services/mongo-saga-lock.service';
import { MongoOperationLogService } from '../../src/infrastructure/mongo/services/mongo-operation-log.service';
import { CacheService } from '../../src/infrastructure/cache/services/cache.service';
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

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        MongoSagaCoordinator,
        {
          provide: MongoService,
          useValue: { getDb: () => db },
        },
        {
          provide: MongoSagaLockService,
          useValue: {
            cleanupOrphanedLocks,
            getOrphanMarkerRecoveryPlan: jest.fn().mockResolvedValue({
              shouldUnsetMarkers: false,
              needsRollbackFirst: false,
            }),
          },
        },
        {
          provide: MongoOperationLogService,
          useValue: { cleanupOldLogs },
        },
        {
          provide: InstanceService,
          useValue: { getInstanceId: () => 'instance-test-abc' },
        },
        {
          provide: CacheService,
          useValue: { acquire, release },
        },
      ],
    }).compile();

    coordinator = module.get<MongoSagaCoordinator>(MongoSagaCoordinator);
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

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        MongoSagaCoordinator,
        {
          provide: MongoService,
          useValue: { getDb: () => db },
        },
        {
          provide: MongoSagaLockService,
          useValue: {
            cleanupOrphanedLocks: cleanupOrphanedLocksLocal,
            getOrphanMarkerRecoveryPlan: jest.fn().mockResolvedValue({
              shouldUnsetMarkers: false,
              needsRollbackFirst: false,
            }),
          },
        },
        {
          provide: MongoOperationLogService,
          useValue: { cleanupOldLogs: cleanupOldLogsLocal },
        },
        {
          provide: InstanceService,
          useValue: { getInstanceId: () => 'solo' },
        },
      ],
    }).compile();

    const solo = module.get<MongoSagaCoordinator>(MongoSagaCoordinator);
    await solo.recoverOrphanedSagas('boot');
    expect(cleanupOrphanedLocksLocal).toHaveBeenCalled();
    expect(cleanupOldLogsLocal).toHaveBeenCalled();
  });
});
