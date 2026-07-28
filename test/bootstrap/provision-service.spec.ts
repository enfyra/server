import { describe, expect, it, vi } from 'vitest';
import { ProvisionService } from '../../src/engines/bootstrap/services/provision.service';

function createService(overrides: Partial<any> = {}) {
  return new ProvisionService({
    commonService: { delay: vi.fn() },
    queryBuilderService: { isMongoDb: () => false },
    databaseConfigService: { getDbType: () => 'postgres' },
    mySqlBootstrapSnapshotService: { recoverPending: vi.fn() },
    mySqlRuntimeWriteBarrierService: {
      recoverExclusive: vi.fn(async (callback) => callback()),
    },
    routeDefinitionProcessor: { ensureMissingHandlers: vi.fn() },
    migrationJournalService: {
      recoverPending: vi.fn().mockResolvedValue(undefined),
      cleanup: vi.fn().mockResolvedValue(undefined),
    },
    mongoMigrationJournalService: { cleanup: vi.fn() },
    mongoSchemaMigrationService: { recoverPendingMigrationSagas: vi.fn() },
    runtimeSchemaJournalService: {
      markRecoveredRollbacks: vi.fn().mockResolvedValue(undefined),
      recoverUnresolved: vi.fn().mockResolvedValue(undefined),
      cleanup: vi.fn().mockResolvedValue(undefined),
    },
    ...overrides,
  } as any);
}

describe('ProvisionService', () => {
  it('fails boot when SQL journal recovery times out', async () => {
    vi.useFakeTimers();
    let rejectDangling: (err: Error) => void;
    const dangling = new Promise<void>((_, reject) => {
      rejectDangling = reject;
    });
    dangling.catch(() => undefined);
    const service = createService({
      migrationJournalService: {
        recoverPending: vi.fn(() => dangling),
        cleanup: vi.fn().mockResolvedValue(undefined),
      },
    });
    (service as any).journalRecoveryTimeoutMs = 5;

    const promise = service.recoverJournals();
    const assertion = expect(promise).rejects.toThrow('timed out');
    await vi.advanceTimersByTimeAsync(5);
    await assertion;
    rejectDangling!(new Error('cleanup'));
    vi.useRealTimers();
  });

  it('fails boot when SQL journal recovery throws', async () => {
    const service = createService({
      migrationJournalService: {
        recoverPending: vi.fn().mockRejectedValue(
          new Error('unresolved journals'),
        ),
        cleanup: vi.fn().mockResolvedValue(undefined),
      },
    });

    await expect(service.recoverJournals()).rejects.toThrow(
      'unresolved journals',
    );
  });

  it('marks MySQL runtime mutations rolled back by snapshot recovery before journal classification', async () => {
    const calls: string[] = [];
    const service = createService({
      databaseConfigService: { getDbType: () => 'mysql' },
      mySqlBootstrapSnapshotService: {
        recoverPending: vi.fn(async () => ({
          rolledBackMutationIds: ['runtime-schema:restored'],
        })),
      },
      runtimeSchemaJournalService: {
        markRecoveredRollbacks: vi.fn(async () => {
          calls.push('mark-rolled-back');
        }),
        recoverUnresolved: vi.fn(async () => {
          calls.push('classify-unresolved');
        }),
      },
      migrationJournalService: {
        recoverPending: vi.fn(async () => {
          calls.push('recover-sql-journal');
        }),
        cleanup: vi.fn().mockResolvedValue(undefined),
      },
    });

    await service.recoverJournals();

    expect(calls).toEqual([
      'mark-rolled-back',
      'recover-sql-journal',
      'classify-unresolved',
    ]);
  });
});
