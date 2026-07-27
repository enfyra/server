import { describe, expect, it, vi } from 'vitest';
import { ProvisionService } from '../../src/engines/bootstrap/services/provision.service';

function createService(overrides: Partial<any> = {}) {
  return new ProvisionService({
    commonService: { delay: vi.fn() },
    queryBuilderService: { isMongoDb: () => false },
    databaseConfigService: { getDbType: () => 'postgres' },
    mySqlBootstrapSnapshotService: { recoverPending: vi.fn() },
    routeDefinitionProcessor: { ensureMissingHandlers: vi.fn() },
    migrationJournalService: {
      recoverPending: vi.fn().mockResolvedValue(undefined),
      cleanup: vi.fn().mockResolvedValue(undefined),
    },
    mongoMigrationJournalService: { cleanup: vi.fn() },
    mongoSchemaMigrationService: { recoverPendingMigrationSagas: vi.fn() },
    runtimeSchemaJournalService: {
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
    await vi.advanceTimersByTimeAsync(5);

    await expect(promise).rejects.toThrow('timed out');
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
});
