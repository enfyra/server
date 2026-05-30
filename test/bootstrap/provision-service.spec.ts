import { describe, expect, it, vi } from 'vitest';
import { ProvisionService } from '../../src/engines/bootstrap/services/provision.service';

function createService(overrides: Partial<any> = {}) {
  return new ProvisionService({
    commonService: { delay: vi.fn() },
    queryBuilderService: { isMongoDb: () => false },
    routeDefinitionProcessor: { ensureMissingHandlers: vi.fn() },
    migrationJournalService: {
      recoverPending: vi.fn().mockResolvedValue(undefined),
      cleanup: vi.fn().mockResolvedValue(undefined),
    },
    mongoMigrationJournalService: { cleanup: vi.fn() },
    mongoSchemaMigrationService: { recoverPendingMigrationSagas: vi.fn() },
    ...overrides,
  } as any);
}

describe('ProvisionService', () => {
  it('does not let non-fatal SQL journal recovery block boot forever', async () => {
    vi.useFakeTimers();
    const service = createService({
      migrationJournalService: {
        recoverPending: vi.fn(
          () => new Promise<void>(() => undefined),
        ),
        cleanup: vi.fn().mockResolvedValue(undefined),
      },
    });
    (service as any).journalRecoveryTimeoutMs = 5;

    const promise = service.recoverJournals();
    await vi.advanceTimersByTimeAsync(5);

    await expect(promise).resolves.toBeUndefined();
    vi.useRealTimers();
  });
});
