import { describe, expect, it, vi } from 'vitest';
import { FirstRunInitializer } from '../../src/engines/bootstrap/services/first-run-initializer.service';

function publicationFixture(lossAt?: 'attestation' | 'publication') {
  let owned = true;
  let initialized = false;
  let committed = false;
  const noop = async () => undefined;
  const initializer = new FirstRunInitializer({
    commonService: { delay: noop },
    queryBuilderService: {},
    cacheService: {
      acquire: async () => true,
      renew: async () => owned,
      release: async () => owned,
    },
    instanceService: { getInstanceId: () => 'audit-publication-owner' },
    metadataCacheService: { clearMetadataCache: noop, reload: noop },
    metadataProvisionService: { createInitMetadata: noop },
    metadataMigrationService: {
      prepareMigrationExecutionPlan: noop,
      executeCoreMigrationPlan: noop,
      executeRemainingMigrationPlan: noop,
    },
    dataProvisionService: { insertAllDefaultRecords: noop },
    dataMigrationService: { hasMigrations: () => false },
    schemaHealingService: {
      repairSystemPhysicalColumnsBeforeMetadataProvision: noop,
      repairSystemMetadataFromSnapshot: noop,
      repairDerivedContracts: noop,
      runExplicitRepairsIfNeeded: noop,
    },
    routeDefinitionProcessor: { ensureMissingHandlers: noop },
    snapshotTargetVerifierService: {
      assertSchemaTargetState: noop,
      assertDataTargetState: async () => {
        if (lossAt === 'attestation') owned = false;
      },
    },
    bootstrapUnitOfWorkService: {
      run: async (callback: () => Promise<void>) => {
        const before = initialized;
        try {
          await callback();
          committed = true;
        } catch (error) {
          initialized = before;
          throw error;
        }
      },
    },
  } as unknown as ConstructorParameters<typeof FirstRunInitializer>[0]);
  const boundary = initializer as unknown as {
    findFirstSetting(): Promise<{ id: number; isInit: boolean }>;
    markInitialized(): Promise<void>;
    runWithProgress(): Promise<void>;
    logPlanning(): void;
    logProgress(): void;
    logPlannedProgress(): void;
  };
  vi.spyOn(boundary, 'findFirstSetting').mockImplementation(async () => ({
    id: 1,
    isInit: initialized,
  }));
  vi.spyOn(boundary, 'markInitialized').mockImplementation(async () => {
    if (lossAt === 'publication') owned = false;
    initialized = true;
  });
  vi.spyOn(boundary, 'logPlanning').mockImplementation(() => undefined);
  vi.spyOn(boundary, 'logProgress').mockImplementation(() => undefined);
  vi.spyOn(boundary, 'logPlannedProgress').mockImplementation(() => undefined);
  return {
    run: () => boundary.runWithProgress(),
    state: () => ({ initialized, committed }),
  };
}

describe('audit: bootstrap publication lease', () => {
  it('commits an attested target while the lease remains owned', async () => {
    const fixture = publicationFixture();
    await fixture.run();
    expect(fixture.state()).toEqual({ initialized: true, committed: true });
  });

  it.each(['attestation', 'publication'] as const)(
    'does not commit after lease loss during %s',
    async (lossAt) => {
      const fixture = publicationFixture(lossAt);
      const error = await fixture.run().then(
        () => null,
        (cause: unknown) => cause,
      );
      expect({ rejected: error instanceof Error, ...fixture.state() }).toEqual({
        rejected: true,
        initialized: false,
        committed: false,
      });
    },
  );
});
