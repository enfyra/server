import { FirstRunInitializer } from '../../src/engines/bootstrap/services/first-run-initializer.service';

type SafetyFixtureOptions = {
  cacheService?: Record<string, unknown>;
  commonService?: Record<string, unknown>;
  instanceId?: string;
  onPhase?: (phase: string, occurrence: number) => Promise<void> | void;
  state?: { isInit: boolean };
  mongoSagaCoordinator?: Record<string, unknown>;
};

function createSafetyFixture(options: SafetyFixtureOptions = {}) {
  const state = options.state ?? { isInit: false };
  const calls: string[] = [];
  const phaseOccurrences = new Map<string, number>();
  const runPhase = async (phase: string): Promise<void> => {
    calls.push(phase);
    const occurrence = (phaseOccurrences.get(phase) ?? 0) + 1;
    phaseOccurrences.set(phase, occurrence);
    await options.onPhase?.(phase, occurrence);
  };
  const cacheService = {
    acquire: jest.fn(async () => true),
    get: jest.fn(async () => null),
    renew: jest.fn(async () => true),
    release: jest.fn(async () => true),
    ...options.cacheService,
  };
  const initializer = new FirstRunInitializer({
    bootstrapUnitOfWorkService: {
      run: jest.fn(async (callback: () => Promise<unknown>) => callback()),
    },
    commonService: {
      delay: jest.fn(async () => undefined),
      ...options.commonService,
    },
    queryBuilderService: {},
    cacheService,
    instanceService: {
      getInstanceId: jest.fn(() => options.instanceId ?? 'test-instance'),
    },
    metadataCacheService: {
      clearMetadataCache: jest.fn(async () => runPhase('clearMetadataCache')),
      reload: jest.fn(async () => runPhase('reloadMetadataCache')),
    },
    metadataProvisionService: {
      createInitMetadata: jest.fn(async () => runPhase('createInitMetadata')),
    },
    metadataMigrationService: {
      executeCoreMigrationPlan: jest.fn(async () =>
        runPhase('executeCoreMigrationPlan'),
      ),
      prepareMigrationExecutionPlan: jest.fn(async () =>
        runPhase('prepareMigrationExecutionPlan'),
      ),
      executeRemainingMigrationPlan: jest.fn(async () =>
        runPhase('executeRemainingMigrationPlan'),
      ),
    },
    dataProvisionService: {
      insertAllDefaultRecords: jest.fn(async () =>
        runPhase('insertAllDefaultRecords'),
      ),
    },
    dataMigrationService: {
      hasMigrations: jest.fn(() => true),
      runMigrations: jest.fn(async () => runPhase('runDataMigrations')),
    },
    schemaHealingService: {
      repairSystemPhysicalColumnsBeforeMetadataProvision: jest.fn(async () =>
        runPhase('repairSystemPhysicalColumnsBeforeMetadataProvision'),
      ),
      repairSystemMetadataFromSnapshot: jest.fn(async () =>
        runPhase('repairSystemMetadataFromSnapshot'),
      ),
      repairDerivedContracts: jest.fn(async () =>
        runPhase('repairDerivedContracts'),
      ),
      runExplicitRepairsIfNeeded: jest.fn(async () =>
        runPhase('runExplicitRepairsIfNeeded'),
      ),
    },
    routeDefinitionProcessor: {
      ensureMissingHandlers: jest.fn(async () =>
        runPhase('ensureMissingHandlers'),
      ),
    },
    snapshotTargetVerifierService: {
      assertSchemaTargetState: jest.fn(async () =>
        runPhase('assertSchemaTargetState'),
      ),
      assertDataTargetState: jest.fn(async () =>
        runPhase('assertDataTargetState'),
      ),
    },
    mongoSagaCoordinator: options.mongoSagaCoordinator,
  } as any);

  (initializer as any).findFirstSetting = jest.fn(async () => ({
    id: 1,
    isInit: state.isInit,
  }));
  (initializer as any).markInitialized = jest.fn(async () => {
    calls.push('markInitialized');
    state.isInit = true;
    await options.onPhase?.('markInitialized', 1);
  });

  return {
    cacheService,
    calls,
    initializer,
    isInitialized: () => state.isInit,
  };
}

describe('FirstRunInitializer safety', () => {
  const originalLogDisableConsole = process.env.LOG_DISABLE_CONSOLE;

  beforeEach(() => {
    process.env.LOG_DISABLE_CONSOLE = '1';
  });

  afterEach(() => {
    jest.restoreAllMocks();
    if (originalLogDisableConsole === undefined) {
      delete process.env.LOG_DISABLE_CONSOLE;
    } else {
      process.env.LOG_DISABLE_CONSOLE = originalLogDisableConsole;
    }
  });

  it('takes over initialization after the previous owner lock expires', async () => {
    const acquire = jest
      .fn()
      .mockResolvedValueOnce(false)
      .mockResolvedValueOnce(true);
    const get = jest
      .fn()
      .mockResolvedValueOnce('dead-instance')
      .mockResolvedValueOnce(null);
    const fixture = createSafetyFixture({
      cacheService: { acquire, get },
    });

    await (fixture.initializer as any).runWithProgress();

    expect(acquire).toHaveBeenCalledTimes(2);
    expect(get).toHaveBeenCalledTimes(2);
    expect(fixture.isInitialized()).toBe(true);
  });

  it('recovers an interrupted bootstrap saga before planning new mutations', async () => {
    let recovered = false;
    const fixture = createSafetyFixture({
      mongoSagaCoordinator: {
        recoverOrWaitForPurpose: jest.fn(async () => {
          recovered = true;
        }),
      },
      onPhase: (phase) => {
        if (phase === 'prepareMigrationExecutionPlan') {
          expect(recovered).toBe(true);
        }
      },
    });

    await (fixture.initializer as any).runWithProgress();

    expect(recovered).toBe(true);
  });

  it('stops before the next mutation when the provision lease is lost', async () => {
    let leaseOwned = true;
    const fixture = createSafetyFixture({
      cacheService: {
        renew: jest.fn(async () => leaseOwned),
      },
      onPhase: (phase) => {
        if (phase === 'executeCoreMigrationPlan') {
          leaseOwned = false;
        }
      },
    });

    await expect(
      (fixture.initializer as any).runWithProgress(),
    ).rejects.toThrow(/provision lease/);

    expect(fixture.calls).toEqual([
      'prepareMigrationExecutionPlan',
      'executeCoreMigrationPlan',
      'clearMetadataCache',
    ]);
    expect(fixture.isInitialized()).toBe(false);
  });

  it('serializes concurrent initializers and lets the follower observe completion', async () => {
    const state = { isInit: false };
    let lockOwner: string | null = null;
    let releaseFirstPhase: (() => void) | null = null;
    let announceFirstPhase: (() => void) | null = null;
    const firstPhaseStarted = new Promise<void>((resolve) => {
      announceFirstPhase = resolve;
    });
    const firstPhaseGate = new Promise<void>((resolve) => {
      releaseFirstPhase = resolve;
    });
    const cacheFor = (owner: string) => ({
      acquire: jest.fn(async (_key: string, value: string) => {
        if (lockOwner !== null) return false;
        lockOwner = value;
        return true;
      }),
      get: jest.fn(async () => lockOwner),
      renew: jest.fn(async (_key: string, value: string) => {
        return lockOwner === value;
      }),
      release: jest.fn(async (_key: string, value: string) => {
        if (lockOwner !== value) return false;
        lockOwner = null;
        return true;
      }),
      owner,
    });
    const owner = createSafetyFixture({
      cacheService: cacheFor('owner'),
      instanceId: 'owner',
      state,
      onPhase: async (phase) => {
        if (phase !== 'executeCoreMigrationPlan') return;
        announceFirstPhase?.();
        await firstPhaseGate;
      },
    });
    const follower = createSafetyFixture({
      cacheService: cacheFor('follower'),
      commonService: {
        delay: jest.fn(
          async () =>
            new Promise<void>((resolve) => {
              setImmediate(resolve);
            }),
        ),
      },
      instanceId: 'follower',
      state,
    });

    const ownerRun = (owner.initializer as any).runWithProgress();
    await firstPhaseStarted;
    const followerRun = (follower.initializer as any).runWithProgress();
    await new Promise<void>((resolve) => setImmediate(resolve));
    releaseFirstPhase?.();
    await Promise.all([ownerRun, followerRun]);

    expect(state.isInit).toBe(true);
    expect(
      [...owner.calls, ...follower.calls].filter(
        (phase) => phase === 'markInitialized',
      ),
    ).toHaveLength(1);
    expect(follower.calls).not.toContain('insertAllDefaultRecords');
    expect(lockOwner).toBeNull();
  });

  it('converges after a crash following every mutation boundary', async () => {
    const mutationPhases = [
      { phase: 'prepareMigrationExecutionPlan', occurrence: 1 },
      { phase: 'executeCoreMigrationPlan', occurrence: 1 },
      { phase: 'executeRemainingMigrationPlan', occurrence: 1 },
      { phase: 'clearMetadataCache', occurrence: 1 },
      {
        phase: 'repairSystemPhysicalColumnsBeforeMetadataProvision',
        occurrence: 1,
      },
      { phase: 'createInitMetadata', occurrence: 1 },
      { phase: 'clearMetadataCache', occurrence: 2 },
      { phase: 'repairSystemMetadataFromSnapshot', occurrence: 1 },
      { phase: 'clearMetadataCache', occurrence: 3 },
      { phase: 'repairDerivedContracts', occurrence: 1 },
      { phase: 'runExplicitRepairsIfNeeded', occurrence: 1 },
      { phase: 'reloadMetadataCache', occurrence: 1 },
      { phase: 'insertAllDefaultRecords', occurrence: 1 },
      { phase: 'ensureMissingHandlers', occurrence: 1 },
      { phase: 'runDataMigrations', occurrence: 1 },
    ];

    for (const crashPoint of mutationPhases) {
      const completed = new Set<string>();
      let crashed = false;
      const fixture = createSafetyFixture({
        onPhase: (phase, occurrence) => {
          completed.add(phase);
          if (
            phase === crashPoint.phase &&
            occurrence === crashPoint.occurrence &&
            !crashed
          ) {
            crashed = true;
            throw new Error(`crash after ${phase}#${occurrence}`);
          }
        },
      });

      await expect(
        (fixture.initializer as any).runWithProgress(),
      ).rejects.toThrow(
        `crash after ${crashPoint.phase}#${crashPoint.occurrence}`,
      );
      expect(fixture.isInitialized()).toBe(false);

      await (fixture.initializer as any).runWithProgress();

      expect(fixture.isInitialized()).toBe(true);
      for (const { phase } of mutationPhases) {
        expect(completed).toContain(phase);
      }
    }
  });

  it('does not rerun bootstrap after the final compare-and-set completed', async () => {
    let exitedAfterCommit = false;
    const fixture = createSafetyFixture({
      onPhase: (phase) => {
        if (phase === 'markInitialized' && !exitedAfterCommit) {
          exitedAfterCommit = true;
          throw new Error('process exited after final compare-and-set');
        }
      },
    });

    await expect(
      (fixture.initializer as any).runWithProgress(),
    ).rejects.toThrow('process exited after final compare-and-set');
    expect(fixture.isInitialized()).toBe(true);
    const mutationsBeforeRestart = fixture.calls.length;

    await (fixture.initializer as any).runWithProgress();

    expect(fixture.calls).toHaveLength(mutationsBeforeRestart);
  });
});
