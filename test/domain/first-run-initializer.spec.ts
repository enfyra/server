import { FirstRunInitializer } from '../../src/engines/bootstrap/services/first-run-initializer.service';
import { DatabaseConfigService } from '../../src/shared/services';

describe('FirstRunInitializer', () => {
  const originalLogDisableConsole = process.env.LOG_DISABLE_CONSOLE;
  const bootstrapUnitOfWorkService = {
    run: jest.fn(async (callback: () => Promise<unknown>) => callback()),
  };

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

  it('renders bootstrap progress as a filled terminal bar', () => {
    delete process.env.LOG_DISABLE_CONSOLE;
    const write = jest
      .spyOn(process.stdout, 'write')
      .mockImplementation(() => true);
    const initializer = new FirstRunInitializer({} as any);

    (initializer as any).logProgress(
      'Installing',
      45,
      'healing system metadata',
    );

    const line = String(write.mock.calls[0][0]);
    const bar = line.match(/\[([█░]+)\]/)?.[1];
    expect(line).toContain('Installing');
    expect(line).toContain('45% healing system metadata');
    expect(bar).toHaveLength(30);
    expect(bar?.match(/█/g)).toHaveLength(14);
    expect(bar?.match(/░/g)).toHaveLength(16);
  });

  it('derives percentage from completed weight while preserving change counts', () => {
    delete process.env.LOG_DISABLE_CONSOLE;
    const write = jest
      .spyOn(process.stdout, 'write')
      .mockImplementation(() => true);
    const initializer = new FirstRunInitializer({} as any);
    (initializer as any).progressTotal = 100;
    (initializer as any).progressCompleted = 1;
    (initializer as any).progressWeightTotal = 100;
    (initializer as any).progressWeightCompleted = 15;

    (initializer as any).logPlannedProgress('Installing', 'first change');

    expect(String(write.mock.calls[0][0])).toContain(
      '15% first change (1/100)',
    );
  });

  it('runs snapshot physical migrations before schema healing preflight', async () => {
    const calls: string[] = [];
    const initializer = new FirstRunInitializer({
      bootstrapUnitOfWorkService,
      commonService: { delay: jest.fn() },
      queryBuilderService: {},
      cacheService: {
        acquire: jest.fn(async () => true),
        renew: jest.fn(async () => true),
        release: jest.fn(async () => undefined),
      },
      instanceService: { getInstanceId: jest.fn(() => 'test-instance') },
      metadataCacheService: {
        clearMetadataCache: jest.fn(async () => undefined),
        getMetadata: jest.fn(async () => ({})),
        reload: jest.fn(async () => undefined),
      },
      metadataProvisionService: {
        createInitMetadata: jest.fn(async () => {
          calls.push('provision');
        }),
      },
      metadataMigrationService: {
        executeCoreMigrationPlan: jest.fn(async () => {
          calls.push('core-migrate');
        }),
        prepareMigrationExecutionPlan: jest.fn(async () => {
          calls.push('validate-migration');
        }),
        executeRemainingMigrationPlan: jest.fn(async () => {
          calls.push('migration-plan');
        }),
        assertSnapshotTargetStateAfterHealing: jest.fn(async () => undefined),
      },
      dataProvisionService: {
        insertAllDefaultRecords: jest.fn(async () => undefined),
      },
      dataMigrationService: {
        hasMigrations: jest.fn(() => false),
        runMigrations: jest.fn(),
      },
      schemaHealingService: {
        repairSystemPhysicalColumnsBeforeMetadataProvision: jest.fn(
          async () => {
            calls.push('heal-preflight');
          },
        ),
        repairSystemMetadataFromSnapshot: jest.fn(async () => {
          calls.push('metadata-heal');
        }),
        repairDerivedContracts: jest.fn(async () => undefined),
        runExplicitRepairsIfNeeded: jest.fn(async () => undefined),
      },
      routeDefinitionProcessor: {
        ensureMissingHandlers: jest.fn(async () => undefined),
      },
      snapshotTargetVerifierService: {
        assertSchemaTargetState: jest.fn(async () => {
          calls.push('schema-target-verify');
        }),
        assertDataTargetState: jest.fn(async () => {
          calls.push('data-target-verify');
        }),
      },
    } as any);

    (initializer as any).findFirstSetting = jest.fn(async () => ({
      id: 1,
      isInit: false,
    }));
    (initializer as any).markInitialized = jest.fn(async () => undefined);

    await (initializer as any).runWithProgress();

    expect(calls.slice(0, 6)).toEqual([
      'validate-migration',
      'core-migrate',
      'migration-plan',
      'heal-preflight',
      'provision',
      'metadata-heal',
    ]);
    expect(calls).toContain('schema-target-verify');
    expect(calls.at(-1)).toBe('data-target-verify');
  });

  it('does not acquire the lock or sync when isInit is true', async () => {
    const acquire = jest.fn(async () => true);
    const migrate = jest.fn(async () => undefined);
    const initializer = new FirstRunInitializer({
      bootstrapUnitOfWorkService,
      commonService: { delay: jest.fn() },
      queryBuilderService: {},
      cacheService: {
        acquire,
        release: jest.fn(async () => undefined),
      },
      instanceService: { getInstanceId: jest.fn(() => 'test-instance') },
      metadataCacheService: {},
      metadataProvisionService: {},
      metadataMigrationService: {
        executeCoreMigrationPlan: migrate,
      },
      dataProvisionService: {},
      dataMigrationService: {},
      schemaHealingService: {},
      routeDefinitionProcessor: {},
    } as any);
    (initializer as any).findFirstSetting = jest.fn(async () => ({
      id: 1,
      isInit: true,
    }));

    await (initializer as any).runWithProgress();

    expect(acquire).not.toHaveBeenCalled();
    expect(migrate).not.toHaveBeenCalled();
  });

  it('fails boot when another instance never finishes initialization', async () => {
    const initializer = new FirstRunInitializer({
      bootstrapUnitOfWorkService,
      commonService: { delay: jest.fn(async () => undefined) },
      queryBuilderService: {},
      cacheService: {},
      instanceService: {},
      metadataCacheService: {},
      metadataProvisionService: {},
      metadataMigrationService: {},
      dataProvisionService: {},
      dataMigrationService: {},
      schemaHealingService: {},
      routeDefinitionProcessor: {},
    } as any);
    (initializer as any).findFirstSetting = jest.fn(async () => ({
      id: 1,
      isInit: false,
    }));

    await expect((initializer as any).waitUntilDone(1)).rejects.toThrow(
      /Timed out waiting for snapshot initialization/,
    );
  });

  it('does not mark initialized when final target attestation fails', async () => {
    const release = jest.fn(async () => undefined);
    const markInitialized = jest.fn(async () => undefined);
    const initializer = new FirstRunInitializer({
      bootstrapUnitOfWorkService,
      commonService: { delay: jest.fn() },
      queryBuilderService: {},
      cacheService: {
        acquire: jest.fn(async () => true),
        renew: jest.fn(async () => true),
        release,
      },
      instanceService: { getInstanceId: jest.fn(() => 'test-instance') },
      metadataCacheService: {
        clearMetadataCache: jest.fn(async () => undefined),
        getMetadata: jest.fn(async () => ({})),
        reload: jest.fn(async () => undefined),
      },
      metadataProvisionService: {
        createInitMetadata: jest.fn(async () => undefined),
      },
      metadataMigrationService: {
        executeCoreMigrationPlan: jest.fn(async () => undefined),
        prepareMigrationExecutionPlan: jest.fn(async () => undefined),
        executeRemainingMigrationPlan: jest.fn(async () => undefined),
        assertSnapshotTargetStateAfterHealing: jest.fn(async () => undefined),
      },
      dataProvisionService: {
        insertAllDefaultRecords: jest.fn(async () => undefined),
      },
      dataMigrationService: {
        hasMigrations: jest.fn(() => false),
      },
      schemaHealingService: {
        repairSystemPhysicalColumnsBeforeMetadataProvision: jest.fn(
          async () => undefined,
        ),
        repairSystemMetadataFromSnapshot: jest.fn(async () => undefined),
        repairDerivedContracts: jest.fn(async () => undefined),
        runExplicitRepairsIfNeeded: jest.fn(async () => undefined),
      },
      routeDefinitionProcessor: {
        ensureMissingHandlers: jest.fn(async () => undefined),
      },
      snapshotTargetVerifierService: {
        assertSchemaTargetState: jest.fn(async () => {
          throw new Error('target mismatch');
        }),
        assertDataTargetState: jest.fn(async () => undefined),
      },
    } as any);
    (initializer as any).findFirstSetting = jest.fn(async () => ({
      id: 1,
      isInit: false,
    }));
    (initializer as any).markInitialized = markInitialized;

    await expect((initializer as any).runWithProgress()).rejects.toThrow(
      'target mismatch',
    );
    expect(markInitialized).not.toHaveBeenCalled();
    expect(release).toHaveBeenCalled();
  });

  it('does not attest or finalize after losing the provision lease', async () => {
    const assertSchemaTargetState = jest.fn(async () => undefined);
    const markInitialized = jest.fn(async () => undefined);
    const initializer = new FirstRunInitializer({
      bootstrapUnitOfWorkService,
      commonService: { delay: jest.fn() },
      queryBuilderService: {},
      cacheService: {
        acquire: jest.fn(async () => true),
        renew: jest.fn(async () => false),
        release: jest.fn(async () => true),
      },
      instanceService: { getInstanceId: jest.fn(() => 'test-instance') },
      metadataCacheService: {
        clearMetadataCache: jest.fn(async () => undefined),
        reload: jest.fn(async () => undefined),
      },
      metadataProvisionService: {
        createInitMetadata: jest.fn(async () => undefined),
      },
      metadataMigrationService: {
        executeCoreMigrationPlan: jest.fn(async () => undefined),
        prepareMigrationExecutionPlan: jest.fn(async () => undefined),
        executeRemainingMigrationPlan: jest.fn(async () => undefined),
      },
      dataProvisionService: {
        insertAllDefaultRecords: jest.fn(async () => undefined),
      },
      dataMigrationService: {
        hasMigrations: jest.fn(() => false),
      },
      schemaHealingService: {
        repairSystemPhysicalColumnsBeforeMetadataProvision: jest.fn(
          async () => undefined,
        ),
        repairSystemMetadataFromSnapshot: jest.fn(async () => undefined),
        repairDerivedContracts: jest.fn(async () => undefined),
        runExplicitRepairsIfNeeded: jest.fn(async () => undefined),
      },
      routeDefinitionProcessor: {
        ensureMissingHandlers: jest.fn(async () => undefined),
      },
      snapshotTargetVerifierService: {
        assertSchemaTargetState,
        assertDataTargetState: jest.fn(async () => undefined),
      },
    } as any);
    (initializer as any).findFirstSetting = jest.fn(async () => ({
      id: 1,
      isInit: false,
    }));
    (initializer as any).markInitialized = markInitialized;

    await expect((initializer as any).runWithProgress()).rejects.toThrow(
      /provision lease/,
    );
    expect(assertSchemaTargetState).not.toHaveBeenCalled();
    expect(markInitialized).not.toHaveBeenCalled();
  });

  it('fails finalization when the SQL setting row disappears', async () => {
    const where = jest.fn(() => ({ update: jest.fn(async () => 0) }));
    const update = jest.fn(async () => 0);
    where.mockReturnValue({ update });
    const knex = jest.fn(() => ({ where }));
    const initializer = new FirstRunInitializer({
      bootstrapUnitOfWorkService,
      commonService: {},
      queryBuilderService: { getKnex: jest.fn(() => knex) },
      cacheService: {},
      instanceService: {},
      metadataCacheService: {},
      metadataProvisionService: {},
      metadataMigrationService: {},
      dataProvisionService: {},
      dataMigrationService: {},
      schemaHealingService: {},
      routeDefinitionProcessor: {},
    } as any);
    (initializer as any).findFirstSetting = jest.fn(async () => ({ id: 1 }));
    (initializer as any).findSqlSettingTableName = jest.fn(
      async () => 'enfyra_setting',
    );

    await expect((initializer as any).markInitialized()).rejects.toThrow(
      /setting row was not updated/,
    );
    expect(where).toHaveBeenCalledWith({ id: 1, isInit: false });
  });

  it('fails finalization when the Mongo setting document disappears', async () => {
    jest
      .spyOn(DatabaseConfigService, 'instanceIsMongoDb')
      .mockReturnValue(true);
    const updateOne = jest.fn(async () => ({
      acknowledged: true,
      matchedCount: 0,
      modifiedCount: 0,
    }));
    const initializer = new FirstRunInitializer({
      bootstrapUnitOfWorkService,
      commonService: {},
      queryBuilderService: {
        getMongoDb: jest.fn(() => ({
          collection: jest.fn(() => ({ updateOne })),
        })),
      },
      cacheService: {},
      instanceService: {},
      metadataCacheService: {},
      metadataProvisionService: {},
      metadataMigrationService: {},
      dataProvisionService: {},
      dataMigrationService: {},
      schemaHealingService: {},
      routeDefinitionProcessor: {},
    } as any);
    (initializer as any).findFirstSetting = jest.fn(async () => ({ _id: 1 }));
    (initializer as any).findMongoSettingCollectionName = jest.fn(
      async () => 'enfyra_setting',
    );

    await expect((initializer as any).markInitialized()).rejects.toThrow(
      /setting document was not updated/,
    );
    expect(updateOne).toHaveBeenCalledWith(
      { _id: 1, isInit: false },
      expect.any(Object),
    );
  });
});
