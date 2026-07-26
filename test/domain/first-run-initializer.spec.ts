import { FirstRunInitializer } from '../../src/engines/bootstrap/services/first-run-initializer.service';
import { DatabaseConfigService } from '../../src/shared/services';

describe('FirstRunInitializer', () => {
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

  it('runs snapshot physical migrations before schema healing preflight', async () => {
    const calls: string[] = [];
    const initializer = new FirstRunInitializer({
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
        runCoreTableRenamesBeforeMetadataSync: jest.fn(async () => {
          calls.push('core-migrate');
        }),
        runTableRenamesBeforeMetadataSync: jest.fn(async () => undefined),
        prepareMigrationExecutionPlan: jest.fn(async () => {
          calls.push('validate-migration');
        }),
        runPhysicalTableRenamesAndDropsAfterCoverage: jest.fn(async () => {
          calls.push('physical-table-migrate');
        }),
        runPhysicalMigrationsBeforeMetadataSync: jest.fn(async () => {
          calls.push('migrate');
        }),
        hasMigrations: jest.fn(() => true),
        runMigrations: jest.fn(async () => {
          calls.push('metadata-migrate');
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

    expect(calls.slice(0, 8)).toEqual([
      'core-migrate',
      'validate-migration',
      'physical-table-migrate',
      'migrate',
      'metadata-migrate',
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
        runCoreTableRenamesBeforeMetadataSync: migrate,
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
        runCoreTableRenamesBeforeMetadataSync: jest.fn(async () => undefined),
        runTableRenamesBeforeMetadataSync: jest.fn(async () => undefined),
        prepareMigrationExecutionPlan: jest.fn(async () => undefined),
        runPhysicalTableRenamesAndDropsAfterCoverage: jest.fn(
          async () => undefined,
        ),
        runPhysicalMigrationsBeforeMetadataSync: jest.fn(async () => undefined),
        hasMigrations: jest.fn(() => false),
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
        runCoreTableRenamesBeforeMetadataSync: jest.fn(async () => undefined),
        runTableRenamesBeforeMetadataSync: jest.fn(async () => undefined),
        prepareMigrationExecutionPlan: jest.fn(async () => undefined),
        runPhysicalTableRenamesAndDropsAfterCoverage: jest.fn(
          async () => undefined,
        ),
        runPhysicalMigrationsBeforeMetadataSync: jest.fn(async () => undefined),
        hasMigrations: jest.fn(() => false),
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
