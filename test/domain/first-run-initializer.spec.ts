import { FirstRunInitializer } from '../../src/engines/bootstrap/services/first-run-initializer.service';

describe('FirstRunInitializer', () => {
  const originalLogDisableConsole = process.env.LOG_DISABLE_CONSOLE;

  beforeEach(() => {
    process.env.LOG_DISABLE_CONSOLE = '1';
  });

  afterEach(() => {
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
        release: jest.fn(async () => undefined),
      },
      instanceService: { getInstanceId: jest.fn(() => 'test-instance') },
      metadataCacheService: {
        clearMetadataCache: jest.fn(async () => undefined),
        getMetadata: jest.fn(async () => ({})),
      },
      metadataProvisionService: {
        createInitMetadata: jest.fn(async () => undefined),
      },
      metadataMigrationService: {
        runCoreTableRenamesBeforeMetadataSync: jest.fn(async () => {
          calls.push('core-migrate');
        }),
        runTableRenamesBeforeMetadataSync: jest.fn(async () => undefined),
        runPhysicalMigrationsBeforeMetadataSync: jest.fn(async () => {
          calls.push('migrate');
        }),
        hasMigrations: jest.fn(() => true),
        runMigrations: jest.fn(async () => {
          calls.push('metadata-migrate');
        }),
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
        runIfNeeded: jest.fn(async () => undefined),
      },
      routeDefinitionProcessor: {
        ensureMissingHandlers: jest.fn(async () => undefined),
      },
    } as any);

    (initializer as any).findFirstSetting = jest.fn(async () => ({
      id: 1,
      isInit: false,
    }));
    (initializer as any).markInitialized = jest.fn(async () => undefined);

    await (initializer as any).runWithProgress();

    expect(calls.slice(0, 5)).toEqual([
      'core-migrate',
      'migrate',
      'heal-preflight',
      'metadata-migrate',
      'metadata-heal',
    ]);
  });
});
