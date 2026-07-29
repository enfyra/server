import { describe, expect, it, vi } from 'vitest';
import { RuntimeSchemaUnitOfWorkService } from '../../src/modules/table-management/services/runtime-schema-unit-of-work.service';

function createHarness(dbType: 'postgres' | 'mysql' | 'mongodb') {
  const knexService = {
    transaction: vi.fn(async (callback: () => Promise<unknown>) => callback()),
  };
  const mongoService = {
    getCurrentSagaId: vi.fn(() => 'tx-runtime-schema-test'),
    runInSaga: vi.fn(async (callback: () => Promise<unknown>) => ({
      data: await callback(),
    })),
  };
  const mySqlBootstrapSnapshotService = {
    run: vi.fn(async (callback: () => Promise<unknown>) => callback()),
  };
  const mySqlRuntimeWriteBarrierService = {
    runExclusive: vi.fn(
      async (_context: unknown, callback: () => Promise<unknown>) => callback(),
    ),
  };
  const service = new RuntimeSchemaUnitOfWorkService({
    databaseConfigService: {
      isMongoDb: () => dbType === 'mongodb',
      getDbType: () => dbType,
    } as any,
    knexService: knexService as any,
    mongoService: mongoService as any,
    mySqlBootstrapSnapshotService: mySqlBootstrapSnapshotService as any,
    mySqlRuntimeWriteBarrierService: mySqlRuntimeWriteBarrierService as any,
  });
  return {
    service,
    knexService,
    mongoService,
    mySqlBootstrapSnapshotService,
    mySqlRuntimeWriteBarrierService,
  };
}

describe('RuntimeSchemaUnitOfWorkService', () => {
  it('uses a native transaction for PostgreSQL', async () => {
    const harness = createHarness('postgres');
    await expect(harness.service.run(async () => 'ok')).resolves.toBe('ok');
    expect(harness.knexService.transaction).toHaveBeenCalledOnce();
    expect(harness.mySqlBootstrapSnapshotService.run).not.toHaveBeenCalled();
    expect(harness.mongoService.runInSaga).not.toHaveBeenCalled();
  });

  it('uses a durable database snapshot for MySQL', async () => {
    const harness = createHarness('mysql');
    await expect(
      harness.service.run(async () => 'ok', {
        mutationId: 'runtime-schema:test',
      } as any),
    ).resolves.toBe('ok');
    expect(
      harness.mySqlRuntimeWriteBarrierService.runExclusive,
    ).toHaveBeenCalledWith(
      { mutationId: 'runtime-schema:test' },
      expect.any(Function),
    );
    expect(harness.mySqlBootstrapSnapshotService.run).toHaveBeenCalledWith(
      expect.any(Function),
      { mutationId: 'runtime-schema:test' },
    );
    expect(harness.knexService.transaction).not.toHaveBeenCalled();
    expect(harness.mongoService.runInSaga).not.toHaveBeenCalled();
  });

  it('uses the forced application saga for MongoDB', async () => {
    const harness = createHarness('mongodb');
    await expect(
      harness.service.run(async () => 'ok', {
        mutationId: 'runtime-schema:mongo-test',
      } as any),
    ).resolves.toBe('ok');
    expect(harness.mongoService.runInSaga).toHaveBeenCalledOnce();
    expect(harness.mongoService.runInSaga).toHaveBeenCalledWith(
      expect.any(Function),
      expect.objectContaining({
        forceApplicationTransaction: true,
        scopeRawDbAccess: true,
        sagaOptions: expect.objectContaining({
          purpose: 'runtime-schema',
          mutationId: 'runtime-schema:mongo-test',
        }),
      }),
    );
    expect(harness.knexService.transaction).not.toHaveBeenCalled();
    expect(harness.mySqlBootstrapSnapshotService.run).not.toHaveBeenCalled();
  });

  it('rejects a MongoDB runtime UOW without its immutable contract', async () => {
    const harness = createHarness('mongodb');
    await expect(harness.service.run(async () => 'ok')).rejects.toThrow(
      /requires its immutable contract/i,
    );
    expect(harness.mongoService.runInSaga).not.toHaveBeenCalled();
  });
});
