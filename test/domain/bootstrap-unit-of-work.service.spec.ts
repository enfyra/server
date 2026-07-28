import { BootstrapUnitOfWorkService } from '../../src/engines/bootstrap';

describe('BootstrapUnitOfWorkService', () => {
  it('runs the complete SQL bootstrap callback in one outer transaction', async () => {
    const transaction = jest.fn(async (callback) => callback({}));
    const service = new BootstrapUnitOfWorkService({
      databaseConfigService: {
        isMongoDb: () => false,
        getDbType: () => 'postgres',
      },
      knexService: { transaction },
      mongoService: {},
      mySqlBootstrapSnapshotService: {},
      mySqlRuntimeWriteBarrierService: {},
    } as any);
    const callback = jest.fn(async () => 'committed');

    await expect(service.run(callback)).resolves.toBe('committed');
    expect(transaction).toHaveBeenCalledTimes(1);
    expect(callback).toHaveBeenCalledTimes(1);
  });

  it('forces the Mongo bootstrap through the application saga', async () => {
    const runInSaga = jest.fn(async (callback, options) => ({
      success: true,
      data: await callback(),
      txId: 'bootstrap-saga',
      options,
    }));
    const service = new BootstrapUnitOfWorkService({
      databaseConfigService: { isMongoDb: () => true },
      knexService: {},
      mongoService: { runInSaga },
      mySqlBootstrapSnapshotService: {},
      mySqlRuntimeWriteBarrierService: {},
    } as any);

    await expect(service.run(async () => 'committed')).resolves.toBe(
      'committed',
    );
    expect(runInSaga).toHaveBeenCalledWith(expect.any(Function), {
      forceApplicationTransaction: true,
      scopeRawDbAccess: true,
      sagaOptions: {
        maxDurationMs: expect.any(Number),
        purpose: 'bootstrap',
        mutationId: expect.stringMatching(/^bootstrap:/),
      },
    });
  });

  it('propagates a bootstrap failure so the outer transaction rolls back', async () => {
    const transaction = jest.fn(async (callback) => callback({}));
    const service = new BootstrapUnitOfWorkService({
      databaseConfigService: {
        isMongoDb: () => false,
        getDbType: () => 'postgres',
      },
      knexService: { transaction },
      mongoService: {},
      mySqlBootstrapSnapshotService: {},
      mySqlRuntimeWriteBarrierService: {},
    } as any);

    await expect(
      service.run(async () => {
        throw new Error('attestation failed');
      }),
    ).rejects.toThrow('attestation failed');
  });

  it('uses durable compensation for MySQL bootstrap DDL and data', async () => {
    const run = jest.fn(async (callback) => callback());
    const runExclusive = jest.fn(async (_context, callback) => callback());
    const service = new BootstrapUnitOfWorkService({
      databaseConfigService: {
        isMongoDb: () => false,
        getDbType: () => 'mysql',
      },
      knexService: {},
      mongoService: {},
      mySqlBootstrapSnapshotService: { run },
      mySqlRuntimeWriteBarrierService: { runExclusive },
    } as any);

    await expect(service.run(async () => 'committed')).resolves.toBe(
      'committed',
    );
    const fenceContext = runExclusive.mock.calls[0][0];
    expect(fenceContext.mutationId).toMatch(/^bootstrap:/);
    expect(runExclusive).toHaveBeenCalledTimes(1);
    expect(run).toHaveBeenCalledWith(expect.any(Function), {
      mutationId: fenceContext.mutationId,
    });
  });
});
