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
    } as any);

    await expect(service.run(async () => 'committed')).resolves.toBe(
      'committed',
    );
    expect(runInSaga).toHaveBeenCalledWith(expect.any(Function), {
      forceApplicationTransaction: true,
      scopeRawDbAccess: true,
      sagaOptions: { maxDurationMs: expect.any(Number) },
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
    } as any);

    await expect(
      service.run(async () => {
        throw new Error('attestation failed');
      }),
    ).rejects.toThrow('attestation failed');
  });

  it('uses durable compensation for MySQL bootstrap DDL and data', async () => {
    const run = jest.fn(async (callback) => callback());
    const service = new BootstrapUnitOfWorkService({
      databaseConfigService: {
        isMongoDb: () => false,
        getDbType: () => 'mysql',
      },
      knexService: {},
      mongoService: {},
      mySqlBootstrapSnapshotService: { run },
    } as any);

    await expect(service.run(async () => 'committed')).resolves.toBe(
      'committed',
    );
    expect(run).toHaveBeenCalledTimes(1);
  });
});
