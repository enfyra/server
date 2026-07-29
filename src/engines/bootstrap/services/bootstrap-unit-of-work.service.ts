import { randomUUID } from 'node:crypto';
import type { KnexService } from '../../knex';
import type { MySqlRuntimeWriteBarrierService } from '../../knex';
import type { MongoService } from '../../mongo';
import type { DatabaseConfigService } from '../../../shared/services';
import { REDIS_TTL } from '../../../shared/utils/constant';
import type { MySqlBootstrapSnapshotService } from './mysql-bootstrap-snapshot.service';

export class BootstrapUnitOfWorkService {
  private readonly databaseConfigService: DatabaseConfigService;
  private readonly knexService: KnexService;
  private readonly mongoService: MongoService;
  private readonly mySqlBootstrapSnapshotService: MySqlBootstrapSnapshotService;
  private readonly mySqlRuntimeWriteBarrierService: MySqlRuntimeWriteBarrierService;

  constructor(deps: {
    databaseConfigService: DatabaseConfigService;
    knexService: KnexService;
    mongoService: MongoService;
    mySqlBootstrapSnapshotService: MySqlBootstrapSnapshotService;
    mySqlRuntimeWriteBarrierService: MySqlRuntimeWriteBarrierService;
  }) {
    this.databaseConfigService = deps.databaseConfigService;
    this.knexService = deps.knexService;
    this.mongoService = deps.mongoService;
    this.mySqlBootstrapSnapshotService = deps.mySqlBootstrapSnapshotService;
    this.mySqlRuntimeWriteBarrierService =
      deps.mySqlRuntimeWriteBarrierService;
  }

  async run<T>(callback: () => Promise<T>): Promise<T> {
    const mutationId = `bootstrap:${randomUUID()}`;
    if (!this.databaseConfigService.isMongoDb()) {
      if (this.databaseConfigService.getDbType() === 'mysql') {
        return this.mySqlRuntimeWriteBarrierService.runExclusive(
          { mutationId },
          () =>
            this.mySqlBootstrapSnapshotService.run(callback, { mutationId }),
        );
      }
      return this.knexService.transaction(async () => callback());
    }

    const result = await this.mongoService.runInSaga(async () => callback(), {
      forceApplicationTransaction: true,
      scopeRawDbAccess: true,
      sagaOptions: {
        maxDurationMs: REDIS_TTL.PROVISION_LOCK_TTL,
        purpose: 'bootstrap',
        mutationId,
      },
    });
    return result.data as T;
  }
}
