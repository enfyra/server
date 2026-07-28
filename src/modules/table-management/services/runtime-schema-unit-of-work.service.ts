import type { KnexService } from '../../../engines/knex';
import type { MongoService } from '../../../engines/mongo';
import type { DatabaseConfigService } from '../../../shared/services';
import type { MySqlBootstrapSnapshotService } from '../../../engines/bootstrap/services/mysql-bootstrap-snapshot.service';
import { REDIS_TTL } from '../../../shared/utils/constant';
import type { MySqlRuntimeWriteBarrierService } from '../../../engines/knex';
import type { RuntimeSchemaMutationContract } from '../types/runtime-schema-mutation.types';
import type { RuntimeSchemaUnitOfWorkContext } from '../types/runtime-schema-executor.types';

export class RuntimeSchemaUnitOfWorkService {
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
    this.mySqlRuntimeWriteBarrierService = deps.mySqlRuntimeWriteBarrierService;
  }

  async run<T>(
    callback: (context: RuntimeSchemaUnitOfWorkContext) => Promise<T>,
    contract?: RuntimeSchemaMutationContract,
  ): Promise<T> {
    if (!this.databaseConfigService.isMongoDb()) {
      if (this.databaseConfigService.getDbType() === 'mysql') {
        if (!contract) {
          throw new Error('MySQL runtime schema UOW requires its immutable contract');
        }
        return this.mySqlRuntimeWriteBarrierService.runExclusive(
          { mutationId: contract.mutationId },
          () =>
            this.mySqlBootstrapSnapshotService.run(() => callback({}), {
              mutationId: contract.mutationId,
            }),
        );
      }
      return this.knexService.transaction(async () => callback({}));
    }
    if (!contract) {
      throw new Error('MongoDB runtime schema UOW requires its immutable contract');
    }
    const result = await this.mongoService.runInSaga(
      async () =>
        callback({ sagaSessionId: this.mongoService.getCurrentSagaId() }),
      {
      forceApplicationTransaction: true,
      scopeRawDbAccess: true,
      sagaOptions: {
        maxDurationMs: REDIS_TTL.PROVISION_LOCK_TTL,
        purpose: 'runtime-schema',
        mutationId: contract.mutationId,
      },
      },
    );
    return result.data as T;
  }
}
