import type { KnexService } from '../../../engines/knex';
import type { MongoService } from '../../../engines/mongo';
import type { DatabaseConfigService } from '../../../shared/services';
import { REDIS_TTL } from '../../../shared/utils/constant';

export class RuntimeSchemaUnitOfWorkService {
  private readonly databaseConfigService: DatabaseConfigService;
  private readonly knexService: KnexService;
  private readonly mongoService: MongoService;

  constructor(deps: {
    databaseConfigService: DatabaseConfigService;
    knexService: KnexService;
    mongoService: MongoService;
  }) {
    this.databaseConfigService = deps.databaseConfigService;
    this.knexService = deps.knexService;
    this.mongoService = deps.mongoService;
  }

  async run<T>(callback: () => Promise<T>): Promise<T> {
    if (!this.databaseConfigService.isMongoDb()) {
      return this.knexService.transaction(async () => callback());
    }
    const result = await this.mongoService.runInSaga(async () => callback(), {
      forceApplicationTransaction: true,
      scopeRawDbAccess: true,
      sagaOptions: {
        maxDurationMs: REDIS_TTL.PROVISION_LOCK_TTL,
      },
    });
    return result.data as T;
  }
}
