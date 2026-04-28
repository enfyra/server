import { DatabaseConfigService } from '../../../shared/services';
import {
  KnexService,
  ReplicationManager,
  SqlPoolClusterCoordinatorService,
} from '../../../engines/knex';

export class RuntimeDbMetricsService {
  private readonly databaseConfigService: DatabaseConfigService;
  private readonly knexService: KnexService;
  private readonly replicationManager?: ReplicationManager;
  private readonly sqlPoolClusterCoordinatorService?: SqlPoolClusterCoordinatorService;

  constructor(deps: {
    databaseConfigService: DatabaseConfigService;
    knexService: KnexService;
    replicationManager?: ReplicationManager;
    sqlPoolClusterCoordinatorService?: SqlPoolClusterCoordinatorService;
  }) {
    this.databaseConfigService = deps.databaseConfigService;
    this.knexService = deps.knexService;
    this.replicationManager = deps.replicationManager;
    this.sqlPoolClusterCoordinatorService =
      deps.sqlPoolClusterCoordinatorService;
  }

  getDbStats() {
    if (this.databaseConfigService.isMongoDb()) {
      return { type: 'mongodb', pool: null };
    }
    return {
      type: this.databaseConfigService.getDbType(),
      pool:
        this.replicationManager?.getPoolStats?.() ??
        this.knexService?.getPoolStats?.() ??
        null,
    };
  }

  async getClusterStats() {
    return this.sqlPoolClusterCoordinatorService?.getClusterStats?.() ?? null;
  }

  getDbPoolTotals(db: any) {
    const pool = db?.pool;
    if (!pool) return { used: 0, idle: 0, available: 0, pending: 0 };
    const rows =
      pool.master || Array.isArray(pool.replicas)
        ? [
            pool.master,
            ...(pool.replicas ?? []).map((replica: any) => replica.pool),
          ]
        : [pool];

    return rows.reduce(
      (
        sum: {
          used: number;
          idle: number;
          available: number;
          pending: number;
        },
        row: any,
      ) => ({
        used: sum.used + (row?.used ?? 0),
        idle: sum.idle + (row?.idle ?? 0),
        available: sum.available + (row?.available ?? 0),
        pending: sum.pending + (row?.pending ?? 0),
      }),
      { used: 0, idle: 0, available: 0, pending: 0 },
    );
  }
}
