import { asClass } from 'awilix';
import {
  KnexService,
  KnexHookManagerService,
  MigrationJournalService,
  MySqlRuntimeWriteBarrierService,
  ReplicationManager,
  SchemaMigrationLockService,
  SqlPoolClusterCoordinatorService,
  SqlSchemaDiffService,
  SqlSchemaMigrationService,
} from '../../engines/knex';
import { SqlFunctionService } from '../../engines/sql';

export const sqlRegisters = {
  knexService: asClass(KnexService)
    .singleton()
    .disposer((service: KnexService) => service.onDestroy()),
  knexHookManagerService: asClass(KnexHookManagerService).singleton(),
  replicationManager: asClass(ReplicationManager)
    .singleton()
    .disposer((service: ReplicationManager) => service.onDestroy()),
  sqlSchemaMigrationService: asClass(SqlSchemaMigrationService).singleton(),
  sqlSchemaDiffService: asClass(SqlSchemaDiffService).singleton(),
  migrationJournalService: asClass(MigrationJournalService).singleton(),
  mySqlRuntimeWriteBarrierService: asClass(MySqlRuntimeWriteBarrierService).singleton(),
  schemaMigrationLockService: asClass(SchemaMigrationLockService).singleton(),
  sqlPoolClusterCoordinatorService: asClass(SqlPoolClusterCoordinatorService)
    .singleton()
    .disposer((service: SqlPoolClusterCoordinatorService) => service.onDestroy()),
  sqlFunctionService: asClass(SqlFunctionService).singleton(),
} as const;
