import { asClass, asFunction } from 'awilix';
import type { Cradle } from '../cradle';
import {
  MongoMigrationJournalService,
  MongoPhysicalMigrationService,
  MongoSagaSnapshotService,
  MongoHookManagerService,
  MongoRelationManagerService,
  MongoSagaCoordinator,
  MongoSagaLockService,
  MongoSchemaDiffService,
  MongoSchemaMigrationLockService,
  MongoSchemaMigrationService,
  MongoService,
} from '../../engines/mongo';

export const mongoRegisters = {
  mongoService: asClass(MongoService)
    .singleton()
    .disposer((service: MongoService) => service.onDestroy()),
  mongoSchemaMigrationService: asClass(MongoSchemaMigrationService).singleton(),
  mongoSchemaMigrationLockService: asClass(MongoSchemaMigrationLockService).singleton(),
  mongoSagaLockService: asClass(MongoSagaLockService).singleton(),
  mongoSagaCoordinator: asFunction(
    (cradle: Cradle) =>
      new MongoSagaCoordinator({
        mongoService: cradle.mongoService,
        lockService: cradle.mongoSagaLockService,
        snapshotService: cradle.mongoSagaSnapshotService,
        instanceService: cradle.instanceService,
        cacheService: cradle.cacheService,
      }),
  )
    .singleton()
    .disposer((service: MongoSagaCoordinator) => service.onDestroy()),
  mongoSagaSnapshotService: asClass(MongoSagaSnapshotService).singleton(),
  mongoMigrationJournalService: asClass(MongoMigrationJournalService).singleton(),
  mongoPhysicalMigrationService: asClass(MongoPhysicalMigrationService)
    .singleton()
    .disposer((service: MongoPhysicalMigrationService) => service.onDestroy()),
  mongoSchemaDiffService: asClass(MongoSchemaDiffService).singleton(),
  mongoHookManagerService: asClass(MongoHookManagerService).singleton(),
  mongoRelationManagerService: asClass(MongoRelationManagerService).singleton(),
} as const;
