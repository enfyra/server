import { Module, Global } from '@nestjs/common';
import { MongoService } from './services/mongo.service';
import { MongoSchemaMigrationService } from './services/mongo-schema-migration.service';
import { MongoSchemaMigrationLockService } from './services/mongo-schema-migration-lock.service';
import { MongoSagaLockService } from './services/mongo-saga-lock.service';
import { MongoOperationLogService } from './services/mongo-operation-log.service';
import { MongoSagaCoordinator } from './services/mongo-saga-coordinator.service';
import { MongoMigrationJournalService } from './services/mongo-migration-journal.service';
import { MongoSchemaDiffService } from './services/mongo-schema-diff.service';
import { MongoRelationManagerService } from './services/mongo-relation-manager.service';

@Global()
@Module({
  providers: [
    MongoService,
    MongoSchemaMigrationService,
    MongoSchemaMigrationLockService,
    MongoSagaLockService,
    MongoOperationLogService,
    MongoSagaCoordinator,
    MongoMigrationJournalService,
    MongoSchemaDiffService,
    MongoRelationManagerService,
  ],
  exports: [
    MongoService,
    MongoSchemaMigrationService,
    MongoSchemaMigrationLockService,
    MongoSagaLockService,
    MongoOperationLogService,
    MongoSagaCoordinator,
    MongoMigrationJournalService,
    MongoSchemaDiffService,
    MongoRelationManagerService,
  ],
})
export class MongoModule {}
