import { Module, Global } from '@nestjs/common';
import { MongoService } from './services/mongo.service';
import { MongoSchemaMigrationService } from './services/mongo-schema-migration.service';
import { MongoSchemaMigrationLockService } from './services/mongo-schema-migration-lock.service';
import { MongoTransactionLockService } from './services/mongo-transaction-lock.service';
import { MongoOperationLogService } from './services/mongo-operation-log.service';
import { MongoSagaCoordinator } from './services/mongo-saga-coordinator.service';

@Global()
@Module({
  providers: [
    MongoService,
    MongoSchemaMigrationService,
    MongoSchemaMigrationLockService,
    MongoTransactionLockService,
    MongoOperationLogService,
    MongoSagaCoordinator,
  ],
  exports: [
    MongoService,
    MongoSchemaMigrationService,
    MongoSchemaMigrationLockService,
    MongoTransactionLockService,
    MongoOperationLogService,
    MongoSagaCoordinator,
  ],
})
export class MongoModule {}
