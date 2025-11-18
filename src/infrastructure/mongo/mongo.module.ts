import { Module, Global } from '@nestjs/common';
import { MongoService } from './services/mongo.service';
import { MongoSchemaMigrationService } from './services/mongo-schema-migration.service';
import { MongoSchemaMigrationLockService } from './services/mongo-schema-migration-lock.service';

@Global()
@Module({
  providers: [MongoService, MongoSchemaMigrationService, MongoSchemaMigrationLockService],
  exports: [MongoService, MongoSchemaMigrationService, MongoSchemaMigrationLockService],
})
export class MongoModule {}

