import { Module, Global } from '@nestjs/common';
import { MongoService } from './services/mongo.service';
import { MongoSchemaMigrationService } from './services/mongo-schema-migration.service';

@Global()
@Module({
  providers: [MongoService, MongoSchemaMigrationService],
  exports: [MongoService, MongoSchemaMigrationService],
})
export class MongoModule {}

