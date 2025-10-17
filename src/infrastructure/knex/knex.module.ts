import { Global, Module, forwardRef } from '@nestjs/common';
import { KnexService } from './knex.service';
import { RelationHandlerService } from './services/relation-handler.service';
import { SqlSchemaMigrationService } from './services/sql-schema-migration.service';
import { DatabaseSchemaService } from './services/database-schema.service';
import { CacheModule } from '../cache/cache.module';

@Global()
@Module({
  imports: [forwardRef(() => CacheModule)],
  providers: [KnexService, RelationHandlerService, SqlSchemaMigrationService, DatabaseSchemaService],
  exports: [KnexService, RelationHandlerService, SqlSchemaMigrationService],
})
export class KnexModule {}

