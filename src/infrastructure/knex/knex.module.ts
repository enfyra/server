import { Global, Module, forwardRef } from '@nestjs/common';
import { KnexService } from './knex.service';
import { SqlSchemaMigrationService } from './services/sql-schema-migration.service';
import { DatabaseSchemaService } from './services/database-schema.service';
import { CacheModule } from '../cache/cache.module';

@Global()
@Module({
  imports: [forwardRef(() => CacheModule)],
  providers: [KnexService, SqlSchemaMigrationService, DatabaseSchemaService],
  exports: [KnexService, SqlSchemaMigrationService],
})
export class KnexModule {}

