import { Global, Module, forwardRef } from '@nestjs/common';
import { KnexService } from './knex.service';
import { RelationHandlerService } from './services/relation-handler.service';
import { SchemaMigrationService } from './services/schema-migration.service';
import { CacheModule } from '../cache/cache.module';

@Global()
@Module({
  imports: [forwardRef(() => CacheModule)],
  providers: [KnexService, RelationHandlerService, SchemaMigrationService],
  exports: [KnexService, RelationHandlerService, SchemaMigrationService],
})
export class KnexModule {}

