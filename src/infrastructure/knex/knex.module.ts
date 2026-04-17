import { Global, Module, forwardRef } from '@nestjs/common';
import { KnexService } from './knex.service';
import { SqlSchemaMigrationService } from './services/sql-schema-migration.service';
import { SqlSchemaDiffService } from './services/sql-schema-diff.service';
import { MigrationJournalService } from './services/migration-journal.service';
import { DatabaseSchemaService } from './services/database-schema.service';
import { CacheModule } from '../cache/cache.module';
import { SchemaMigrationLockService } from './services/schema-migration-lock.service';
import { ReplicationManager } from './services/replication-manager.service';
import { SqlPoolClusterCoordinatorService } from './services/sql-pool-cluster-coordinator.service';
import { CommonModule } from '../../shared/common/common.module';
import { KnexHookManagerService } from './services/knex-hook-manager.service';

@Global()
@Module({
  imports: [forwardRef(() => CacheModule), CommonModule],
  providers: [
    ReplicationManager,
    KnexHookManagerService,
    KnexService,
    SqlPoolClusterCoordinatorService,
    SqlSchemaMigrationService,
    SqlSchemaDiffService,
    MigrationJournalService,
    DatabaseSchemaService,
    SchemaMigrationLockService,
  ],
  exports: [
    KnexService,
    SqlSchemaMigrationService,
    MigrationJournalService,
    SchemaMigrationLockService,
    ReplicationManager,
  ],
})
export class KnexModule {}
